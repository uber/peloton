package goalstate

import (
	"context"
	"errors"
	"testing"
	"time"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskLaunchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockHostMgr := hostmocks.NewMockInternalHostServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobStore:      jobStore,
		taskStore:     taskStore,
		jobFactory:    jobFactory,
		hostmgrClient: mockHostMgr,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}
	runtime := &pb_task.RuntimeInfo{
		State:       pb_task.TaskState_LAUNCHED,
		MesosTaskId: oldMesosTaskID,
		GoalState:   pb_task.TaskState_SUCCEEDED,
	}
	config := &pb_task.TaskConfig{}
	newRuntime := runtime
	jobConfig := &pb_job.JobConfig{
		Type: pb_job.JobType_BATCH,
	}

	for i := 0; i < 2; i++ {
		jobFactory.EXPECT().
			GetJob(jobID).Return(cachedJob)

		cachedJob.EXPECT().
			GetTask(instanceID).Return(cachedTask)

		cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(runtime, nil)

		cachedTask.EXPECT().
			GetLastRuntimeUpdateTime().Return(time.Now().Add(-goalStateDriver.cfg.LaunchTimeout))

		jobFactory.EXPECT().
			GetJob(jobID).Return(cachedJob)

		cachedJob.EXPECT().
			GetTask(instanceID).Return(cachedTask)

		cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(runtime, nil)

		jobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)

		cachedJob.EXPECT().UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Do(
			func(_ context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
				for _, updatedRuntimeInfo := range runtimes {
					newRuntime = updatedRuntimeInfo
				}
			}).Return(nil)

		cachedJob.EXPECT().
			GetJobType().Return(pb_job.JobType_BATCH)

		taskGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		jobGoalStateEngine.EXPECT().
			Enqueue(gomock.Any(), gomock.Any()).
			Return()

		if i == 0 {
			// test happy case
			taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, nil)
			mockHostMgr.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
				TaskIds: []*mesos_v1.TaskID{oldMesosTaskID},
			})
		} else {
			// test skip task kill
			taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New(""))
			mockHostMgr.EXPECT()
		}

		err := TaskLaunchRetry(context.Background(), taskEnt)
		assert.NoError(t, err)
		assert.NotEqual(t, oldMesosTaskID, newRuntime.MesosTaskId)
		assert.Equal(t, pb_task.TaskState_INITIALIZED, newRuntime.State)
		assert.Equal(t, pb_task.TaskState_SUCCEEDED, newRuntime.GoalState)
	}
}

func TestLaunchedTaskSendLaunchInfoResMgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	runtime := &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_LAUNCHED,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	resmgrClient.EXPECT().
		MarkTasksLaunched(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.MarkTasksLaunchedResponse{}, nil)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskLaunchRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStartTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockHostMgr := hostmocks.NewMockInternalHostServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobStore:      jobStore,
		taskStore:     taskStore,
		jobFactory:    jobFactory,
		hostmgrClient: mockHostMgr,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	oldMesosTaskID := &mesos_v1.TaskID{
		Value: &[]string{uuid.New()}[0],
	}
	runtime := &pb_task.RuntimeInfo{
		State:       pb_task.TaskState_STARTING,
		MesosTaskId: oldMesosTaskID,
		GoalState:   pb_task.TaskState_SUCCEEDED,
	}
	config := &pb_task.TaskConfig{}
	jobConfig := &pb_job.JobConfig{
		Type: pb_job.JobType_BATCH,
	}

	newRuntime := runtime

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	taskStore.EXPECT().GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(config, nil)

	cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now().Add(-goalStateDriver.cfg.LaunchTimeout))

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	jobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)

	cachedJob.EXPECT().UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Do(
		func(_ context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
			for _, updatedRuntimeInfo := range runtimes {
				newRuntime = updatedRuntimeInfo
			}
		}).Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	mockHostMgr.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{oldMesosTaskID},
	})

	err := TaskLaunchRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
	assert.NotEqual(t, oldMesosTaskID, newRuntime.MesosTaskId)
	assert.Equal(t, pb_task.TaskState_INITIALIZED, newRuntime.State)
	assert.Equal(t, pb_task.TaskState_SUCCEEDED, newRuntime.GoalState)
}

func TestStartingTaskReenqueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	runtime := &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_STARTING,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskLaunchRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskWithUnexpectedStateReenqueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	runtime := &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskLaunchRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}
