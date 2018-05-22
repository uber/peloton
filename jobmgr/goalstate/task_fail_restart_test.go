package goalstate

import (
	"context"
	"fmt"
	"testing"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskFailNoRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 0,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 3,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
			assert.True(t, runtimes[instanceID].GetMesosTaskId().GetValue() != mesosTaskID)
			assert.True(t, runtimes[instanceID].GetPrevMesosTaskId().GetValue() == mesosTaskID)
			assert.True(t, runtimes[instanceID].GetState() == pb_task.TaskState_INITIALIZED)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailSystemFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String(),
	}

	taskConfig := pb_task.TaskConfig{
		RestartPolicy: &pb_task.RestartPolicy{
			MaxFailures: 0,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
			assert.True(t, runtimes[instanceID].GetMesosTaskId().GetValue() != mesosTaskID)
			assert.True(t, runtimes[instanceID].GetPrevMesosTaskId().GetValue() == mesosTaskID)
			assert.True(t, runtimes[instanceID].GetState() == pb_task.TaskState_INITIALIZED)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		State:         pb_task.TaskState_FAILED,
		GoalState:     pb_task.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(nil, fmt.Errorf("fake db error"))

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.Error(t, err)
}
