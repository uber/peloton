package goalstate

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	hostMock := hostmocks.NewMockInternalHostServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobFactory:    jobFactory,
		hostmgrClient: hostMock,
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

	taskID := &mesos_v1.TaskID{
		Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
	}

	runtime := &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: taskID,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask).Times(2)

	cachedTask.EXPECT().
		GetRunTime().Return(runtime)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	runtime.State = pbtask.TaskState_KILLING
	runtime.Message = "Killing the task"
	runtime.Reason = ""
	cachedJob.EXPECT().UpdateTasks(gomock.Any(), map[uint32]*pbtask.RuntimeInfo{
		instanceID: runtime,
	}, cached.UpdateCacheAndDB)

	hostMock.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{taskID},
	}).Return(nil, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			// The test should not take more than one min
			assert.True(t, deadline.Sub(time.Now()).Round(time.Minute) ==
				_defaultShutdownExecutorTimeout)
		}).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime().Return(nil)

	err = TaskStop(context.Background(), taskEnt)
	assert.EqualError(t, err, "task has no runtime info in cache")
}

func TestTaskStopIfInitializedCallsKillOnResmgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockResmgr := resmocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: mockResmgr,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	instanceID := uint32(7)
	taskID := &peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7"}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	killResponseErr = append(killResponseErr,
		&resmgrsvc.KillTasksResponse_Error{
			NotFound: &resmgrsvc.TasksNotFound{
				Message: "Tasks Not Found",
				Task:    taskID,
			},
		})
	res := &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime().Return(runtime)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	mockResmgr.EXPECT().KillTasks(context.Background(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7",
			},
		},
	}).Return(res, nil)

	taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), jobID, instanceID).Return(runtime, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStopIfPendingCallsKillOnResmgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockResmgr := resmocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: mockResmgr,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	instanceID := uint32(7)
	taskID := &peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7"}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_PENDING,
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	killResponseErr = append(killResponseErr,
		&resmgrsvc.KillTasksResponse_Error{
			NotFound: &resmgrsvc.TasksNotFound{
				Message: "Tasks Not Found",
				Task:    taskID,
			},
		})
	res := &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime().Return(runtime)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	mockResmgr.EXPECT().KillTasks(gomock.Any(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7",
			},
		},
	}).Return(res, nil)

	taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), jobID, instanceID).Return(runtime, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)
}
