package goalstate

import (
	"context"
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskReloadRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		taskStore:  taskStore,
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
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

	runtime := &pbtask.RuntimeInfo{}

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		AddTask(instanceID).
		Return(cachedTask)

	taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), jobID, instanceID).
		Return(runtime, nil)

	cachedTask.EXPECT().
		UpdateRuntime(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly)

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

	err := TaskReloadRuntime(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStateInvalidAction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
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

	newRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	newRuntimes[0] = &pbtask.RuntimeInfo{
		State: pbtask.TaskState_FAILED,
	}

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).
		Return(cachedTask)

	cachedTask.EXPECT().
		CurrentState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_RUNNING,
		})

	cachedTask.EXPECT().
		GoalState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_KILLING,
		})

	cachedTask.EXPECT().
		ID().
		Return(instanceID)

	err := TaskStateInvalid(context.Background(), taskEnt)
	assert.NoError(t, err)
}
