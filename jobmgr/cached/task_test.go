package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// initializeTask initializes a test task to be used in the unit test
func initializeTask(taskStore *storemocks.MockTaskStore, jobID *peloton.JobID, instanceID uint32, runtime *pbtask.RuntimeInfo) *task {
	tt := &task{
		id:      instanceID,
		jobID:   jobID,
		runtime: runtime,
		jobFactory: &jobFactory{
			mtx:       NewMetrics(tally.NoopScope),
			taskStore: taskStore,
			running:   true,
		},
	}
	return tt
}

func initializeTaskRuntime(state pbtask.TaskState, version uint64) *pbtask.RuntimeInfo {
	runtime := &pbtask.RuntimeInfo{
		State: state,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   version,
		},
	}
	return runtime
}

// TestTask tests fetching the identifiers, the state and goal state from the task
func TestTask(t *testing.T) {
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(1)

	tt := &task{
		id:    instanceID,
		jobID: &jobID,
	}

	assert.Equal(t, instanceID, tt.ID())
	assert.Equal(t, jobID, *tt.JobID())

	// Test fetching state and goal state of task
	runtime := pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	tt.UpdateRuntime(context.Background(), &runtime, UpdateCacheOnly)

	curState := tt.CurrentState()
	curGoalState := tt.GoalState()
	assert.Equal(t, runtime.State, curState.State)
	assert.Equal(t, runtime.GoalState, curGoalState.State)
}

// TestTaskValidateFailRevision tests that updating a runtime with invalid revision fails
func TestTaskValidateFailRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 1)
	assert.False(t, tt.validateStateUpdate(newRuntime))

	newRuntime = initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailMesosToRes tests that transitioning state from a mesos
// owned state to a resource manager owned state fails
func TestTaskValidateFailMesosToRes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_PLACING,
	}
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailResToInitialized tests that transitioning state from a
// resource manager owned state to INITIALIZED state fails
func TestTaskValidateFailResToInitialized(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_PLACING, 2)
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
	}
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailMesosToLaunched tests that transitioning state from a mesos
// owned state to LAUNCHED state fails
func TestTaskValidateFailMesosToLaunched(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_LAUNCHED,
	}
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailTerminalTaskUpdate tests that transitioning from a
// terminal task state withouth changing the mesos task ID fails
func TestTaskValidateFailTerminalTaskUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_SUCCEEDED, 2)
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateMesosTaskIdChange tests state transition with a mesos
// task identifier change
func TestTaskValidateMesosTaskIdChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	mesosID := "old"
	runtime := initializeTaskRuntime(pbtask.TaskState_FAILED, 2)
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &mesosID,
	}
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newMesosID := "new"
	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
		MesosTaskId: &mesosv1.TaskID{
			Value: &newMesosID,
		},
	}
	assert.True(t, tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailOldMesosTaskId tests that transitioning to an
// older mesos task identifier fails
func TestTaskValidateFailOldMesosTaskId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	mesosID := "old"
	runtime := initializeTaskRuntime(pbtask.TaskState_FAILED, 2)
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &mesosID,
	}
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newMesosID := "new"
	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesosv1.TaskID{
			Value: &newMesosID,
		},
	}
	assert.False(t, tt.validateStateUpdate(newRuntime))
}

func validateMergeRuntime(
	t *testing.T,
	newRuntime *pbtask.RuntimeInfo,
	mergedRuntime *pbtask.RuntimeInfo,
	expectedVersion uint64) {
	assert.Equal(t, newRuntime.GetState(), mergedRuntime.GetState())
	assert.Equal(t, newRuntime.GetMesosTaskId(), mergedRuntime.GetMesosTaskId())
	assert.Equal(t, newRuntime.GetStartTime(), mergedRuntime.GetStartTime())
	assert.Equal(t, newRuntime.GetCompletionTime(), mergedRuntime.GetCompletionTime())
	assert.Equal(t, newRuntime.GetHost(), mergedRuntime.GetHost())
	assert.Equal(t, newRuntime.GetPorts(), mergedRuntime.GetPorts())
	assert.Equal(t, newRuntime.GetGoalState(), mergedRuntime.GetGoalState())
	assert.Equal(t, newRuntime.GetMessage(), mergedRuntime.GetMessage())
	assert.Equal(t, newRuntime.GetReason(), mergedRuntime.GetReason())
	assert.Equal(t, newRuntime.GetFailureCount(), mergedRuntime.GetFailureCount())
	assert.Equal(t, newRuntime.GetConfigVersion(), mergedRuntime.GetConfigVersion())
	assert.Equal(t, newRuntime.GetDesiredConfigVersion(), mergedRuntime.GetDesiredConfigVersion())
	assert.Equal(t, newRuntime.GetAgentID(), mergedRuntime.GetAgentID())
	assert.Equal(t, newRuntime.GetPrevMesosTaskId(), mergedRuntime.GetPrevMesosTaskId())
	assert.Equal(t, expectedVersion, mergedRuntime.Revision.Version)
}

// TestTaskMergeRuntime tests merging current and new runtime
func TestTaskMergeRuntime(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	mesos1Old := "mesos1-old"
	mesos1Older := "mesos1-older"
	agentOld := "agent-old"
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_LAUNCHED,
		MesosTaskId: &mesosv1.TaskID{
			Value: &mesos1Old,
		},
		StartTime:      time.Now().UTC().Format(time.RFC3339Nano),
		CompletionTime: time.Now().UTC().Format(time.RFC3339Nano),
		Host:           "host1",
		Ports: map[string]uint32{
			"current": 1,
		},
		GoalState:            pbtask.TaskState_SUCCEEDED,
		Message:              "old runtime",
		Reason:               "old runtime",
		FailureCount:         1,
		ConfigVersion:        1,
		DesiredConfigVersion: 1,
		AgentID: &mesosv1.AgentID{
			Value: &agentOld,
		},
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   2,
		},
		PrevMesosTaskId: &mesosv1.TaskID{
			Value: &mesos1Older,
		},
	}
	tt := initializeTask(nil, jobID, instID, runtime)

	curTime := time.Now().UTC().Format(time.RFC3339Nano)

	newMesosID := "mesos1-new"
	agentNew := "agent-new"
	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesosv1.TaskID{
			Value: &newMesosID,
		},
		StartTime:      curTime,
		CompletionTime: curTime,
		Host:           "host2",
		Ports: map[string]uint32{
			"current": 2,
		},
		GoalState:            pbtask.TaskState_RUNNING,
		Message:              "new runtime",
		Reason:               "new runtime",
		FailureCount:         2,
		ConfigVersion:        2,
		DesiredConfigVersion: 3,
		AgentID: &mesosv1.AgentID{
			Value: &agentNew,
		},
		PrevMesosTaskId: &mesosv1.TaskID{
			Value: &mesos1Old,
		},
	}

	mergedRuntime := tt.mergeRuntime(newRuntime)
	validateMergeRuntime(t, newRuntime, mergedRuntime, uint64(3))
}

// TestTaskUpdateRuntime tests updating the task runtime without any DB errors
func TestTaskUpdateRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	taskstore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), jobID, instID, gomock.Any()).
		Do(func(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pbtask.RuntimeInfo) {
			assert.Equal(t, runtime.GetState(), pbtask.TaskState_RUNNING)
			assert.Equal(t, runtime.Revision.Version, uint64(3))
			assert.Equal(t, runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	assert.Nil(t, err)
}

// TestTaskUpdateRuntimeWithNoRuntimeInCache tests updating task runtime when
// the runtime in cache is nil
func TestTaskUpdateRuntimeWithNoRuntimeInCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(taskstore, jobID, instID, nil)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	taskstore.EXPECT().
		GetTaskRuntime(gomock.Any(), jobID, instID).Return(runtime, nil)

	taskstore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), jobID, instID, gomock.Any()).
		Do(func(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pbtask.RuntimeInfo) {
			assert.Equal(t, runtime.GetState(), pbtask.TaskState_RUNNING)
			assert.Equal(t, runtime.Revision.Version, uint64(3))
			assert.Equal(t, runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	assert.Nil(t, err)
}

// TestTaskUpdateRuntimeDBError tests updating the task runtime with DB errors
func TestTaskUpdateRuntimeDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	taskstore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), jobID, instID, gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	assert.NotNil(t, err)
}

// TestTaskUpdateRuntimeInCacheOnly tests updating runtime in the cache only
func TestTaskUpdateRuntimeInCacheOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(taskstore, jobID, instID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheOnly)
	assert.Nil(t, err)
	assert.Equal(t, tt.runtime.GetState(), pbtask.TaskState_RUNNING)
}
