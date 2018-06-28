package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
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
			jobs:      map[string]*job{},
		},
	}
	config := &cachedConfig{
		jobType: pbjob.JobType_BATCH,
	}
	job := &job{
		id:     jobID,
		config: config,
	}
	tt.jobFactory.jobs[jobID.GetValue()] = job
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

type TaskTestSuite struct {
	suite.Suite

	ctrl       *gomock.Controller
	jobID      *peloton.JobID
	instanceID uint32
	taskStore  *storemocks.MockTaskStore
}

func (suite *TaskTestSuite) SetupTest() {
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}

	suite.instanceID = uint32(1)

	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
}

func (suite *TaskTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

// TestTaskValidateFailRevision tests that updating a runtime with invalid revision fails
func (suite *TaskTestSuite) TestTaskValidateFailRevision() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instID := uint32(1)
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(suite.taskStore, jobID, instID, runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 1)
	suite.False(tt.validateStateUpdate(newRuntime))

	newRuntime = initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	suite.False(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailMesosToRes tests that transitioning state from a mesos
// owned state to a resource manager owned state fails
func (suite *TaskTestSuite) TestTaskValidateFailMesosToRes() {
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_PLACING,
	}
	suite.False(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailResToInitialized tests that transitioning state from a
// resource manager owned state to INITIALIZED state fails
func (suite *TaskTestSuite) TestTaskValidateFailResToInitialized() {
	runtime := initializeTaskRuntime(pbtask.TaskState_PLACING, 2)
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
	}
	suite.False(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailMesosToLaunched tests that transitioning state from a mesos
// owned state to LAUNCHED state fails
func (suite *TaskTestSuite) TestTaskValidateFailMesosToLaunched() {
	runtime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_LAUNCHED,
	}
	suite.False(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailTerminalTaskUpdate tests that transitioning from a
// terminal task state withouth changing the mesos task ID fails
func (suite *TaskTestSuite) TestTaskValidateFailTerminalTaskUpdate() {
	runtime := initializeTaskRuntime(pbtask.TaskState_SUCCEEDED, 2)
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	suite.False(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateMesosTaskIdChange tests state transition with a mesos
// task identifier change
func (suite *TaskTestSuite) TestTaskValidateMesosTaskIdChange() {
	mesosID := "old"
	runtime := initializeTaskRuntime(pbtask.TaskState_FAILED, 2)
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &mesosID,
	}
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newMesosID := "new"
	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
		MesosTaskId: &mesosv1.TaskID{
			Value: &newMesosID,
		},
	}
	suite.True(tt.validateStateUpdate(newRuntime))
}

// TestTaskValidateFailOldMesosTaskId tests that transitioning to an
// older mesos task identifier fails
func (suite *TaskTestSuite) TestTaskValidateFailOldMesosTaskId() {
	mesosID := "old"
	runtime := initializeTaskRuntime(pbtask.TaskState_FAILED, 2)
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &mesosID,
	}
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newMesosID := "new"
	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
		MesosTaskId: &mesosv1.TaskID{
			Value: &newMesosID,
		},
	}
	suite.False(tt.validateStateUpdate(newRuntime))
}

func validateMergeRuntime(
	suite *TaskTestSuite,
	newRuntime *pbtask.RuntimeInfo,
	mergedRuntime *pbtask.RuntimeInfo,
	expectedVersion uint64) {
	suite.Equal(newRuntime.GetState(), mergedRuntime.GetState())
	suite.Equal(newRuntime.GetMesosTaskId(), mergedRuntime.GetMesosTaskId())
	suite.Equal(newRuntime.GetStartTime(), mergedRuntime.GetStartTime())
	suite.Equal(newRuntime.GetCompletionTime(), mergedRuntime.GetCompletionTime())
	suite.Equal(newRuntime.GetHost(), mergedRuntime.GetHost())
	suite.Equal(newRuntime.GetPorts(), mergedRuntime.GetPorts())
	suite.Equal(newRuntime.GetGoalState(), mergedRuntime.GetGoalState())
	suite.Equal(newRuntime.GetMessage(), mergedRuntime.GetMessage())
	suite.Equal(newRuntime.GetReason(), mergedRuntime.GetReason())
	suite.Equal(newRuntime.GetFailureCount(), mergedRuntime.GetFailureCount())
	suite.Equal(newRuntime.GetConfigVersion(), mergedRuntime.GetConfigVersion())
	suite.Equal(newRuntime.GetDesiredConfigVersion(), mergedRuntime.GetDesiredConfigVersion())
	suite.Equal(newRuntime.GetAgentID(), mergedRuntime.GetAgentID())
	suite.Equal(newRuntime.GetPrevMesosTaskId(), mergedRuntime.GetPrevMesosTaskId())
	suite.Equal(expectedVersion, mergedRuntime.Revision.Version)
}

// TestTaskMergeRuntime tests merging current and new runtime
func (suite *TaskTestSuite) TestTaskMergeRuntime() {
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
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

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
	validateMergeRuntime(suite, newRuntime, mergedRuntime, uint64(3))
}

// TestTaskUpdateRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestTaskUpdateRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	suite.Nil(err)
}

// TestTaskUpdateRuntimeWithNoRuntimeInCache tests updating task runtime when
// the runtime in cache is nil
func (suite *TaskTestSuite) TestTaskUpdateRuntimeWithNoRuntimeInCache() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, nil)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).Return(runtime, nil)

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	suite.Nil(err)
}

// TestTaskUpdateRuntimeDBError tests updating the task runtime with DB errors
func (suite *TaskTestSuite) TestTaskUpdateRuntimeDBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := tt.UpdateRuntime(context.Background(), newRuntime, UpdateCacheAndDB)
	suite.NotNil(err)
}

// TestTaskPatchRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	diff := map[string]interface{}{
		StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.PatchRuntime(context.Background(), diff)
	suite.Nil(err)
}

// TestTaskPatchRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchRuntime_WithInitializedState() {
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	currentMesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-1"
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &currentMesosTaskID,
	}
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	mesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-2"
	diff := map[string]interface{}{
		StateField: pbtask.TaskState_INITIALIZED,
		MesosTaskIDField: &mesosv1.TaskID{
			Value: &mesosTaskID,
		},
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_INITIALIZED)
			suite.Equal(uint64(runtime.Revision.Version), uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.PatchRuntime(context.Background(), diff)
	suite.Nil(err)
}

// TestPatchRuntime_KillInitializedTask tests updating the case of
// trying to kill initialized task
func (suite *TaskTestSuite) TestPatchRuntime_KillInitializedTask() {
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	diff := map[string]interface{}{
		GoalStateField: pbtask.TaskState_KILLED,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(
			ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_INITIALIZED)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_KILLED)
		}).
		Return(nil)

	err := tt.PatchRuntime(context.Background(), diff)
	suite.Nil(err)
}

// TestTaskPatchRuntime_NoRuntimeInCache tests updating task runtime when
// the runtime in cache is nil
func (suite *TaskTestSuite) TestPatchRuntime_NoRuntimeInCache() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, nil)

	diff := map[string]interface{}{
		StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).Return(runtime, nil)

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Do(func(ctx context.Context,
			jobID *peloton.JobID,
			instanceID uint32,
			runtime *pbtask.RuntimeInfo,
			jobType pbjob.JobType) {
			suite.Equal(runtime.GetState(), pbtask.TaskState_RUNNING)
			suite.Equal(runtime.Revision.Version, uint64(3))
			suite.Equal(runtime.GetGoalState(), pbtask.TaskState_SUCCEEDED)
		}).
		Return(nil)

	err := tt.PatchRuntime(context.Background(), diff)
	suite.Nil(err)
}

// TestPatchRuntime_DBError tests updating the task runtime with DB errors
func (suite *TaskTestSuite) TestPatchRuntime_DBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	diff := map[string]interface{}{
		StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			gomock.Any(),
			gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := tt.PatchRuntime(context.Background(), diff)
	suite.NotNil(err)
}

// TestReplaceRuntime tests replacing runtime in the cache only
func (suite *TaskTestSuite) TestReplaceRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 3)

	err := tt.ReplaceRuntime(newRuntime, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_RUNNING)
}

// TestReplaceRuntime_NoExistingCache tests replacing cache when
// there is no existing cache
func (suite *TaskTestSuite) TestReplaceRuntime_NoExistingCache() {
	tt := &task{
		id:    suite.instanceID,
		jobID: suite.jobID,
	}

	suite.Equal(suite.instanceID, tt.ID())
	suite.Equal(suite.jobID.Value, tt.jobID.Value)

	// Test fetching state and goal state of task
	runtime := pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}
	tt.ReplaceRuntime(&runtime, false)

	curState := tt.CurrentState()
	curGoalState := tt.GoalState()
	suite.Equal(runtime.State, curState.State)
	suite.Equal(runtime.GoalState, curGoalState.State)
}

// TestReplaceRuntime_StaleRuntime tests replacing with stale runtime
func (suite *TaskTestSuite) TestReplaceRuntime_StaleRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 1)

	err := tt.ReplaceRuntime(newRuntime, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_LAUNCHED)
}
