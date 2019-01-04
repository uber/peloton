package cached

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	storemocks "github.com/uber/peloton/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/jobmgr/common"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

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
	listeners  []*FakeTaskListener
}

func (suite *TaskTestSuite) SetupTest() {
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}

	suite.instanceID = uint32(1)

	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.listeners = append(suite.listeners,
		new(FakeTaskListener),
		new(FakeTaskListener))
}

func (suite *TaskTestSuite) TearDownTest() {
	suite.listeners = nil
	suite.ctrl.Finish()
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

// initializeTask initializes a test task to be used in the unit test
func (suite *TaskTestSuite) initializeTask(
	taskStore *storemocks.MockTaskStore,
	jobID *peloton.JobID, instanceID uint32,
	runtime *pbtask.RuntimeInfo) *task {
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
	for _, l := range suite.listeners {
		tt.jobFactory.listeners = append(tt.jobFactory.listeners, l)
	}
	config := &cachedConfig{
		jobType:   pbjob.JobType_BATCH,
		changeLog: &peloton.ChangeLog{Version: 1},
	}
	job := &job{
		id:     jobID,
		config: config,
		runtime: &pbjob.RuntimeInfo{
			ConfigurationVersion: config.changeLog.Version},
	}
	tt.jobFactory.jobs[jobID.GetValue()] = job
	return tt
}

// checkListeners verifies that listeners received the correct data
func (suite *TaskTestSuite) checkListeners(tt *task, jobType pbjob.JobType) {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Equal(suite.jobID, l.jobID, msg)
		suite.Equal(suite.instanceID, l.instanceID, msg)
		suite.Equal(jobType, l.jobType, msg)
		suite.Equal(tt.runtime, l.taskRuntime, msg)
	}
}

// checkListenersNotCalled verifies that listeners did not get invoked
func (suite *TaskTestSuite) checkListenersNotCalled() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Nil(l.jobID, msg)
		suite.Nil(l.taskRuntime, msg)
	}
}

// TestTaskCreateRuntime tests creating task runtime without any DB errors
func (suite *TaskTestSuite) TestCreateRuntime() {
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, nil)
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	jobType := pbjob.JobType_BATCH

	suite.taskStore.EXPECT().
		CreateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			suite.instanceID,
			runtime,
			gomock.Any(),
			jobType).
		Return(nil)

	err := tt.CreateRuntime(context.Background(), runtime, "team10")
	suite.Nil(err)
	suite.checkListeners(tt, jobType)
}

// TestTaskPatchRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
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
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestTaskPatchRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchRuntime_WithInitializedState() {
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	currentMesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-1"
	runtime.MesosTaskId = &mesosv1.TaskID{
		Value: &currentMesosTaskID,
	}
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	mesosTaskID := "acf6e6d4-51be-4b60-8900-683f11252848" + "-1-2"
	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_INITIALIZED,
		jobmgrcommon.MesosTaskIDField: &mesosv1.TaskID{
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
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestPatchRuntime_KillInitializedTask tests updating the case of
// trying to kill initialized task
func (suite *TaskTestSuite) TestPatchRuntime_KillInitializedTask() {
	runtime := initializeTaskRuntime(pbtask.TaskState_INITIALIZED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: pbtask.TaskState_KILLED,
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
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestTaskPatchRuntime_NoRuntimeInCache tests updating task runtime when
// the runtime in cache is nil
func (suite *TaskTestSuite) TestPatchRuntime_NoRuntimeInCache() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		nil)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
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
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestPatchRuntime_DBError tests updating the task runtime with DB errors
func (suite *TaskTestSuite) TestPatchRuntime_DBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	diff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
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
	suite.checkListenersNotCalled()
}

func (suite *TaskTestSuite) TestCompareAndSetNilRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	tt := suite.initializeTask(suite.taskStore, suite.jobID,
		suite.instanceID, runtime)
	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		nil,
		pbjob.JobType_BATCH,
	)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestCompareAndSetRuntime tests changing the task runtime
// using compare and set
func (suite *TaskTestSuite) TestCompareAndSetRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

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

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestCompareAndSetRuntimeFailValidation tests changing the task runtime
// using compare and set but the new runtime fails validation
func (suite *TaskTestSuite) TestCompareAndSetRuntimeFailValidation() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_PENDING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetRuntimeLoadRuntime tests changing the task runtime
// using compare and set and runtime needs to be reloaded from DB
func (suite *TaskTestSuite) TestCompareAndSetRuntimeLoadRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.runtime = nil

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(runtime, nil)

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

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Nil(err)
	suite.checkListeners(tt, pbjob.JobType_BATCH)
}

// TestCompareAndSetRuntimeLoadRuntimeDBError tests changing the task runtime
// using compare and set and runtime reload from DB yields error
func (suite *TaskTestSuite) TestCompareAndSetRuntimeLoadRuntimeDBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)
	tt.runtime = nil

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(runtime, fmt.Errorf("fake db error"))

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetRuntimeVersionError tests changing the task runtime
// using compare and set but with a version error
func (suite *TaskTestSuite) TestCompareAndSetRuntimeVersionError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 3)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.NotNil(err)
	suite.Equal(err, jobmgrcommon.UnexpectedVersionError)
	suite.checkListenersNotCalled()
}

// TestCompareAndSetRuntimeDBError tests changing the task runtime
// using compare and set but getting a DB error in the write
func (suite *TaskTestSuite) TestCompareAndSetRuntimeDBError() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 2)
	newRuntime.GoalState = pbtask.TaskState_SUCCEEDED

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
		Return(fmt.Errorf("fake DB Error"))

	_, err := tt.CompareAndSetRuntime(
		context.Background(),
		newRuntime,
		pbjob.JobType_BATCH,
	)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestReplaceRuntime tests replacing runtime in the cache only
func (suite *TaskTestSuite) TestReplaceRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 3)

	err := tt.ReplaceRuntime(newRuntime, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_RUNNING)
	suite.checkListenersNotCalled()
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
	suite.checkListenersNotCalled()
}

// TestReplaceRuntime_StaleRuntime tests replacing with stale runtime
func (suite *TaskTestSuite) TestReplaceRuntime_StaleRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := suite.initializeTask(suite.taskStore, suite.jobID, suite.instanceID,
		runtime)

	newRuntime := initializeTaskRuntime(pbtask.TaskState_RUNNING, 1)

	err := tt.ReplaceRuntime(newRuntime, false)
	suite.Nil(err)
	suite.Equal(tt.runtime.GetState(), pbtask.TaskState_LAUNCHED)
	suite.checkListenersNotCalled()
}

func (suite *TaskTestSuite) TestValidateState() {
	mesosIDWithRunID1 := "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1"
	mesosIDWithRunID2 := "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2"

	tt := []struct {
		curRuntime     *pbtask.RuntimeInfo
		newRuntime     *pbtask.RuntimeInfo
		expectedResult bool
		message        string
	}{
		{
			curRuntime:     &pbtask.RuntimeInfo{},
			newRuntime:     nil,
			expectedResult: false,
			message:        "nil new runtime should fail validation",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: true,
			message:        "state has no change, validate should succeed",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_SUCCEEDED},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: false,
			message:        "current state is in terminal, no state change allowed",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				MesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID2},
			},
			newRuntime: &pbtask.RuntimeInfo{
				MesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID1},
			},
			expectedResult: false,
			message:        "runID in mesos id cannot decrease",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				DesiredMesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID2},
			},
			newRuntime: &pbtask.RuntimeInfo{
				DesiredMesosTaskId: &mesosv1.TaskID{Value: &mesosIDWithRunID1},
			},
			expectedResult: false,
			message:        "runID in desired mesos id cannot decrease",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PLACING},
			expectedResult: true,
			message:        "state can change between resmgr state change",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_INITIALIZED},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			expectedResult: true,
			message:        "state can change from INITIALIZED to resmgr state ",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_KILLING},
			expectedResult: true,
			message:        "any state can change to KILLING state",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_PENDING},
			expectedResult: false,
			message:        "invalid sate transition from RUNNING to PENDING",
		},
		{
			curRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_KILLING},
			newRuntime:     &pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING},
			expectedResult: false,
			message:        "KILLING can only transit to terminal state",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				GoalState:            pbtask.TaskState_DELETED,
				DesiredConfigVersion: 3,
			},
			newRuntime: &pbtask.RuntimeInfo{
				GoalState:            pbtask.TaskState_RUNNING,
				DesiredConfigVersion: 3,
			},
			expectedResult: false,
			message:        "DELETED goal state cannot be overwritten with config change",
		},
		{
			curRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_DELETED,
				DesiredConfigVersion: 3,
			},
			newRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				DesiredConfigVersion: 4,
			},
			expectedResult: true,
			message:        "DELETED goal state can be overwritten with config change",
		},
	}

	for i, t := range tt {
		task := suite.initializeTask(
			suite.taskStore, suite.jobID, suite.instanceID, t.curRuntime)
		suite.Equal(task.validateState(t.newRuntime), t.expectedResult,
			"test %d fails. message: %s", i, t.message)
	}
}

// TestGetResourceManagerProcessingStates tests
// whether GetResourceManagerProcessingStates returns right states
func TestGetResourceManagerProcessingStates(t *testing.T) {
	expect := []string{
		pbtask.TaskState_LAUNCHING.String(),
		pbtask.TaskState_PENDING.String(),
		pbtask.TaskState_READY.String(),
		pbtask.TaskState_PLACING.String(),
		pbtask.TaskState_PLACED.String(),
		pbtask.TaskState_PREEMPTING.String(),
	}
	states := GetResourceManagerProcessingStates()
	sort.Strings(expect)
	sort.Strings(states)

	assert.Equal(t, expect, states)
}
