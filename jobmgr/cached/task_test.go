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

// TestTaskPatchRuntime tests updating the task runtime without any DB errors
func (suite *TaskTestSuite) TestPatchRuntime() {
	runtime := initializeTaskRuntime(pbtask.TaskState_LAUNCHED, 2)
	runtime.GoalState = pbtask.TaskState_SUCCEEDED
	tt := initializeTask(suite.taskStore, suite.jobID, suite.instanceID, runtime)

	diff := RuntimeDiff{
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
	diff := RuntimeDiff{
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

	diff := RuntimeDiff{
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

	diff := RuntimeDiff{
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

	diff := RuntimeDiff{
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
