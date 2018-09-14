package goalstate

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TaskActionTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	jobStore    *storemocks.MockJobStore
	taskStore   *storemocks.MockTaskStore
	updateStore *storemocks.MockUpdateStore

	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	goalStateDriver     *driver
	taskEnt             *taskEntity
	jobID               *peloton.JobID
	instanceID          uint32
}

func TestTaskAction(t *testing.T) {
	suite.Run(t, new(TaskActionTestSuite))
}

func (suite *TaskActionTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)

	suite.goalStateDriver = &driver{
		taskStore:  suite.taskStore,
		jobEngine:  suite.jobGoalStateEngine,
		taskEngine: suite.taskGoalStateEngine,
		jobFactory: suite.jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)

	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}
}

func (suite *TaskActionTestSuite) TestTaskReloadRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), suite.instanceID).
		Return(suite.cachedTask, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, suite.instanceID).
		Return(&pbtask.RuntimeInfo{}, nil)
	suite.cachedTask.EXPECT().
		ReplaceRuntime(gomock.Any(), gomock.Any())
	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
	err := TaskReloadRuntime(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskActionTestSuite) prepareTest() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).
		Return(suite.cachedTask)
	suite.cachedTask.EXPECT().
		CurrentState().Return(
		cached.TaskStateVector{
			State: pbtask.TaskState_UNKNOWN,
		})
}

func (suite *TaskActionTestSuite) TestTaskStateInvalidAction() {
	newRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	newRuntimes[0] = &pbtask.RuntimeInfo{
		State: pbtask.TaskState_FAILED,
	}
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).
		Return(suite.cachedTask)
	suite.cachedTask.EXPECT().
		CurrentState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_RUNNING,
		})
	suite.cachedTask.EXPECT().
		GoalState().
		Return(cached.TaskStateVector{
			State: pbtask.TaskState_KILLING,
		})
	suite.cachedTask.EXPECT().
		ID().
		Return(suite.instanceID)
	err := TaskStateInvalid(context.Background(), suite.taskEnt)
	suite.NoError(err)
}
