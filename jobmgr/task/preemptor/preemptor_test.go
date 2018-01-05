package preemptor

import (
	"testing"
	"time"

	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	tracked_mocks "code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type PreemptorTestSuite struct {
	suite.Suite
	mockCtrl           *gomock.Controller
	preemptor          *preemptor
	mockResmgr         *mocks.MockResourceManagerServiceYARPCClient
	mockTaskStore      *storage_mocks.MockTaskStore
	mockTrackedManager *tracked_mocks.MockManager
}

func (suite *PreemptorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResmgr = mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.mockTaskStore = storage_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.mockTrackedManager = tracked_mocks.NewMockManager(suite.mockCtrl)
	suite.preemptor = &preemptor{
		resMgrClient:   suite.mockResmgr,
		taskStore:      suite.mockTaskStore,
		trackedManager: suite.mockTrackedManager,
		config: &Config{
			PreemptionPeriod:         1 * time.Minute,
			PreemptionDequeueLimit:   10,
			DequeuePreemptionTimeout: 100,
		},
		metrics: NewMetrics(tally.NoopScope),
	}
}

func (suite *PreemptorTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *PreemptorTestSuite) TestPreemptionCycle() {
	taskID := &peloton.TaskID{Value: "test-preemption-1"}
	task := &resmgr.Task{
		Id: taskID,
	}
	taskInfo := &peloton_task.TaskInfo{
		Runtime: &peloton_task.RuntimeInfo{
			State: peloton_task.TaskState_RUNNING,
		},
	}

	suite.mockResmgr.EXPECT().GetPreemptibleTasks(gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetPreemptibleTasksResponse{
			Tasks: []*resmgr.Task{task},
			Error: nil,
		}, nil,
	)
	suite.mockTaskStore.EXPECT().GetTaskByID(gomock.Any(), taskID.Value).Return(taskInfo, nil)
	suite.mockTrackedManager.EXPECT().UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), taskInfo.Runtime, tracked.UpdateAndSchedule).Return(nil)

	err := suite.preemptor.performPreemptionCycle()
	suite.NoError(err)
	suite.Equal(peloton_task.TaskState_PREEMPTING, taskInfo.GetRuntime().GoalState)
}

func (suite *PreemptorTestSuite) TestReconciler_StartStop() {
	defer func() {
		suite.preemptor.Stop()
		suite.False(suite.preemptor.isRunning())
	}()
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.True(suite.preemptor.isRunning())
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}
