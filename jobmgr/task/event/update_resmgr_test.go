package event

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type TaskUpdaterRMTestSuite struct {
	suite.Suite

	updater            *statusUpdateRM
	ctrl               *gomock.Controller
	testScope          tally.TestScope
	mockResmgrClient   *res_mocks.MockResourceManagerServiceYARPCClient
	mockJobStore       *store_mocks.MockJobStore
	mockTrackedManager *mocks.MockManager
}

func (suite *TaskUpdaterRMTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockResmgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTrackedManager = mocks.NewMockManager(suite.ctrl)
	suite.testScope = tally.NewTestScope("", map[string]string{})

	suite.updater = &statusUpdateRM{
		jobStore:       suite.mockJobStore,
		trackedManager: suite.mockTrackedManager,
		rootCtx:        context.Background(),
		resmgrClient:   suite.mockResmgrClient,
		metrics:        NewMetrics(suite.testScope),
	}
}

func (suite *TaskUpdaterRMTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestPelotonTaskUpdaterRM(t *testing.T) {
	suite.Run(t, new(TaskUpdaterRMTestSuite))
}

// Test happy case of processing status update.
func (suite *TaskUpdaterRMTestSuite) TestProcessStatusUpdate() {
	defer suite.ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewUUID().String()}
	uuidStr := uuid.NewUUID().String()
	instanceID := uint32(0)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.Value, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID.Value, instanceID)

	pelotonState := task.TaskState_RUNNING
	pelotonEvent := &task.TaskEvent{
		TaskId: &peloton.TaskID{
			Value: pelotonTaskID,
		},
		State: pelotonState,
	}

	event := &pb_eventstream.Event{
		PelotonTaskEvent: pelotonEvent,
		Type:             pb_eventstream.Event_PELOTON_TASK_EVENT,
	}

	runtime := &task.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{Value: &mesosTaskID},
		State:       task.TaskState_INITIALIZED,
		GoalState:   task.TaskGoalState_SUCCEED,
	}
	updateRuntime := &task.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{Value: &mesosTaskID},
		State:       task.TaskState_RUNNING,
		GoalState:   task.TaskGoalState_SUCCEED,
	}

	gomock.InOrder(
		suite.mockTrackedManager.EXPECT().
			GetTaskRuntime(context.Background(), jobID, instanceID).
			Return(runtime, nil),
		suite.mockTrackedManager.EXPECT().
			UpdateTaskRuntime(context.Background(), jobID, instanceID, updateRuntime).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}
