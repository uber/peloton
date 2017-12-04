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
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type TaskUpdaterRMTestSuite struct {
	suite.Suite

	updater          *statusUpdateRM
	ctrl             *gomock.Controller
	testScope        tally.TestScope
	mockResmgrClient *res_mocks.MockResourceManagerServiceYARPCClient
	mockJobStore     *store_mocks.MockJobStore
	mockTaskStore    *store_mocks.MockTaskStore
}

func (suite *TaskUpdaterRMTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockResmgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.testScope = tally.NewTestScope("", map[string]string{})

	suite.updater = &statusUpdateRM{
		jobStore:     suite.mockJobStore,
		taskStore:    suite.mockTaskStore,
		rootCtx:      context.Background(),
		resmgrClient: suite.mockResmgrClient,
		metrics:      NewMetrics(suite.testScope),
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

	jobID := uuid.NewUUID().String()
	uuidStr := uuid.NewUUID().String()
	instanceID := 0
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instanceID, uuidStr)
	pelotonTaskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	pelotonJobID := &peloton.JobID{
		Value: jobID,
	}

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

	taskInfo := &task.TaskInfo{
		JobId:      pelotonJobID,
		InstanceId: uint32(instanceID),
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &mesosTaskID},
			State:       task.TaskState_INITIALIZED,
			GoalState:   task.TaskState_SUCCEEDED,
		},
	}
	updateTaskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &mesosTaskID},
			State:       task.TaskState_RUNNING,
			GoalState:   task.TaskState_SUCCEEDED,
			Reason:      "REASON_RESOURCE_TASK_UPDATE",
		},
	}

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockTaskStore.EXPECT().
			UpdateTaskRuntime(context.Background(), pelotonJobID, uint32(instanceID), updateTaskInfo.Runtime).
			Return(nil),
	)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}
