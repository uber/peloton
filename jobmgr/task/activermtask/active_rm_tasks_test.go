package activermtask

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

func TestPelotonActiveRMTasks(t *testing.T) {
	suite.Run(t, new(TestActiveRMTasks))
}

type TestActiveRMTasks struct {
	suite.Suite
	mockResmgr    *resmocks.MockResourceManagerServiceYARPCClient
	activeRMTasks activeRMTasks
	ctrl          *gomock.Controller
}

func (suite *TestActiveRMTasks) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockResmgr = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)

	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	testCache := make(map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntry)

	for i := 0; i < 10; i++ {
		taskID := fmt.Sprintf("TASK_%v", i)
		testCache[taskID] = &resmgrsvc.GetActiveTasksResponse_TaskEntry{
			TaskID:    taskID,
			TaskState: "PENDING",
			Reason:    fmt.Sprintf("REASON_%v", i),
		}
	}

	suite.activeRMTasks = activeRMTasks{
		resmgrClient: suite.mockResmgr,
		metrics:      metrics,
		taskCache:    testCache,
	}
}

func (suite *TestActiveRMTasks) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *TestActiveRMTasks) TestGetActiveTasks() {
	taskEntry := suite.activeRMTasks.GetActiveTasks("TASK_3")
	assert.NotNil(suite.T(), taskEntry)
	assert.Equal(suite.T(), taskEntry.Reason, "REASON_3")

	taskEntry = suite.activeRMTasks.GetActiveTasks("TASK_11")
	assert.Nil(suite.T(), taskEntry)

	emptyActiveRMTasks := activeRMTasks{
		resmgrClient: suite.mockResmgr,
		metrics:      nil,
		taskCache:    nil,
	}
	taskEntry = emptyActiveRMTasks.GetActiveTasks("TASK_0")
	assert.Nil(suite.T(), taskEntry)
}

func (suite *TestActiveRMTasks) TestUpdateActiveTasks() {

	taskEntries := []*resmgrsvc.GetActiveTasksResponse_TaskEntry{
		&resmgrsvc.GetActiveTasksResponse_TaskEntry{
			TaskID: "TASK_RESMGR",
			Reason: "REASON_RESMGR",
		},
	}
	states := cached.GetResourceManagerProcessingStates()
	sort.Strings(states)
	suite.mockResmgr.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			States: states,
		}).Return(&resmgrsvc.GetActiveTasksResponse{
		TasksByState: map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries{
			task.TaskState_PLACING.String(): {TaskEntry: taskEntries}},
	}, nil)

	suite.activeRMTasks.resmgrClient = suite.mockResmgr
	suite.activeRMTasks.UpdateActiveTasks()
}

func (suite *TestActiveRMTasks) TestUpdateActiveTasksError() {
	states := cached.GetResourceManagerProcessingStates()
	sort.Strings(states)
	suite.mockResmgr.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			States: states,
		}).Return(nil, errors.New("ResMgr Error"))
	// UpdateActiveTasks will not failed
	suite.activeRMTasks.UpdateActiveTasks()
}
