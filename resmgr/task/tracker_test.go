package task

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/uber-go/tally"

	"peloton/api/peloton"
	"peloton/api/task"

	"peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
)

type StateMachineTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	tracker            Tracker
	eventStreamHandler *eventstream.Handler
	task               *resmgr.Task
}

func (suite *StateMachineTestSuite) SetupTest() {
	InitTaskTracker()
	suite.tracker = GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
	suite.task = suite.createTask()
	resp, _ := respool.NewRespool("respool-1", nil, nil)
	suite.tracker.AddTask(suite.task, suite.eventStreamHandler, resp)
}

func (suite *StateMachineTestSuite) createTask() *resmgr.Task {
	return &resmgr.Task{

		Name:     "job1-1",
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: "job1-1"},
	}
}

func TestStateMachine(t *testing.T) {
	suite.Run(t, new(StateMachineTestSuite))
}

func (suite *StateMachineTestSuite) TestTransition() {
	err := suite.tracker.GetTask(suite.task.Id).TransitTo(
		task.TaskState_PENDING.String())
	suite.NoError(err)
	err = suite.tracker.GetTask(suite.task.Id).TransitTo(
		task.TaskState_READY.String())
	suite.NoError(err)
}

func (suite *StateMachineTestSuite) TestDelete() {
	suite.tracker.DeleteTask(suite.task.Id)
	task := suite.tracker.GetTask(suite.task.Id)
	suite.Nil(task)
}
