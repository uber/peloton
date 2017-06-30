package task

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	resp "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

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
	respool            respool.ResPool
}

func (suite *StateMachineTestSuite) SetupTest() {
	InitTaskTracker(tally.NoopScope)
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
	rootID := resp.ResourcePoolID{Value: respool.RootResPoolID}
	policy := resp.SchedulingPolicy_PriorityFIFO
	respoolConfig := &resp.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    &rootID,
		Resources: suite.getResourceConfig(),
		Policy:    policy,
	}
	suite.respool, _ = respool.NewRespool(tally.NoopScope, "respool-1", nil, respoolConfig)
	suite.tracker.AddTask(suite.task, suite.eventStreamHandler, suite.respool)
}

// Returns resource configs
func (suite *StateMachineTestSuite) getResourceConfig() []*resp.ResourceConfig {

	resConfigs := []*resp.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

func (suite *StateMachineTestSuite) createTask() *resmgr.Task {
	return &resmgr.Task{

		Name:     "job1-1",
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: "job1-1"},
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
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

func (suite *StateMachineTestSuite) TestClear() {
	suite.tracker.Clear()
	suite.Equal(suite.tracker.GetSize(), int64(0))
}

func (suite *StateMachineTestSuite) TestAddResources() {
	res := suite.respool.GetAllocation()
	suite.Equal(res.GetCPU(), float64(0))
	suite.tracker.AddResources(&peloton.TaskID{Value: "job1-1"})
	res = suite.respool.GetAllocation()
	suite.Equal(res.GetCPU(), float64(1))
}
