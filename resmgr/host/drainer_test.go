package host

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	res_mocks "code.uber.internal/infra/peloton/resmgr/respool/mocks"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/lifecycle"
	preemption_mocks "code.uber.internal/infra/peloton/resmgr/preemption/mocks"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	hostname      = "testHostname"
	drainerPeriod = 1 * time.Second
	taskName      = "testTask"
)

type DrainerTestSuite struct {
	suite.Suite
	mockCtrl           *gomock.Controller
	tracker            rm_task.Tracker
	drainer            Drainer
	preemptor          *preemption_mocks.MockPreemptor
	mockHostmgr        *host_mocks.MockInternalHostServiceYARPCClient
	eventStreamHandler *eventstream.Handler
}

func (suite *DrainerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostmgr = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)

	rm_task.InitTaskTracker(tally.NoopScope, &rm_task.Config{}, suite.mockHostmgr)
	suite.tracker = rm_task.GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.preemptor = preemption_mocks.NewMockPreemptor(suite.mockCtrl)

	suite.drainer = Drainer{
		drainerPeriod: drainerPeriod,
		hostMgrClient: suite.mockHostmgr,
		preemptor:     suite.preemptor,
		rmTracker:     suite.tracker,
		lifecycle:     lifecycle.NewLifeCycle(),
	}
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(DrainerTestSuite))
}

func (suite *DrainerTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *DrainerTestSuite) TestDrainer_Init() {
	r := NewDrainer(tally.NoopScope, suite.mockHostmgr, drainerPeriod, suite.tracker, suite.preemptor)
	suite.NotNil(r)
}

func (suite *DrainerTestSuite) TestDrainer_StartStop() {
	defer func() {
		suite.drainer.Stop()
		_, ok := <-suite.drainer.lifecycle.StopCh()
		suite.False(ok)

		// Stopping drainer again should be no-op
		err := suite.drainer.Stop()
		suite.NoError(err)
	}()
	err := suite.drainer.Start()
	suite.NoError(err)
	suite.NotNil(suite.drainer.lifecycle.StopCh())

	// Starting drainer again should be no-op
	err = suite.drainer.Start()
	suite.NoError(err)
}

func (suite *DrainerTestSuite) TestDrainCycle() {
	t := &resmgr.Task{
		Name:     taskName,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: taskName},
		Hostname: hostname,
	}

	mockResPool := res_mocks.NewMockResPool(suite.mockCtrl)
	mockResPool.EXPECT().
		GetPath().
		Return("/mock/path")
	suite.mockHostmgr.EXPECT().
		MarkHostDrained(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.MarkHostDrainedResponse{},
			nil,
		)
	suite.tracker.AddTask(
		t,
		suite.eventStreamHandler,
		mockResPool,
		&rm_task.Config{},
	)

	hostnames := []string{hostname}

	//Test Enqueue error
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.GetDrainingHostsResponse{
				Hostnames: hostnames,
			}, nil,
		)
	suite.preemptor.EXPECT().
		EnqueueTasks(
			gomock.Any(),
			gomock.Any()).
		Return(
			fmt.Errorf("fake Enqueue error"),
		)
	err := suite.drainer.performDrainCycle()
	suite.Error(err)

	// Test success
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.GetDrainingHostsResponse{
				Hostnames: hostnames,
			}, nil,
		)
	suite.preemptor.EXPECT().
		EnqueueTasks(
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	err = suite.drainer.performDrainCycle()
	suite.NoError(err)

	// Test empty GetDrainingHostsResponse
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.GetDrainingHostsResponse{},
			nil,
		)
	err = suite.drainer.performDrainCycle()
	suite.NoError(err)

	// Test GetDrainingHosts error
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			nil,
			fmt.Errorf("fake GetDrainingHosts error"),
		)
	err = suite.drainer.performDrainCycle()
	suite.Error(err)

	// Test MarkHostDrained call
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.GetDrainingHostsResponse{
				Hostnames: []string{"dummyhost"},
			}, nil,
		)
	suite.mockHostmgr.EXPECT().
		MarkHostDrained(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.MarkHostDrainedResponse{},
			nil)
	err = suite.drainer.performDrainCycle()
	suite.NoError(err)

	// Test MarkHostDrained error
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(
			gomock.Any(),
			gomock.Any()).
		Return(
			&hostsvc.GetDrainingHostsResponse{
				Hostnames: []string{"dummyhost"},
			},
			nil,
		)
	suite.mockHostmgr.EXPECT().
		MarkHostDrained(
			gomock.Any(),
			gomock.Any()).
		Return(
			nil,
			fmt.Errorf("fake MarkHostDrained error"),
		)
	suite.preemptor.EXPECT().
		EnqueueTasks(
			gomock.Any(),
			gomock.Any()).Return(nil)
	err = suite.drainer.performDrainCycle()
	suite.Error(err)
}
