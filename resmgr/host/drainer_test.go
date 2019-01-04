package host

import (
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	res_mocks "github.com/uber/peloton/resmgr/respool/mocks"

	"github.com/uber/peloton/common"
	"github.com/uber/peloton/common/eventstream"
	"github.com/uber/peloton/common/lifecycle"
	"github.com/uber/peloton/common/stringset"
	preemption_mocks "github.com/uber/peloton/resmgr/preemption/mocks"
	rm_task "github.com/uber/peloton/resmgr/task"

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
	preemptor          *preemption_mocks.MockQueue
	mockHostmgr        *host_mocks.MockInternalHostServiceYARPCClient
	eventStreamHandler *eventstream.Handler
	hostnames          []string
}

func (suite *DrainerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	rm_task.InitTaskTracker(tally.NoopScope, &rm_task.Config{})
	suite.tracker = rm_task.GetTracker()
}

func (suite *DrainerTestSuite) SetupTest() {
	suite.mockHostmgr = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)

	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.preemptor = preemption_mocks.NewMockQueue(suite.mockCtrl)

	suite.drainer = Drainer{
		drainerPeriod:   drainerPeriod,
		hostMgrClient:   suite.mockHostmgr,
		preemptionQueue: suite.preemptor,
		rmTracker:       suite.tracker,
		lifecycle:       lifecycle.NewLifeCycle(),
		drainingHosts:   stringset.New(),
	}

	t := &resmgr.Task{
		Name:     taskName,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: taskName},
		Hostname: hostname,
	}

	mockRespool := res_mocks.NewMockResPool(suite.mockCtrl)
	mockRespool.EXPECT().GetPath().Return("mockRespoolPath")
	suite.tracker.AddTask(
		t, suite.eventStreamHandler,
		mockRespool,
		&rm_task.Config{})

	suite.hostnames = []string{hostname}
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(DrainerTestSuite))
}

func (suite *DrainerTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *DrainerTestSuite) TestDrainer_Init() {
	r := NewDrainer(
		tally.NoopScope,
		suite.mockHostmgr,
		drainerPeriod,
		suite.tracker,
		suite.preemptor)
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

func (suite *DrainerTestSuite) TestDrainCycle_EnqueueError() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: suite.hostnames,
		}, nil)
	suite.preemptor.EXPECT().
		EnqueueTasks(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake Enqueue error"))
	err := suite.drainer.performDrainCycle()
	suite.Error(err)
	suite.drainer.drainingHosts.Clear()
}

func (suite *DrainerTestSuite) TestDrainCycle() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: suite.hostnames,
		}, nil)
	suite.preemptor.EXPECT().
		EnqueueTasks(gomock.Any(), gomock.Any()).
		Return(nil)
	err := suite.drainer.performDrainCycle()
	suite.NoError(err)
	suite.Len(suite.drainer.drainingHosts.ToSlice(), len(suite.hostnames))
	for _, host := range suite.hostnames {
		suite.Equal(true, suite.drainer.drainingHosts.Contains(host))
	}
}

func (suite *DrainerTestSuite) TestDrainCycle_NoHostsToDrain() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{}, nil)
	err := suite.drainer.performDrainCycle()
	suite.NoError(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_GetDrainingHostsError() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake GetDrainingHosts error"))
	err := suite.drainer.performDrainCycle()
	suite.Error(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_MarkHostsDrained() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: []string{"dummyhost"},
		}, nil)
	suite.mockHostmgr.EXPECT().
		MarkHostsDrained(gomock.Any(), gomock.Any()).
		Return(&hostsvc.MarkHostsDrainedResponse{}, nil)
	err := suite.drainer.performDrainCycle()
	suite.NoError(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_MarkHostsDrainedError() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: []string{"dummyhost"},
		}, nil)
	suite.mockHostmgr.EXPECT().
		MarkHostsDrained(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake MarkHostsDrained error")).
		Times(markHostDrainedBackoffRetryCount)
	err := suite.drainer.performDrainCycle()
	suite.Error(err)
}
