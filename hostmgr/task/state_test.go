package task

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common/cirbuf"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/rpc"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	mpb_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
)

const (
	_encoding      = mpb.ContentTypeJSON
	_zkPath        = "zkpath"
	_frameworkID   = "framework-id"
	_frameworkName = "framework-name"
	_streamID      = "stream_id"
)

type stateManagerTestSuite struct {
	suite.Suite

	sync.Mutex
	ctrl         *gomock.Controller
	context      context.Context
	resMgrClient *res_mocks.MockResourceManagerServiceYARPCClient
	stateManager StateManager

	taskStatusUpdate *sched.Event
	event            *pb_eventstream.Event

	store           *storage_mocks.MockFrameworkInfoStore
	driver          hostmgr_mesos.SchedulerDriver
	schedulerClient *mpb_mocks.MockSchedulerClient
}

func (s *stateManagerTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())
	s.context = context.Background()

	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: outbound,
		},
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  nil,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})
	s.schedulerClient = mpb_mocks.NewMockSchedulerClient(s.ctrl)

	s.resMgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(s.ctrl)
	s.stateManager = NewStateManager(
		dispatcher,
		s.schedulerClient,
		10,
		10,
		s.resMgrClient,
		tally.NoopScope)

	s.store = storage_mocks.NewMockFrameworkInfoStore(s.ctrl)

	s.driver = hostmgr_mesos.InitSchedulerDriver(
		&hostmgr_mesos.Config{
			Framework: &hostmgr_mesos.FrameworkConfig{
				Name:                        _frameworkName,
				GPUSupported:                true,
				TaskKillingStateSupported:   false,
				PartitionAwareSupported:     false,
				RevocableResourcesSupported: false,
			},
			ZkPath:   _zkPath,
			Encoding: _encoding,
		},
		s.store,
		http.Header{},
	).(hostmgr_mesos.SchedulerDriver)
}

func (s *stateManagerTestSuite) TearDownSuite() {
	s.ctrl.Finish()
}

func (s *stateManagerTestSuite) SetupTest() {
	_uuid := "d2c41522-0216-4704-8903-2945414c414c"
	state := mesos.TaskState_TASK_STARTING
	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_uuid,
		},
		State: &state,
		Uuid:  []byte(_uuid),
		AgentId: &mesos.AgentID{
			Value: &_uuid,
		},
	}

	s.taskStatusUpdate = &sched.Event{
		Update: &sched.Event_Update{
			Status: status,
		},
	}

	s.event = &pb_eventstream.Event{
		Offset:          uint64(1),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: s.taskStatusUpdate.GetUpdate().GetStatus(),
	}
}

func (s *stateManagerTestSuite) TearDownTest() {
}

func (s *stateManagerTestSuite) TestInitStateManager() {
	s.NotNil(s.stateManager)
}

func (s *stateManagerTestSuite) TestAddTaskStatusUpdate() {
	var events []*pb_eventstream.Event
	event := &pb_eventstream.Event{
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: s.taskStatusUpdate.GetUpdate().GetStatus(),
	}
	events = append(events, event)
	request := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}

	s.resMgrClient.EXPECT().
		NotifyTaskUpdates(gomock.Any(), request).
		Return(&resmgrsvc.NotifyTaskUpdatesResponse{
			PurgeOffset: 1,
		}, nil)

	s.stateManager.Update(s.context, s.taskStatusUpdate)
	s.stateManager.UpdateCounters(nil)
	stateManager := s.stateManager.(*stateManager)
	s.Equal(stateManager.getTaskUpdateCount(), int64(1))

	time.Sleep(500 * time.Millisecond)
}

func (s *stateManagerTestSuite) TestAckTaskStatusUpdate() {
	var items []*cirbuf.CircularBufferItem
	item := &cirbuf.CircularBufferItem{
		SequenceID: uint64(1),
		Value:      s.event,
	}
	items = append(items, item)

	value := _frameworkID
	frameworkID := &mesos.FrameworkID{
		Value: &value,
	}
	_uuid := "d2c41522-0216-4704-8903-2945414c414c"
	callType := sched.Call_ACKNOWLEDGE
	msg := &sched.Call{
		FrameworkId: frameworkID,
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			TaskId: &mesos.TaskID{
				Value: &_uuid,
			},
			Uuid: []byte(_uuid),
			AgentId: &mesos.AgentID{
				Value: &_uuid,
			},
		},
	}
	gomock.InOrder(
		s.store.EXPECT().
			GetFrameworkID(gomock.Any(), gomock.Eq(_frameworkName)).
			Return(value, nil),
		s.store.EXPECT().
			GetMesosStreamID(gomock.Any(), gomock.Eq(_frameworkName)).
			Return(_streamID, nil),
		s.schedulerClient.EXPECT().Call(_streamID, msg).Return(nil),
	)

	s.stateManager.EventPurged(items)
	time.Sleep(500 * time.Millisecond)
}

func TestStateManager(t *testing.T) {
	suite.Run(t, new(stateManagerTestSuite))
}
