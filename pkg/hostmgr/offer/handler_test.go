// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package offer

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/cirbuf"
	"github.com/uber/peloton/pkg/common/rpc"
	binpacking "github.com/uber/peloton/pkg/hostmgr/binpacking"
	config "github.com/uber/peloton/pkg/hostmgr/config"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	mesosmanager "github.com/uber/peloton/pkg/hostmgr/p2k/plugins/mesos"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"
	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"

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

type HostMgrOfferHandlerTestSuite struct {
	suite.Suite

	sync.Mutex
	ctrl         *gomock.Controller
	context      context.Context
	resMgrClient *res_mocks.MockResourceManagerServiceYARPCClient

	dispatcher *yarpc.Dispatcher
	testScope  tally.TestScope

	taskStatusUpdate *sched.Event
	event            *pb_eventstream.Event

	store           *storage_mocks.MockFrameworkInfoStore
	driver          hostmgr_mesos.SchedulerDriver
	defragRanker    binpacking.Ranker
	schedulerClient *mpb_mocks.MockSchedulerClient
	watchProcessor  *watchmocks.MockWatchProcessor
	mesosPlugin     *mesosmanager.MesosManager
}

func (s *HostMgrOfferHandlerTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())
	s.context = context.Background()

	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: outbound,
		},
	}
	s.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  nil,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})
	s.schedulerClient = mpb_mocks.NewMockSchedulerClient(s.ctrl)
	s.resMgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(s.ctrl)
	s.watchProcessor = watchmocks.NewMockWatchProcessor(s.ctrl)
	s.testScope = tally.NewTestScope("", map[string]string{})
	s.store = storage_mocks.NewMockFrameworkInfoStore(s.ctrl)
	s.defragRanker = binpacking.GetRankerByName(binpacking.FirstFit)
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
	s.mesosPlugin = mesosmanager.NewMesosManager(
		s.dispatcher, nil, s.schedulerClient, nil,
		time.Second, time.Second,
		tally.NoopScope, nil, nil)

	hmConfig := config.Config{
		OfferHoldTimeSec:              60,
		OfferPruningPeriodSec:         60,
		HostPlacingOfferStatusTimeout: 1 * time.Minute,
		HostPruningPeriodSec:          1 * time.Minute,
		HeldHostPruningPeriodSec:      1 * time.Minute,
		BinPackingRefreshIntervalSec:  1 * time.Minute,
		TaskUpdateBufferSize:          10,
		TaskUpdateAckConcurrency:      1,
	}

	InitEventHandler(
		s.dispatcher,
		s.testScope,
		s.schedulerClient,
		s.resMgrClient,
		background.NewManager(),
		s.defragRanker,
		hmConfig,
		s.watchProcessor,
		nil,
		s.mesosPlugin,
	)
	eh := GetEventHandler()
	s.NotNil(eh.GetEventStreamHandler())

	_uuid := "d2c41522-0216-4704-8903-2945414c414c"
	state := mesos.TaskState_TASK_STARTING
	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_uuid,
		},
		State: &state,
		Uuid:  []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
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
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: s.taskStatusUpdate.GetUpdate().GetStatus(),
	}
}

// Test Simple Add Mesos Task Status Update Error
func (s *HostMgrOfferHandlerTestSuite) TestAddTaskStatusUpdateError() {
	var events []*pb_eventstream.Event
	events = append(events, s.event)
	request := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}

	s.resMgrClient.EXPECT().
		NotifyTaskUpdates(gomock.Any(), request).
		Return(&resmgrsvc.NotifyTaskUpdatesResponse{
			Error: &resmgrsvc.NotifyTaskUpdatesResponse_Error{
				Error: &resmgrsvc.NotifyTaskUpdatesError{
					Message: "Resource Manager Error",
				},
			},
		}, errors.New("resource manager error"))
	s.watchProcessor.EXPECT().NotifyEventChange(gomock.Any())

	handler.Update(s.context, s.taskStatusUpdate)
	time.Sleep(500 * time.Millisecond)
}

// Test Simple Add Mesos Task Status Update
func (s *HostMgrOfferHandlerTestSuite) TestAddTaskStatusUpdate() {
	var events []*pb_eventstream.Event
	events = append(events, s.event)
	request := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}

	s.resMgrClient.EXPECT().
		NotifyTaskUpdates(gomock.Any(), request).
		Return(&resmgrsvc.NotifyTaskUpdatesResponse{
			PurgeOffset: 1,
		}, nil)
	s.watchProcessor.EXPECT().NotifyEventChange(gomock.Any())

	handler.Update(s.context, s.taskStatusUpdate)
	time.Sleep(500 * time.Millisecond)
	s.Equal(int64(1), s.testScope.Snapshot().Counters()["task_updates+"].Value())
	s.Equal(int64(1), s.testScope.Snapshot().Counters()["task_state_TASK_STARTING+"].Value())
}

// Test Simple Task Status Update Acknowledge Error
func (s *HostMgrOfferHandlerTestSuite) TestAckTaskStatusUpdateError() {
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
			Uuid: []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
			AgentId: &mesos.AgentID{
				Value: &_uuid,
			},
		},
	}

	s.store.EXPECT().
		GetFrameworkID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(value, nil).AnyTimes()
	s.store.EXPECT().
		GetMesosStreamID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(_streamID, nil).AnyTimes()
	s.schedulerClient.EXPECT().Call(_streamID, msg).Return(errors.New("Mesos Master Error"))

	handler.EventPurged(items)
	time.Sleep(500 * time.Millisecond)
}

// Test Simple Task Status Update Acknowledgement
func (s *HostMgrOfferHandlerTestSuite) TestAckTaskStatusUpdate() {
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
			Uuid: []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
			AgentId: &mesos.AgentID{
				Value: &_uuid,
			},
		},
	}

	s.store.EXPECT().
		GetFrameworkID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(value, nil).AnyTimes()
	s.store.EXPECT().
		GetMesosStreamID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(_streamID, nil).AnyTimes()
	s.schedulerClient.EXPECT().Call(_streamID, msg).Return(nil)

	handler.EventPurged(items)
	time.Sleep(500 * time.Millisecond)
	s.Equal(float64(0), s.testScope.Snapshot().Gauges()["task_ack_map_size+"].Value())
	s.Equal(int64(1), s.testScope.Snapshot().Counters()["task_update_ack+"].Value())
}

// Test Simple Task Status Update Dedupe
func (s *HostMgrOfferHandlerTestSuite) TestStatusUpdateDedupe() {
	var items []*cirbuf.CircularBufferItem
	var item *cirbuf.CircularBufferItem
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
			Uuid: []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
			AgentId: &mesos.AgentID{
				Value: &_uuid,
			},
		},
	}

	_uuid = "59f2d54b-9688-4075-83dd-5fdf305a4f5e"
	for i := 1; i <= 5; i++ {
		item = &cirbuf.CircularBufferItem{
			SequenceID: uint64(i),
			Value:      createEvent(_uuid, i),
		}
		items = append(items, item)
	}

	s.store.EXPECT().
		GetFrameworkID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(value, nil).
		AnyTimes()
	s.store.EXPECT().
		GetMesosStreamID(gomock.Any(), gomock.Eq(_frameworkName)).
		Return(_streamID, nil).
		AnyTimes()
	// Acknowledge call can happen multiple times, as multiple go-routines
	// process simultaneously
	s.schedulerClient.EXPECT().
		Call(_streamID, msg).
		Return(nil).
		MinTimes(1)

	handler.EventPurged(items)
	handler.UpdateCounters()

	// Four ack are deduped
	// Since ack uses multiple go-routines, can not use precise values for comparison
	s.True(s.testScope.Snapshot().Counters()["task_update_ack_dedupe+"].Value() <= int64(4))
	s.True(s.testScope.Snapshot().Gauges()["task_ack_map_size+"].Value() >= float64(1))

	time.Sleep(500 * time.Millisecond)
}

func createEvent(_uuid string, offset int) *pb_eventstream.Event {
	state := mesos.TaskState_TASK_STARTING
	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_uuid,
		},
		State: &state,
		Uuid:  []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
		AgentId: &mesos.AgentID{
			Value: &_uuid,
		},
	}

	taskStatusUpdate := &sched.Event{
		Update: &sched.Event_Update{
			Status: status,
		},
	}

	return &pb_eventstream.Event{
		Offset:          uint64(offset),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: taskStatusUpdate.GetUpdate().GetStatus(),
	}
}

func TestHostMgrOfferHander(t *testing.T) {
	suite.Run(t, new(HostMgrOfferHandlerTestSuite))
}
