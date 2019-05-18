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

package task

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common/cirbuf"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/rpc"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
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

type stateManagerTestSuite struct {
	suite.Suite

	sync.Mutex
	ctrl         *gomock.Controller
	context      context.Context
	resMgrClient *res_mocks.MockResourceManagerServiceYARPCClient
	stateManager StateManager

	dispatcher *yarpc.Dispatcher
	testScope  tally.TestScope

	taskStatusUpdate *sched.Event
	event            *pb_eventstream.Event

	store           *storage_mocks.MockFrameworkInfoStore
	driver          hostmgr_mesos.SchedulerDriver
	schedulerClient *mpb_mocks.MockSchedulerClient
	watchProcessor  *watchmocks.MockWatchProcessor
}

func (s *stateManagerTestSuite) SetupTest() {
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
	s.watchProcessor = watchmocks.NewMockWatchProcessor(s.ctrl)

	s.resMgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(s.ctrl)
	s.testScope = tally.NewTestScope("", map[string]string{})

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
		Offset:          uint64(1),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: s.taskStatusUpdate.GetUpdate().GetStatus(),
	}
}

func (s *stateManagerTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *stateManagerTestSuite) createNewStateManager(ackConcurrency int) StateManager {
	return NewStateManager(
		s.dispatcher,
		s.schedulerClient,
		s.watchProcessor,
		10,
		ackConcurrency,
		s.resMgrClient,
		s.testScope)
}

func (s *stateManagerTestSuite) TestStatusUpdateDedupe() {
	s.stateManager = s.createNewStateManager(0)
	var items []*cirbuf.CircularBufferItem
	var item *cirbuf.CircularBufferItem
	_uuid := "59f2d54b-9688-4075-83dd-5fdf305a4f5e"
	for i := 1; i <= 5; i++ {
		item = &cirbuf.CircularBufferItem{
			SequenceID: uint64(i),
			Value:      createEvent(_uuid, i),
		}
		items = append(items, item)
	}

	s.stateManager.EventPurged(items)
	s.stateManager.UpdateCounters(nil)

	// Four ack are deduped
	s.Equal(s.testScope.Snapshot().Counters()["taskStateManager.task_update_ack_dedupe+"].Value(), int64(4))
	s.Equal(s.testScope.Snapshot().Gauges()["taskStateManager.task_ack_map_size+"].Value(), float64(1))
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

func (s *stateManagerTestSuite) TestInitStateManager() {
	s.stateManager = s.createNewStateManager(10)
	s.NotNil(s.stateManager)
}

func (s *stateManagerTestSuite) TestAddTaskStatusUpdate() {
	s.stateManager = s.createNewStateManager(10)
	var events []*pb_eventstream.Event
	var event *pb_eventstream.Event
	event = &pb_eventstream.Event{
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
	s.watchProcessor.EXPECT().NotifyEventChange(gomock.Any())

	s.stateManager.Update(s.context, s.taskStatusUpdate)
	s.stateManager.UpdateCounters(nil)

	time.Sleep(500 * time.Millisecond)
}

func (s *stateManagerTestSuite) TestAckTaskStatusUpdate() {
	s.stateManager = s.createNewStateManager(10)
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
	s.Equal(s.testScope.Snapshot().Gauges()["taskStateManager.task_ack_map_size+"].Value(), float64(0))
}

func TestStateManager(t *testing.T) {
	suite.Run(t, new(stateManagerTestSuite))
}
