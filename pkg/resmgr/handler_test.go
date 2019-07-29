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

package resmgr

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostsvc_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/statemachine"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	hostmover_mocks "github.com/uber/peloton/pkg/resmgr/hostmover/mocks"
	"github.com/uber/peloton/pkg/resmgr/preemption/mocks"
	"github.com/uber/peloton/pkg/resmgr/respool"
	rm "github.com/uber/peloton/pkg/resmgr/respool/mocks"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"
	task_mocks "github.com/uber/peloton/pkg/resmgr/task/mocks"
	"github.com/uber/peloton/pkg/resmgr/tasktestutil"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	timeout = 1 * time.Second
)

var (
	jobID    = "bca875f5-322a-4439-b0c9-63e3cf9f982e"
	newJobID = uuid.New()
)

type handlerTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	context context.Context

	handler           *ServiceHandler
	resTree           respool.Tree
	rmTaskTracker     rm_task.Tracker
	mockHostmgrClient *hostsvc_mocks.MockInternalHostServiceYARPCClient
	mockBatchScorer   *hostmover_mocks.MockScorer

	cfg rc.PreemptionConfig
}

func (s *handlerTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())

	mockResPoolOps := objectmocks.NewMockResPoolOps(s.ctrl)
	mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)

	s.mockHostmgrClient = hostsvc_mocks.NewMockInternalHostServiceYARPCClient(s.ctrl)
	s.mockBatchScorer = hostmover_mocks.NewMockScorer(s.ctrl)

	s.cfg = rc.PreemptionConfig{
		Enabled: false,
	}

	// setup resource pool tree
	s.resTree = respool.NewTree(
		tally.NoopScope,
		mockResPoolOps,
		mockJobStore,
		mockTaskStore,
		s.cfg)

	// Initializing the resmgr state machine
	rm_task.InitTaskTracker(
		tally.NoopScope,
		tasktestutil.CreateTaskConfig())

	s.rmTaskTracker = rm_task.GetTracker()

	// Initialize the task scheduler
	rm_task.InitScheduler(
		tally.NoopScope,
		s.resTree,
		1*time.Second,
		s.rmTaskTracker,
	)

	// Initialize the handler
	s.handler = &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: s.resTree,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: s.rmTaskTracker,
		config: Config{
			RmTaskConfig: tasktestutil.CreateTaskConfig(),
		},
		hostmgrClient: s.mockHostmgrClient,
		batchScorer:   s.mockBatchScorer,
	}
	s.handler.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
}

func (s *handlerTestSuite) TearDownSuite() {
	s.ctrl.Finish()
	s.rmTaskTracker.Clear()
}

func (s *handlerTestSuite) SetupTest() {
	s.context = context.Background()
	s.NoError(s.resTree.Start())
	s.NoError(rm_task.GetScheduler().Start())
}

func (s *handlerTestSuite) TearDownTest() {
	s.NoError(s.resTree.Stop())
	s.NoError(rm_task.GetScheduler().Stop())
	s.rmTaskTracker.Clear()
}

func TestResManagerHandler(t *testing.T) {
	suite.Run(t, new(handlerTestSuite))
}

func (s *handlerTestSuite) TestNewServiceHandler() {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonResourceManager,
		Inbounds:  nil,
		Outbounds: nil,
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})

	tracker := task_mocks.NewMockTracker(s.ctrl)
	mockPreemptionQueue := mocks.NewMockQueue(s.ctrl)
	mockHostmgrClient := hostsvc_mocks.NewMockInternalHostServiceYARPCClient(s.ctrl)
	mockBatchScorer := hostmover_mocks.NewMockScorer(s.ctrl)
	handler := NewServiceHandler(
		dispatcher,
		tally.NoopScope,
		tracker,
		mockBatchScorer,
		s.resTree,
		mockPreemptionQueue,
		mockHostmgrClient,
		Config{})
	s.NotNil(handler)

	streamHandler := s.handler.GetStreamHandler()
	s.NotNil(streamHandler)
}

func (s *handlerTestSuite) TestEnqueueDequeueGangsOneResPool() {
	gangs := s.pendingGangs()
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   gangs,
	}
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)

	s.NoError(err)
	s.Nil(enqResp.GetError())

	s.assertTasksAdmitted(gangs)
}

func (s *handlerTestSuite) TestDequeueGangsOnReservedTasks() {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = s.pendingGang0()
	gangs[1] = s.reservingGang0()
	gangs[2] = s.reservingGang1()

	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   gangs,
	}
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)

	s.NoError(err)
	s.Nil(enqResp.GetError())

	s.assertTasksAdmitted(gangs)
}

func (s *handlerTestSuite) TestReEnqueueGangThatFailedPlacement() {
	gangs := s.pendingGangs()
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   gangs,
	}
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)

	// SetFailedPlacements for re-enqueue
	s.assertSetFailedPlacement(gangs)

	s.assertTasksAdmitted(gangs)
}

// Tests that the tasks should move back to PENDING state(and the pending queue)
// after they have been tried to be placed a configured number of times.
func (s *handlerTestSuite) TestReEnqueueGangThatFailedPlacementManyTimes() {
	// setup the resource pool with enough entitlement
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	gangs := s.pendingGangsWithoutPlacement()

	// enqueue the gangs
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   gangs,
	}
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())

	// check weather all tasks have been admitted
	s.assertTasksAdmitted(gangs)

	// Return failed placements for re-enqueue
	s.assertSetFailedPlacement(gangs)

	// The tasks should move to pending state since the threshold to place it
	// has been crossed
	for _, gang := range gangs {
		// we only have 1 task per gang
		rmTask := s.handler.rmTracker.GetTask(gang.Tasks[0].Id)
		s.EqualValues(task.TaskState_PENDING, rmTask.GetCurrentState().State)
	}
}

// This tests the requeue of the same task with same mesos task id as well
// as the different mesos task id
func (s *handlerTestSuite) TestRequeue() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   gangs,
	}

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING,
		task.TaskState_LAUNCHED})

	// Testing to see if we can send same task in the enqueue
	// request then it should error out
	node.SetNonSlackEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
	s.EqualValues(enqResp.GetError().GetFailure().GetFailed()[0].Errorcode,
		resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST)

	// Testing to see if we can send different Mesos taskID
	// in the enqueue request
	uuidStr := "uuidstr-2"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	gangs[0].Tasks[0].TaskId = &mesos.TaskID{
		Value: &mesosTaskID,
	}
	enqReq.Gangs = gangs

	enqResp, err = s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())
	s.Nil(enqResp.GetError().GetFailure().GetFailed())
}

// TestRequeueTaskNotPresent tests the requeue but if the task is been
// removed from the tracker then result should be failed
func (s *handlerTestSuite) TestRequeueTaskNotPresent() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING})
	s.rmTaskTracker.DeleteTask(s.pendingGang0().Tasks[0].Id)
	failed, err := s.handler.requeueTask(s.pendingGang0().Tasks[0], node)
	s.Error(err)
	s.NotNil(failed)
	s.EqualValues(failed.Errorcode, resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL)
}

func (s *handlerTestSuite) TestRequeueFailures() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   []*resmgrsvc.Gang{s.pendingGang0()},
	}

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING})
	// Testing to see if we can send same task in the enqueue
	// request then it should error out
	node.SetNonSlackEntitlement(s.getEntitlement())
	// Testing to see if we can send different Mesos taskID
	// in the enqueue request then it should move task to
	// ready state and ready queue
	uuidStr := "uuidstr-2"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	enqReq.Gangs[0].Tasks[0].TaskId = &mesos.TaskID{
		Value: &mesosTaskID,
	}
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_RUNNING,
		task.TaskState_SUCCEEDED})
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
	s.EqualValues(enqResp.GetError().GetFailure().GetFailed()[0].Errorcode,
		resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL)
}

func (s *handlerTestSuite) TestAddingToPendingQueue() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED})
	err = s.handler.addingGangToPendingQueue(s.pendingGang0(), node)
	s.Error(err)
	s.EqualValues(err.Error(), errGangNotEnqueued.Error())
}

func (s *handlerTestSuite) TestAddingToPendingQueueFailure() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED})
	err = s.handler.addingGangToPendingQueue(&resmgrsvc.Gang{}, node)
	s.Error(err)
	s.EqualValues(err.Error(), errGangNotEnqueued.Error())
	s.rmTaskTracker.Clear()
}

// test Setting failed placement
func (s *handlerTestSuite) TestSetFailedPlacement() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	gang := s.pendingGang0()
	gangs := []*resmgrsvc.Gang{gang}
	ttask := gang.Tasks[0]
	ttask.PlacementAttemptCount = 3 // Simulate all 3 placements fail in a cycle

	// Add task to tracker and move to PLACED
	s.rmTaskTracker.AddTask(
		ttask,
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmTask := s.rmTaskTracker.GetTask(ttask.Id)
	err = rmTask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*ttask.TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
	})

	s.assertSetFailedPlacement(gangs)

	// Task should move to PENDING
	// check the task states
	for _, gang := range gangs {
		// we only have 1 task per gang
		rmTask := s.handler.rmTracker.GetTask(gang.Tasks[0].Id)
		s.EqualValues(
			task.TaskState_PENDING,
			rmTask.GetCurrentState().State,
		)
	}
}

func (s *handlerTestSuite) TestEnqueueGangsResPoolNotFound() {
	tt := []struct {
		respoolID      *peloton.ResourcePoolID
		wantErrMessage string
	}{
		{
			respoolID:      nil,
			wantErrMessage: "resource pool ID can't be nil",
		},
		{
			respoolID:      &peloton.ResourcePoolID{Value: "respool10"},
			wantErrMessage: "resource pool (respool10) not found",
		},
	}

	for _, t := range tt {
		enqReq := &resmgrsvc.EnqueueGangsRequest{
			ResPool: t.respoolID,
			Gangs:   s.pendingGangs(),
		}
		enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
		s.NoError(err)
		notFound := &resmgrsvc.ResourcePoolNotFound{
			Id:      t.respoolID,
			Message: t.wantErrMessage,
		}
		s.Equal(notFound, enqResp.GetError().GetNotFound())
	}
}

func (s *handlerTestSuite) TestEnqueueGangsFailure() {
	// TODO: Mock ResPool.Enqueue task to simulate task enqueue failures
	s.True(true)
}

func (s *handlerTestSuite) TestSetAndGetPlacementsSuccess() {
	handler := &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: nil,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: s.rmTaskTracker,
	}
	handler.eventStreamHandler = s.handler.eventStreamHandler

	placements := s.getPlacements(10, 5)
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: placements,
	}
	for _, placement := range setReq.Placements {
		for _, t := range placement.GetTaskIDs() {
			rmTask := handler.rmTracker.GetTask(t.GetPelotonTaskID())
			tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
				task.TaskState_PENDING,
				task.TaskState_READY,
				task.TaskState_PLACING})
		}
	}
	setResp, err := handler.SetPlacements(s.context, setReq)
	s.NoError(err)
	s.Nil(setResp.GetError())

	getReq := &resmgrsvc.GetPlacementsRequest{
		Limit:   10,
		Timeout: 1 * 1000, // 1 sec
	}
	getResp, err := handler.GetPlacements(s.context, getReq)
	s.NoError(err)
	s.Nil(getResp.GetError())
	s.Equal(placements, getResp.GetPlacements())
}

// TestSetPlacementsRunIDDifferentFromTracker tests the failure case of
// writing placements due to mesos task id of the task in placement being
// different from that in the tracker
func (s *handlerTestSuite) TestSetPlacementsRunIDDifferentFromTracker() {
	handler := &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: nil,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: s.rmTaskTracker,
	}
	handler.eventStreamHandler = s.handler.eventStreamHandler

	placements := s.getPlacements(1, 2)
	placement := proto.Clone(placements[0]).(*resmgr.Placement)
	// Change run id of the task
	placement.TaskIDs[0].MesosTaskID.Value = &[]string{
		fmt.Sprintf("%s-2", placement.GetTaskIDs()[0].GetPelotonTaskID().GetValue()),
	}[0]
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: []*resmgr.Placement{placement},
	}
	for _, placement := range placements {
		for _, t := range placement.GetTaskIDs() {
			rmTask := handler.rmTracker.GetTask(t.GetPelotonTaskID())
			tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
				task.TaskState_PENDING,
				task.TaskState_READY,
				task.TaskState_PLACING})
		}
	}
	setResp, err := handler.SetPlacements(s.context, setReq)
	s.NoError(err)
	s.Nil(setResp.GetError())

	getReq := &resmgrsvc.GetPlacementsRequest{
		Limit:   10,
		Timeout: 1 * 1000, // 1 sec
	}
	getResp, err := handler.GetPlacements(s.context, getReq)
	s.NoError(err)
	s.Nil(getResp.GetError())
	s.Len(getResp.GetPlacements(), 1)
	s.Len(getResp.GetPlacements()[0].GetTaskIDs(), 1)
	s.Equal(getResp.GetPlacements()[0].GetTaskIDs()[0], placements[0].GetTaskIDs()[1])
}

func (s *handlerTestSuite) TestTransitTasksInPlacement() {

	tracker := task_mocks.NewMockTracker(s.ctrl)
	s.handler.rmTracker = tracker

	placements := s.getPlacements(10, 5)

	tracker.EXPECT().GetTask(gomock.Any()).Return(nil).Times(5)

	p := s.handler.transitTasksInPlacement(placements[0],
		[]task.TaskState{
			task.TaskState_PLACED,
		},
		task.TaskState_LAUNCHING,
		"placement dequeued, waiting for launch")

	s.EqualValues(len(p.TaskIDs), 0)

	resp, err := respool.NewRespool(tally.NoopScope, "respool-1", nil, &pb_respool.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    nil,
		Resources: s.getResourceConfig(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	},
		s.cfg)
	uuidStr := uuid.NewUUID().String()
	t := &resmgr.Task{
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", uuidStr, 0),
		},
	}
	rmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t, nil,
		resp,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
		task.TaskState_PENDING,
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
	})

	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask).Times(5)
	placements = s.getPlacements(10, 5)
	p = s.handler.transitTasksInPlacement(placements[0],
		[]task.TaskState{
			task.TaskState_RUNNING,
		},
		task.TaskState_LAUNCHING,
		"placement dequeued, waiting for launch")
	s.EqualValues(len(p.TaskIDs), 0)

	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask).Times(5)
	placements = s.getPlacements(10, 5)
	p = s.handler.transitTasksInPlacement(placements[0],
		[]task.TaskState{
			task.TaskState_PLACED,
		},
		task.TaskState_RUNNING,
		"placement dequeued, waiting for launch")
	s.EqualValues(len(p.TaskIDs), 0)

	s.handler.rmTracker = rm_task.GetTracker()
	s.rmTaskTracker.Clear()
}

func (s *handlerTestSuite) TestGetTasksByHosts() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: s.getPlacements(10, 5),
	}
	hostnames := make([]string, 0, len(setReq.Placements))
	for _, placement := range setReq.Placements {
		hostnames = append(hostnames, placement.Hostname)
		for _, t := range placement.GetTaskIDs() {
			rmTask := s.handler.rmTracker.GetTask(t.GetPelotonTaskID())
			tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
				task.TaskState_PENDING,
				task.TaskState_READY,
				task.TaskState_PLACING})
		}
	}
	setResp, err := s.handler.SetPlacements(s.context, setReq)
	s.NoError(err)
	s.Nil(setResp.GetError())

	req := &resmgrsvc.GetTasksByHostsRequest{
		Hostnames: hostnames,
	}
	res, err := s.handler.GetTasksByHosts(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Equal(len(hostnames), len(res.HostTasksMap))
	for _, hostname := range hostnames {
		_, exists := res.HostTasksMap[hostname]
		s.True(exists)
	}
	for _, placement := range setReq.Placements {
		s.Equal(len(placement.TaskIDs), len(res.HostTasksMap[placement.Hostname].Tasks))
	}
}

func (s *handlerTestSuite) TestRemoveTasksFromPlacement() {
	rmTasks, _ := s.createRMTasks()
	placement := &resmgr.Placement{
		TaskIDs:  getPlacementTasks(rmTasks),
		Hostname: fmt.Sprintf("host-%d", 1),
	}
	s.Equal(len(placement.GetTaskIDs()), 5)
	tasksToRemove := make(map[string]struct{})
	for j := 0; j < 2; j++ {
		tasksToRemove[rmTasks[j].GetTaskId().GetValue()] = struct{}{}
	}
	newPlacement := s.handler.removeTasksFromPlacements(placement, tasksToRemove)
	s.NotNil(newPlacement)
	s.Equal(len(newPlacement.GetTaskIDs()), 3)
}

func (s *handlerTestSuite) TestRemoveTasksFromGang() {
	rmtasks, _ := s.createRMTasks()
	gang := &resmgrsvc.Gang{
		Tasks: rmtasks,
	}
	s.Equal(len(gang.Tasks), 5)
	tasksToRemove := make(map[string]*resmgr.Task)
	tasksToRemove[rmtasks[0].Id.Value] = rmtasks[0]
	tasksToRemove[rmtasks[1].Id.Value] = rmtasks[1]
	newGang := s.handler.removeFromGang(gang, tasksToRemove)
	s.NotNil(newGang)
	s.Equal(len(newGang.Tasks), 3)
}

func (s *handlerTestSuite) TestKillTasks() {
	s.rmTaskTracker.Clear()
	_, tasks := s.createRMTasks()

	var killedtasks []*peloton.TaskID
	killedtasks = append(killedtasks, tasks[0])
	killedtasks = append(killedtasks, tasks[1])

	killReq := &resmgrsvc.KillTasksRequest{
		Tasks: killedtasks,
	}
	s.mockHostmgrClient.EXPECT().
		ReleaseHostsHeldForTasks(
			gomock.Any(),
			&hostsvc.ReleaseHostsHeldForTasksRequest{
				Ids: killReq.GetTasks(),
			},
		).
		Return(&hostsvc.ReleaseHostsHeldForTasksResponse{}, nil)
	// This is a valid list tasks should be deleted
	// Result is no error and tracker should have remaining 3 tasks
	res, err := s.handler.KillTasks(s.context, killReq)
	s.NoError(err)
	s.Nil(res.Error)
	s.Equal(s.rmTaskTracker.GetSize(), int64(3))

	var notValidkilledtasks []*peloton.TaskID
	killReq = &resmgrsvc.KillTasksRequest{
		Tasks: notValidkilledtasks,
	}
	// This list does not have any tasks in the list
	// this should return error.
	res, err = s.handler.KillTasks(s.context, killReq)
	s.NotNil(res.Error)
	notValidkilledtasks = append(notValidkilledtasks, tasks[0])

	killReq = &resmgrsvc.KillTasksRequest{
		Tasks: notValidkilledtasks,
	}
	s.mockHostmgrClient.EXPECT().
		ReleaseHostsHeldForTasks(
			gomock.Any(),
			&hostsvc.ReleaseHostsHeldForTasksRequest{
				Ids: killReq.GetTasks(),
			},
		).
		Return(&hostsvc.ReleaseHostsHeldForTasksResponse{}, nil)
	// This list have invalid task in the list which should be not
	// present in the tracker and should return error
	res, err = s.handler.KillTasks(s.context, killReq)
	s.NotNil(res.Error)
	s.NotNil(res.Error[0].NotFound)
	s.Nil(res.Error[0].KillError)
	s.Equal(res.Error[0].NotFound.Task.Value, tasks[0].Value)
}

// TestUpdateTasksState tests the update tasks by state
func (s *handlerTestSuite) TestUpdateTasksState() {
	s.rmTaskTracker.Clear()
	// Creating the New set of Tasks and add them to the tracker
	rmTasks, _ := s.createRMTasks()
	invalidMesosID := "invalid_mesos_id"

	testTable := []struct {
		updateStateRequestTask *resmgr.Task
		updatedTaskState       task.TaskState
		mesosTaskID            *mesos.TaskID
		expectedState          task.TaskState
		expectedTask           string
		msg                    string
		deletefromTracker      bool
	}{
		{
			msg:                    "Testing valid tasks, test if the state reached to LAUNCHED",
			updateStateRequestTask: rmTasks[0],
			updatedTaskState:       task.TaskState_UNKNOWN,
			mesosTaskID:            nil,
			expectedState:          task.TaskState_LAUNCHED,
			expectedTask:           "",
		},
		{
			msg:                    "Testing update the same task, It should not update the Launched state",
			updateStateRequestTask: rmTasks[0],
			updatedTaskState:       task.TaskState_UNKNOWN,
			mesosTaskID:            nil,
			expectedState:          task.TaskState_LAUNCHED,
			expectedTask:           "",
		},
		{
			msg:                    "Testing if we pass the nil Resource Manager Task",
			updateStateRequestTask: nil,
			updatedTaskState:       task.TaskState_UNKNOWN,
			mesosTaskID:            nil,
			expectedState:          task.TaskState_UNKNOWN,
			expectedTask:           "",
		},
		{
			msg:                    "Testing RMtask with invalid mesos task id with Terminal state",
			updateStateRequestTask: rmTasks[0],
			updatedTaskState:       task.TaskState_KILLED,
			mesosTaskID: &mesos.TaskID{
				Value: &invalidMesosID,
			},
			expectedState: task.TaskState_UNKNOWN,
			expectedTask:  "",
		},
		{
			msg:                    "Testing RMtask with invalid mesos task id.",
			updateStateRequestTask: rmTasks[0],
			updatedTaskState:       task.TaskState_UNKNOWN,
			mesosTaskID: &mesos.TaskID{
				Value: &invalidMesosID,
			},
			expectedState: task.TaskState_UNKNOWN,
			expectedTask:  "not_nil",
		},
		{
			msg:                    "Testing RMtask with Terminal State",
			updateStateRequestTask: rmTasks[0],
			updatedTaskState:       task.TaskState_KILLED,
			mesosTaskID:            nil,
			expectedState:          task.TaskState_UNKNOWN,
			expectedTask:           "nil",
		},
		{
			msg:                    "Testing RMtask with Task deleted from tracker",
			updateStateRequestTask: rmTasks[1],
			updatedTaskState:       task.TaskState_UNKNOWN,
			mesosTaskID:            nil,
			expectedState:          task.TaskState_UNKNOWN,
			expectedTask:           "nil",
			deletefromTracker:      true,
		},
	}

	for _, tt := range testTable {
		if tt.deletefromTracker {
			s.rmTaskTracker.DeleteTask(tt.updateStateRequestTask.Id)
		}
		req := s.createUpdateTasksStateRequest(tt.updateStateRequestTask)
		if tt.mesosTaskID != nil {
			req.GetTaskStates()[0].MesosTaskId = tt.mesosTaskID
		}
		if tt.updatedTaskState != task.TaskState_UNKNOWN {
			req.GetTaskStates()[0].State = tt.updatedTaskState
		}
		_, err := s.handler.UpdateTasksState(s.context, req)
		s.NoError(err)
		if tt.expectedState != task.TaskState_UNKNOWN {
			s.Equal(tt.expectedState, s.rmTaskTracker.GetTask(
				tt.updateStateRequestTask.Id).GetCurrentState().State)
		}
		switch tt.expectedTask {
		case "nil":
			s.Nil(s.rmTaskTracker.GetTask(tt.updateStateRequestTask.Id))
		case "not_nil":
			s.NotNil(s.rmTaskTracker.GetTask(tt.updateStateRequestTask.Id))
		default:
			break
		}
	}
}

func (s *handlerTestSuite) TestNotifyTaskStatusUpdate() {
	var c uint64
	rm_task.InitTaskTracker(
		tally.NoopScope,
		tasktestutil.CreateTaskConfig())
	handler := &ServiceHandler{
		metrics:   NewMetrics(tally.NoopScope),
		maxOffset: &c,
		rmTracker: rm_task.GetTracker(),
	}
	jobID := uuid.New()
	var events []*pb_eventstream.Event
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil,
		&pb_respool.ResourcePoolConfig{
			Name:      "respool1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)
	for i := 0; i < 100; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-1", jobID, i)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		event := pb_eventstream.Event{
			Offset:          uint64(1000 + i),
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		}
		events = append(events, &event)
		ptask := &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", jobID, i),
		}

		handler.rmTracker.AddTask(&resmgr.Task{
			Id: ptask,
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
		}, nil, resp, tasktestutil.CreateTaskConfig())
	}
	req := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}
	response, _ := handler.NotifyTaskUpdates(context.Background(), req)
	assert.Equal(s.T(), uint64(1099), response.PurgeOffset)
	assert.Nil(s.T(), response.Error)
}

func (s *handlerTestSuite) TestHandleEventError() {
	tracker := task_mocks.NewMockTracker(s.ctrl)
	s.handler.rmTracker = tracker

	var c uint64
	s.handler.maxOffset = &c

	jobID := uuid.New()
	var events []*pb_eventstream.Event

	for i := 0; i < 1; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-1", jobID, i)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		event := pb_eventstream.Event{
			Offset:          uint64(1000 + i),
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		}
		events = append(events, &event)
	}

	req := &resmgrsvc.NotifyTaskUpdatesRequest{}
	// testing empty events
	response, err := s.handler.NotifyTaskUpdates(context.Background(), req)
	s.NoError(err)

	req = &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}

	tracker.EXPECT().GetTask(gomock.Any()).Return(nil)
	tracker.EXPECT().
		GetOrphanTask(events[0].GetMesosTaskStatus().GetTaskId().GetValue()).
		Return(nil)
	response, _ = s.handler.NotifyTaskUpdates(context.Background(), req)

	s.EqualValues(uint64(1000), response.PurgeOffset)
	s.Nil(response.Error)

	resp, _ := respool.NewRespool(
		tally.NoopScope, "respool-1", nil, &pb_respool.ResourcePoolConfig{
			Name:      "respool-1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)

	mesosTaskID := fmt.Sprintf("%s-%d-1", jobID, 0)
	t := &resmgr.Task{
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", jobID, 0),
		},
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	rmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t,
		nil,
		resp,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)
	s.NoError(err)

	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask)
	tracker.EXPECT().MarkItDone(gomock.Any()).Return(nil)
	response, _ = s.handler.NotifyTaskUpdates(context.Background(), req)
	s.EqualValues(uint64(1000), response.PurgeOffset)
	s.Nil(response.Error)

	// Testing wrong mesos task id
	wmesosTaskID := fmt.Sprintf("%s-%d-%s", "job1", 0, jobID)
	t = &resmgr.Task{
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", jobID, 0),
		},
		TaskId: &mesos.TaskID{
			Value: &wmesosTaskID,
		},
	}
	wrmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t,
		nil,
		resp,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)
	s.NoError(err)

	tracker.EXPECT().GetTask(gomock.Any()).Return(wrmTask)
	tracker.EXPECT().
		GetOrphanTask(events[0].GetMesosTaskStatus().GetTaskId().GetValue()).
		Return(nil)
	response, _ = s.handler.NotifyTaskUpdates(context.Background(), req)
	s.EqualValues(uint64(1000), response.PurgeOffset)
	s.Nil(response.Error)

	// Testing with markitdone error
	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask)
	tracker.EXPECT().MarkItDone(gomock.Any()).Return(errors.New("error"))
	response, _ = s.handler.NotifyTaskUpdates(context.Background(), req)
	s.EqualValues(uint64(1000), response.PurgeOffset)
	s.Nil(response.Error)

	s.handler.rmTracker = rm_task.GetTracker()
}

func (s *handlerTestSuite) TestHandleRunningEventError() {
	tracker := task_mocks.NewMockTracker(s.ctrl)
	s.handler.rmTracker = tracker

	var c uint64
	s.handler.maxOffset = &c

	uuidStr := uuid.NewUUID().String()
	var events []*pb_eventstream.Event

	for i := 0; i < 1; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", uuidStr, i, uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		event := pb_eventstream.Event{
			Offset:          uint64(1000 + i),
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		}
		events = append(events, &event)
	}

	req := &resmgrsvc.NotifyTaskUpdatesRequest{}

	req = &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}

	// Testing with task state running
	resp, _ := respool.NewRespool(
		tally.NoopScope, "respool-1", nil, &pb_respool.ResourcePoolConfig{
			Name:      "respool-1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)

	mesosTaskID := fmt.Sprintf("%s-%d-%s", uuidStr, 0, uuidStr)

	t := &resmgr.Task{
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", uuidStr, 0),
		},
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	rmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t,
		nil,
		resp,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
		task.TaskState_PENDING,
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING,
		task.TaskState_RUNNING,
	})

	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask)
	response, _ := s.handler.NotifyTaskUpdates(context.Background(), req)

	s.EqualValues(uint64(1000), response.PurgeOffset)
	s.Nil(response.Error)

	s.handler.rmTracker = rm_task.GetTracker()

}

func (s *handlerTestSuite) TestGetActiveTasks() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: s.getPlacements(10, 5),
	}
	for _, placement := range setReq.Placements {
		for _, t := range placement.GetTaskIDs() {
			rmTask := s.handler.rmTracker.GetTask(t.GetPelotonTaskID())
			tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
				task.TaskState_PENDING,
				task.TaskState_READY,
				task.TaskState_PLACING,
			})
		}
	}
	setResp, err := s.handler.SetPlacements(s.context, setReq)
	s.NoError(err)
	s.Nil(setResp.GetError())

	req := &resmgrsvc.GetActiveTasksRequest{}
	res, err := s.handler.GetActiveTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	totalTasks := 0
	for _, tasks := range res.GetTasksByState() {
		totalTasks += len(tasks.GetTaskEntry())
	}
	s.Equal(50, totalTasks)
}

func (s *handlerTestSuite) TestGetActiveTasksValidateResponseFields() {
	placements := s.getPlacements(1, 1)
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: placements,
	}

	rmTask := s.handler.rmTracker.GetTask(
		placements[0].GetTaskIDs()[0].GetPelotonTaskID(),
	)
	tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
		task.TaskState_PENDING,
		task.TaskState_READY,
		task.TaskState_PLACING,
	})
	setResp, err := s.handler.SetPlacements(s.context, setReq)
	s.NoError(err)
	s.Nil(setResp.GetError())

	req := &resmgrsvc.GetActiveTasksRequest{}
	res, err := s.handler.GetActiveTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Len(res.GetTasksByState(), 1)
	for _, tasks := range res.GetTasksByState() {
		s.Len(tasks.GetTaskEntry(), 1)
		t := tasks.GetTaskEntry()[0]
		s.Equal(placements[0].GetTaskIDs()[0].GetMesosTaskID().GetValue(), t.GetTaskID())
		s.Equal(task.TaskState_PLACED.String(), t.GetTaskState())
		s.Equal(placements[0].GetHostname(), t.GetHostname())
	}
}

func (s *handlerTestSuite) TestGetPreemptibleTasks() {
	defer s.handler.rmTracker.Clear()

	mockPreemptionQueue := mocks.NewMockQueue(s.ctrl)
	s.handler.preemptionQueue = mockPreemptionQueue

	// Mock tasks in RUNNING state
	resp, err := respool.NewRespool(
		tally.NoopScope,
		"respool-1",
		nil,
		&pb_respool.ResourcePoolConfig{
			Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		},
		s.cfg,
	)
	s.NoError(err, "create resource pool should not fail")

	var expectedTasks []*resmgr.Task
	for j := 1; j <= 5; j++ {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("task-test-dequque-preempt-%d-%d", j, j),
		}
		expectedTasks = append(expectedTasks, &resmgr.Task{
			Id: taskID,
		})
		s.rmTaskTracker.AddTask(&resmgr.Task{
			Id: taskID,
		}, nil, resp,
			tasktestutil.CreateTaskConfig())
		rmTask := s.handler.rmTracker.GetTask(taskID)
		tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
			task.TaskState_PLACING,
			task.TaskState_PLACED,
			task.TaskState_LAUNCHING,
			task.TaskState_RUNNING,
		})
	}

	var calls []*gomock.Call
	for _, et := range expectedTasks {
		calls = append(calls, mockPreemptionQueue.
			EXPECT().
			DequeueTask(gomock.Any()).
			Return(&resmgr.PreemptionCandidate{
				Id:     et.Id,
				Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
			}, nil))
	}
	gomock.InOrder(calls...)

	// Make RPC request
	req := &resmgrsvc.GetPreemptibleTasksRequest{
		Timeout: 100,
		Limit:   5,
	}
	res, err := s.handler.GetPreemptibleTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Equal(5, len(res.PreemptionCandidates))
}

func (s *handlerTestSuite) TestGetPreemptibleTasksError() {
	tracker := task_mocks.NewMockTracker(s.ctrl)
	mockPreemptionQueue := mocks.NewMockQueue(s.ctrl)
	s.handler.preemptionQueue = mockPreemptionQueue
	s.handler.rmTracker = tracker

	// Mock tasks in RUNNING state
	resp, _ := respool.NewRespool(
		tally.NoopScope, "respool-1", nil, &pb_respool.ResourcePoolConfig{
			Name:      "respool-1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)
	t := &resmgr.Task{
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job1-", 0),
		},
	}
	rmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t,
		nil,
		resp,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)

	s.NoError(err)
	tracker.EXPECT().AddTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return(nil)

	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask)

	var expectedTasks []*resmgr.Task
	for j := 1; j <= 1; j++ {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("task-test-dequque-preempt-%d-%d", j, j),
		}
		expectedTasks = append(expectedTasks, &resmgr.Task{
			Id: taskID,
		})
		tracker.AddTask(&resmgr.Task{
			Id: taskID,
		}, nil, resp,
			tasktestutil.CreateTaskConfig())
		rmTask := tracker.GetTask(taskID)
		tasktestutil.ValidateStateTransitions(rmTask, []task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
			task.TaskState_PLACING,
			task.TaskState_PLACED,
			task.TaskState_LAUNCHING,
		})
	}

	mockPreemptionQueue.EXPECT().DequeueTask(gomock.Any()).Return(nil, errors.New("error"))

	// Make RPC request
	req := &resmgrsvc.GetPreemptibleTasksRequest{
		Timeout: 100,
		Limit:   1,
	}
	res, err := s.handler.GetPreemptibleTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Equal(0, len(res.PreemptionCandidates))

	mockPreemptionQueue.EXPECT().DequeueTask(gomock.Any()).Return(&resmgr.PreemptionCandidate{
		Id:     expectedTasks[0].Id,
		Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
	}, nil)
	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask)
	res, err = s.handler.GetPreemptibleTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Equal(0, len(res.PreemptionCandidates))

	mockPreemptionQueue.EXPECT().DequeueTask(gomock.Any()).Return(&resmgr.PreemptionCandidate{
		Id:     expectedTasks[0].Id,
		Reason: resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
	}, nil)
	tracker.EXPECT().GetTask(gomock.Any()).Return(nil)
	res, err = s.handler.GetPreemptibleTasks(context.Background(), req)
	s.NoError(err)
	s.NotNil(res)
	s.Equal(0, len(res.PreemptionCandidates))
	s.handler.rmTracker = rm_task.GetTracker()
}

func (s *handlerTestSuite) TestAddTaskError() {
	tracker := task_mocks.NewMockTracker(s.ctrl)
	s.handler.rmTracker = tracker

	tracker.EXPECT().AddTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return(errors.New("error"))
	resp, _ := respool.NewRespool(
		tally.NoopScope, "respool-1", nil, &pb_respool.ResourcePoolConfig{
			Name:      "respool-1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)

	response, err := s.handler.addTask(&resmgr.Task{}, resp)
	s.Error(err)
	s.Equal(response.Message, "error")
	s.handler.rmTracker = rm_task.GetTracker()
}

func (s *handlerTestSuite) TestRequeueInvalidatedTasks() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   []*resmgrsvc.Gang{s.pendingGang0()},
	}

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		tasktestutil.CreateTaskConfig())
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo(mesosTaskID,
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	tasktestutil.ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING,
	})

	// Marking this task to Invalidate
	// It will not invalidate as its in Launching state
	s.rmTaskTracker.MarkItInvalid(s.pendingGang0().Tasks[0].GetTaskId().GetValue())

	// Tasks should be removed from Tracker
	taskget := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].GetId())
	s.Nil(taskget)

	// Testing to see if we can send same task in the enqueue
	// after invalidating the task
	node.SetNonSlackEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())
	// Waiting for scheduler to kick in, As the task was not
	// in initialized and pending state it will not be invalidate
	// and should be able to requeue and get to READY state
	time.Sleep(timeout)
	rmtask = s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
}

func (s *handlerTestSuite) TestGetPendingTasks() {
	respoolID := &peloton.ResourcePoolID{Value: "respool3"}
	limit := uint32(1)

	mr := rm.NewMockResPool(s.ctrl)
	mr.EXPECT().IsLeaf().Return(true)
	mr.EXPECT().PeekGangs(respool.PendingQueue, limit).Return([]*resmgrsvc.Gang{
		{
			Tasks: []*resmgr.Task{
				{
					Id: &peloton.TaskID{Value: "pendingqueue-job"},
				},
			},
		},
	}, nil).Times(1)
	mr.EXPECT().PeekGangs(respool.NonPreemptibleQueue, limit).Return([]*resmgrsvc.Gang{
		{
			Tasks: []*resmgr.Task{
				{
					Id: &peloton.TaskID{Value: "npqueue-job"},
				},
			},
		},
	}, nil).Times(1)
	mr.EXPECT().PeekGangs(respool.ControllerQueue, limit).Return([]*resmgrsvc.Gang{
		{
			Tasks: []*resmgr.Task{
				{
					Id: &peloton.TaskID{Value: "controllerqueue-job"},
				},
			},
		},
	}, nil).Times(1)
	mr.EXPECT().PeekGangs(respool.RevocableQueue, limit).Return([]*resmgrsvc.Gang{
		{
			Tasks: []*resmgr.Task{
				{
					Id: &peloton.TaskID{Value: "revocablequeue-job"},
				},
			},
		},
	}, nil).Times(1)

	mt := rm.NewMockTree(s.ctrl)
	mt.EXPECT().Get(respoolID).Return(mr, nil).Times(1)

	req := &resmgrsvc.GetPendingTasksRequest{
		RespoolID: respoolID,
		Limit:     limit,
	}

	handler := &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: mt,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: s.rmTaskTracker,
		config: Config{
			RmTaskConfig: tasktestutil.CreateTaskConfig(),
		},
	}

	resp, err := handler.GetPendingTasks(s.context, req)
	s.NoError(err)

	s.Equal(4, len(resp.GetPendingGangsByQueue()))
	for q, gangs := range resp.GetPendingGangsByQueue() {
		s.Equal(1, len(gangs.GetPendingGangs()))
		expectedTaskID := ""
		switch q {
		case "controller":
			expectedTaskID = "controllerqueue-job"
		case "non-preemptible":
			expectedTaskID = "npqueue-job"
		case "pending":
			expectedTaskID = "pendingqueue-job"
		case "revocable":
			expectedTaskID = "revocablequeue-job"
		}

		for _, gang := range gangs.GetPendingGangs() {
			for _, tid := range gang.GetTaskIDs() {
				s.Equal(expectedTaskID, tid)
			}
		}
	}
}

func (s *handlerTestSuite) TestGetOrphanTasks() {
	rmTasks, _ := s.createRMTasks()
	for _, t := range rmTasks {
		rmTask := s.rmTaskTracker.GetTask(t.GetId())
		s.NoError(rmTask.TransitTo(task.TaskState_RUNNING.String()))
		newTask := proto.Clone(t).(*resmgr.Task)
		oldTaskID := t.GetTaskId().GetValue()
		newTask.TaskId.Value = &[]string{oldTaskID[:len(oldTaskID)-1] + "2"}[0]
		s.NoError(s.rmTaskTracker.AddTask(newTask, nil, rmTask.Respool(), tasktestutil.CreateTaskConfig()))
	}

	resp, err := s.handler.GetOrphanTasks(s.context, &resmgrsvc.GetOrphanTasksRequest{})
	s.NoError(err)
	s.Len(resp.GetOrphanTasks(), len(rmTasks))

	rmTaskMap := make(map[string]*resmgr.Task)
	for _, t := range rmTasks {
		rmTaskMap[t.GetTaskId().GetValue()] = t
	}

	for _, t := range resp.GetOrphanTasks() {
		s.Equal(rmTaskMap[t.GetTaskId().GetValue()], t)
	}
}

func (s *handlerTestSuite) TestGetHostsByScores() {
	hosts := []string{"host1", "host2"}
	s.mockBatchScorer.EXPECT().GetHostsByScores(gomock.Any()).Return(hosts)

	resp, err := s.handler.GetHostsByScores(
		s.context,
		&resmgrsvc.GetHostsByScoresRequest{Limit: 10})
	s.NoError(err)
	s.NotNil(resp)

	s.Equal(hosts, resp.Hosts)
}

// Test helpers
// -----------------

func (s *handlerTestSuite) getEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 1000,
		DISK:   100,
		GPU:    2,
	}
}

func (s *handlerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "root",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (s *handlerTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID, instance, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func (s *handlerTestSuite) pendingGang1() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	instance := 2
	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID, instance, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 1,
			JobId:    &peloton.JobID{Value: jobID},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			Preemptible: true,
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func (s *handlerTestSuite) pendingGang2() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	mesosTaskID1 := fmt.Sprintf("%s-%d-%d", jobID, 1, 1)
	mesosTaskID2 := fmt.Sprintf("%s-%d-%d", jobID, 2, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:         "job2-1",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: newJobID},
			Id:           &peloton.TaskID{Value: fmt.Sprintf("%s-%d", newJobID, 1)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID1,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
		{
			Name:         "job2-2",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: newJobID},
			Id:           &peloton.TaskID{Value: fmt.Sprintf("%s-%d", newJobID, 2)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID2,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func (s *handlerTestSuite) reservingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	jobID := uuid.New()
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID, instance, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job10-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: jobID},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     3,
			PlacementAttemptCount:   3,
			ReadyForHostReservation: true,
		},
	}
	return &gang
}

func (s *handlerTestSuite) reservingGang1() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	jobID := uuid.New()
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID, instance, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job11-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: jobID},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     3,
			PlacementAttemptCount:   3,
			ReadyForHostReservation: true,
		},
	}
	return &gang
}

func (s *handlerTestSuite) pendingGangWithoutPlacement() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	jobID := uuid.New()
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID, instance, 1)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job9-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: jobID},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     0,
			PlacementAttemptCount:   2,
		},
	}
	return &gang
}

func (s *handlerTestSuite) pendingGangsWithoutPlacement() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 1)
	gangs[0] = s.pendingGangWithoutPlacement()
	return gangs
}

func (s *handlerTestSuite) pendingGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = s.pendingGang0()
	gangs[1] = s.pendingGang1()
	gangs[2] = s.pendingGang2()
	return gangs
}

func (s *handlerTestSuite) expectedGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = s.pendingGang2()
	gangs[1] = s.pendingGang1()
	gangs[2] = s.pendingGang0()
	return gangs
}

func (s *handlerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
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

// asserts that the tasks have been admitted and move to PLACING after dequeue
func (s *handlerTestSuite) assertTasksAdmitted(gangs []*resmgrsvc.Gang) {
	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	// TODO Fix
	time.Sleep(2 * time.Second)

	// check the task states
	for _, gang := range gangs {
		// we only have 1 task per gang
		rmTask := s.handler.rmTracker.GetTask(gang.Tasks[0].Id)
		s.EqualValues(
			rmTask.GetCurrentState().State,
			task.TaskState_READY,
		)
	}

	deqReq := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}

	// Checking whether we get the task from ready queue
	deqResp, err := s.handler.DequeueGangs(s.context, deqReq)
	s.NoError(err)
	s.Nil(deqResp.GetError())

	// check the task states
	for _, gang := range gangs {
		// we only have 1 task per gang
		rmTask := s.handler.rmTracker.GetTask(gang.Tasks[0].Id)
		if rmTask.Task().ReadyForHostReservation {
			s.EqualValues(
				rmTask.GetCurrentState().State,
				task.TaskState_RESERVED,
			)
		} else {
			s.EqualValues(
				rmTask.GetCurrentState().State,
				task.TaskState_PLACING,
			)
		}
	}
}

// asserts the failed placements call
func (s *handlerTestSuite) assertSetFailedPlacement(gangs []*resmgrsvc.Gang) {
	var failedPlacements []*resmgrsvc.SetPlacementsRequest_FailedPlacement
	for _, gang := range gangs {
		failedPlacements = append(
			failedPlacements,
			&resmgrsvc.SetPlacementsRequest_FailedPlacement{
				Gang: gang,
			})
	}
	setPlacementReq := &resmgrsvc.SetPlacementsRequest{
		FailedPlacements: failedPlacements,
	}
	setPlacementResp, err := s.handler.SetPlacements(s.context, setPlacementReq)
	s.NoError(err)
	s.Nil(setPlacementResp.GetError())
}

func (s *handlerTestSuite) getPlacements(numJobs int, numTasks int) []*resmgr.Placement {
	var placements []*resmgr.Placement
	resp, err := respool.NewRespool(
		tally.NoopScope,
		"respool-1",
		nil,
		&pb_respool.ResourcePoolConfig{
			Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		},
		s.cfg,
	)
	s.NoError(err, "create resource pool shouldn't fail")

	for i := 0; i < numJobs; i++ {
		jobID := uuid.New()
		var placementTasks []*resmgr.Placement_Task
		var pelotonTasks []*peloton.TaskID
		for j := 0; j < numTasks; j++ {
			pelotonTask := &peloton.TaskID{
				Value: fmt.Sprintf("%s-%d", jobID, j),
			}
			pelotonTasks = append(pelotonTasks, pelotonTask)

			mesosTask := &mesos.TaskID{Value: &[]string{fmt.Sprintf("%s-%d-1", jobID, j)}[0]}

			placementTasks = append(placementTasks, &resmgr.Placement_Task{
				PelotonTaskID: pelotonTask,
				MesosTaskID:   mesosTask,
			})

			s.rmTaskTracker.AddTask(&resmgr.Task{
				Id:     pelotonTask,
				TaskId: mesosTask,
			}, nil, resp, tasktestutil.CreateTaskConfig())
		}

		placement := &resmgr.Placement{
			TaskIDs:  placementTasks,
			Hostname: fmt.Sprintf("host-%d", i),
		}
		placements = append(placements, placement)
	}
	return placements
}

func (s *handlerTestSuite) createRMTasks() ([]*resmgr.Task, []*peloton.TaskID) {
	var tasks []*peloton.TaskID
	var rmTasks []*resmgr.Task
	jobID := uuid.New()
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil,
		&pb_respool.ResourcePoolConfig{
			Name:      "respool1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		}, s.cfg)
	for j := 0; j < 5; j++ {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", jobID, j),
		}
		tasks = append(tasks, taskID)
		rmTask := &resmgr.Task{
			Id: taskID,
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos.TaskID{
				Value: &[]string{taskID.GetValue() + "-1"}[0],
			},
		}
		s.rmTaskTracker.AddTask(rmTask, nil, resp,
			tasktestutil.CreateTaskConfig())
		rmTasks = append(rmTasks, rmTask)
	}
	return rmTasks, tasks
}

// createUpdateTasksStateRequest creates UpdateTaskstate Request
// based on the resource manager task specified
func (s *handlerTestSuite) createUpdateTasksStateRequest(
	rmTask *resmgr.Task,
) *resmgrsvc.UpdateTasksStateRequest {
	taskList := make([]*resmgrsvc.UpdateTasksStateRequest_UpdateTaskStateEntry, 0, 1)
	if rmTask != nil {
		taskList = append(taskList, &resmgrsvc.UpdateTasksStateRequest_UpdateTaskStateEntry{
			State:       task.TaskState_LAUNCHED,
			MesosTaskId: rmTask.GetTaskId(),
			Task:        rmTask.GetId(),
		})
	}
	return &resmgrsvc.UpdateTasksStateRequest{
		TaskStates: taskList,
	}
}

func getPlacementTasks(rmTasks []*resmgr.Task) []*resmgr.Placement_Task {
	var placementTasks []*resmgr.Placement_Task
	for _, rmTask := range rmTasks {
		placementTasks = append(placementTasks, &resmgr.Placement_Task{
			PelotonTaskID: rmTask.GetId(),
			MesosTaskID:   rmTask.GetTaskId(),
		})
	}
	return placementTasks
}
