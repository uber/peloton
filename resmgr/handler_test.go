package resmgr

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/preemption/mocks"
)

const (
	timeout = 1 * time.Second
)

type HandlerTestSuite struct {
	suite.Suite
	handler       *ServiceHandler
	context       context.Context
	resTree       respool.Tree
	taskScheduler rm_task.Scheduler
	ctrl          *gomock.Controller
	rmTaskTracker rm_task.Tracker
}

func (s *HandlerTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(s.ctrl)
	mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)

	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)
	s.resTree = respool.GetTree()
	// Initializing the resmgr state machine
	rm_task.InitTaskTracker(tally.NoopScope)
	s.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(tally.NoopScope, 1*time.Second, s.rmTaskTracker)
	s.taskScheduler = rm_task.GetScheduler()

	s.handler = &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: respool.GetTree(),
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: s.rmTaskTracker,
		config: Config{
			RmTaskConfig: &rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
			},
		},
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

func (s *HandlerTestSuite) TearDownSuite() {
	s.ctrl.Finish()
	s.rmTaskTracker.Clear()
}

func (s *HandlerTestSuite) SetupTest() {
	s.context = context.Background()
	err := s.resTree.Start()
	s.NoError(err)

	err = s.taskScheduler.Start()
	s.NoError(err)

}

func (s *HandlerTestSuite) TearDownTest() {
	log.Info("tearing down")

	err := respool.GetTree().Stop()
	s.NoError(err)
	err = rm_task.GetScheduler().Stop()
	s.NoError(err)
}

func TestResManagerHandler(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

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

func (s *HandlerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

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

func (s *HandlerTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	uuidStr := "uuidstr-1"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
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
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible: true,
		},
	}
	return &gang
}

func (s *HandlerTestSuite) pendingGang1() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 1,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: "job1-2"},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			Preemptible: true,
		},
	}
	return &gang
}

func (s *HandlerTestSuite) pendingGang2() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = []*resmgr.Task{
		{
			Name:         "job2-1",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-1"},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			Preemptible: true,
		},
		{
			Name:         "job2-2",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-2"},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			Preemptible: true,
		},
	}
	return &gang
}

func (s *HandlerTestSuite) pendingGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = s.pendingGang0()
	gangs[1] = s.pendingGang1()
	gangs[2] = s.pendingGang2()
	return gangs
}

func (s *HandlerTestSuite) expectedGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = s.pendingGang2()
	gangs[1] = s.pendingGang1()
	gangs[2] = s.pendingGang0()
	return gangs
}

func (s *HandlerTestSuite) TestEnqueueDequeueGangsOneResPool() {
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   s.pendingGangs(),
	}
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)

	s.NoError(err)
	s.Nil(enqResp.GetError())

	deqReq := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)

	deqResp, err := s.handler.DequeueGangs(s.context, deqReq)
	s.NoError(err)
	s.Nil(deqResp.GetError())
	s.Equal(s.expectedGangs(), deqResp.GetGangs())
}

func (s *HandlerTestSuite) TestReEnqueueGangNonExistingGangFails() {
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		Gangs: s.pendingGangs(),
	}
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
	s.NotNil(enqResp.GetError().GetFailure().GetFailed())
}

func (s *HandlerTestSuite) TestReEnqueueGangThatFailedPlacement() {
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &peloton.ResourcePoolID{Value: "respool3"},
		Gangs:   s.pendingGangs(),
	}
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)

	// Re-enqueue the gangs without a resource pool
	enqReq.ResPool = nil
	enqResp, err = s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())

	// Make sure we dequeue the gangs again for the next test to work
	deqReq := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	// Scheduler.scheduleTasks method is run asynchronously.
	// We need to wait here
	time.Sleep(timeout)
	// Checking whether we get the task from ready queue
	deqResp, err := s.handler.DequeueGangs(s.context, deqReq)
	s.NoError(err)
	s.Nil(deqResp.GetError())
}

// This tests the requeue of the same task with same mesos task id as well
// as the different mesos task id
func (s *HandlerTestSuite) TestRequeue() {
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
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)

	// Testing to see if we can send same task in the enqueue
	// request then it should error out
	node.SetEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
	log.Error(err)
	log.Error(enqResp.GetError())
	s.EqualValues(enqResp.GetError().GetFailure().GetFailed()[0].Errorcode,
		resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST)
	// Testing to see if we can send different Mesos taskID
	// in the enqueue request then it should move task to
	// ready state and ready queue
	uuidStr := "uuidstr-2"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	enqReq.Gangs[0].Tasks[0].TaskId = &mesos_v1.TaskID{
		Value: &mesosTaskID,
	}
	enqResp, err = s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())
	s.Nil(enqResp.GetError().GetFailure().GetFailed())

	rmtask = s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	s.EqualValues(rmtask.GetCurrentState(), task.TaskState_READY)

	deqReq := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	// Checking whether we get the task from ready queue
	deqResp, err := s.handler.DequeueGangs(s.context, deqReq)
	s.NoError(err)
	s.Nil(deqResp.GetError())
	log.Info(*deqResp.GetGangs()[0].Tasks[0].TaskId.Value)
	s.Equal(mesosTaskID, *deqResp.GetGangs()[0].Tasks[0].TaskId.Value)
}

// TestRequeueTaskNotPresent tests the requeue but if the task is been
// removed from the tracker then result should be failed
func (s *HandlerTestSuite) TestRequeueTaskNotPresent() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
	s.rmTaskTracker.DeleteTask(s.pendingGang0().Tasks[0].Id)
	failed, err := s.handler.requeueTask(s.pendingGang0().Tasks[0])
	s.Error(err)
	s.NotNil(failed)
	s.EqualValues(failed.Errorcode, resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL)
}

func (s *HandlerTestSuite) TestRequeueFailures() {
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
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
	// Testing to see if we can send same task in the enqueue
	// request then it should error out
	node.SetEntitlement(s.getEntitlement())
	// Testing to see if we can send different Mesos taskID
	// in the enqueue request then it should move task to
	// ready state and ready queue
	uuidStr := "uuidstr-2"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	enqReq.Gangs[0].Tasks[0].TaskId = &mesos_v1.TaskID{
		Value: &mesosTaskID,
	}
	err = rmtask.TransitTo(task.TaskState_RUNNING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_SUCCEEDED.String())
	s.NoError(err)
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
	s.EqualValues(enqResp.GetError().GetFailure().GetFailed()[0].Errorcode,
		resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL)
}

func (s *HandlerTestSuite) TestAddingToPendingQueue() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = s.handler.addingGangToPendingQueue(s.pendingGang0(), node)
	s.Error(err)
	s.EqualValues(err.Error(), errGangNotEnqueued.Error())
}

func (s *HandlerTestSuite) TestAddingToPendingQueueFailure() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = s.handler.addingGangToPendingQueue(&resmgrsvc.Gang{}, node)
	s.Error(err)
	s.EqualValues(err.Error(), errGangNotEnqueued.Error())
	s.rmTaskTracker.Clear()
}

func (s *HandlerTestSuite) TestRequeuePlacementFailure() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())

	s.rmTaskTracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: nil,
		Gangs:   gangs,
	}

	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.NotNil(enqResp.GetError())
}

func (s *HandlerTestSuite) TestEnqueueGangsResPoolNotFound() {
	respool.InitTree(tally.NoopScope, nil, nil, nil)

	respoolID := &peloton.ResourcePoolID{Value: "respool10"}
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: respoolID,
		Gangs:   s.pendingGangs(),
	}
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	log.Infof("%v", enqResp)
	notFound := &resmgrsvc.ResourcePoolNotFound{
		Id:      respoolID,
		Message: "resource pool (respool10) not found",
	}
	s.Equal(notFound, enqResp.GetError().GetNotFound())
}

func (s *HandlerTestSuite) TestEnqueueGangsFailure() {
	// TODO: Mock ResPool.Enqueue task to simulate task enqueue failures
	s.True(true)
}

func (s *HandlerTestSuite) getPlacements() []*resmgr.Placement {
	var placements []*resmgr.Placement
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil, nil)
	for i := 0; i < 10; i++ {
		var tasks []*peloton.TaskID
		for j := 0; j < 5; j++ {
			task := &peloton.TaskID{
				Value: fmt.Sprintf("task-%d-%d", i, j),
			}
			tasks = append(tasks, task)
			s.rmTaskTracker.AddTask(&resmgr.Task{
				Id: task,
			}, nil, resp, &rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
			})
		}

		placement := &resmgr.Placement{
			Tasks:    tasks,
			Hostname: fmt.Sprintf("host-%d", i),
		}
		placements = append(placements, placement)
	}
	return placements
}

func (s *HandlerTestSuite) TestSetAndGetPlacementsSuccess() {
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

	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: s.getPlacements(),
	}
	for _, placement := range setReq.Placements {
		for _, taskID := range placement.Tasks {
			rmTask := handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
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
	s.Equal(s.getPlacements(), getResp.GetPlacements())
}

func (s *HandlerTestSuite) TestGetTasksByHosts() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: s.getPlacements(),
	}
	hostnames := make([]string, 0, len(setReq.Placements))
	for _, placement := range setReq.Placements {
		hostnames = append(hostnames, placement.Hostname)
		for _, taskID := range placement.Tasks {
			rmTask := s.handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
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
		s.Equal(len(placement.Tasks), len(res.HostTasksMap[placement.Hostname].Tasks))
	}
}

func (s *HandlerTestSuite) TestRemoveTasksFromPlacement() {
	_, tasks := s.createRMTasks()
	placement := &resmgr.Placement{
		Tasks:    tasks,
		Hostname: fmt.Sprintf("host-%d", 1),
	}
	s.Equal(len(placement.Tasks), 5)
	taskstoremove := make(map[string]*peloton.TaskID)
	for j := 0; j < 2; j++ {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("task-1-%d", j),
		}
		taskstoremove[taskID.Value] = taskID
	}
	newPlacement := s.handler.removeTasksFromPlacements(placement, taskstoremove)
	s.NotNil(newPlacement)
	s.Equal(len(newPlacement.Tasks), 3)
}

func (s *HandlerTestSuite) TestRemoveTasksFromGang() {
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

func (s *HandlerTestSuite) createRMTasks() ([]*resmgr.Task, []*peloton.TaskID) {
	var tasks []*peloton.TaskID
	var rmTasks []*resmgr.Task
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil,
		&pb_respool.ResourcePoolConfig{
			Name:      "respool1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		})
	for j := 0; j < 5; j++ {
		taskid := &peloton.TaskID{
			Value: fmt.Sprintf("task-1-%d", j),
		}
		tasks = append(tasks, taskid)
		rmTask := &resmgr.Task{
			Id: taskid,
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
		}
		s.rmTaskTracker.AddTask(rmTask, nil, resp,
			&rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
			})
		rmTasks = append(rmTasks, rmTask)
	}
	return rmTasks, tasks
}

func (s *HandlerTestSuite) TestKillTasks() {
	s.rmTaskTracker.Clear()
	_, tasks := s.createRMTasks()

	var killedtasks []*peloton.TaskID
	killedtasks = append(killedtasks, tasks[0])
	killedtasks = append(killedtasks, tasks[1])

	killReq := &resmgrsvc.KillTasksRequest{
		Tasks: killedtasks,
	}
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
	// This list have invalid task in the list which should be not
	// present in the tracker and should return error
	res, err = s.handler.KillTasks(s.context, killReq)
	s.NotNil(res.Error)
	s.NotNil(res.Error[0].NotFound)
	s.Nil(res.Error[0].KillError)
	s.Equal(res.Error[0].NotFound.Task.Value, tasks[0].Value)
}

func (s *HandlerTestSuite) TestMarkTasksLaunched() {
	s.rmTaskTracker.Clear()
	_, tasks := s.createRMTasks()

	var launchedTasks []*peloton.TaskID
	launchedTasks = append(launchedTasks, tasks[0])
	launchedTasks = append(launchedTasks, tasks[1])

	// Send valid tasks
	req := &resmgrsvc.MarkTasksLaunchedRequest{
		Tasks: launchedTasks,
	}
	_, err := s.handler.MarkTasksLaunched(s.context, req)
	s.NoError(err)
	rmTask := s.rmTaskTracker.GetTask(tasks[0])
	s.Equal(rmTask.GetCurrentState(), task.TaskState_LAUNCHED)
	rmTask = s.rmTaskTracker.GetTask(tasks[1])
	s.Equal(rmTask.GetCurrentState(), task.TaskState_LAUNCHED)

	// Send invalid tasks
	var notValidkilledtasks []*peloton.TaskID
	req = &resmgrsvc.MarkTasksLaunchedRequest{
		Tasks: notValidkilledtasks,
	}
	_, err = s.handler.MarkTasksLaunched(s.context, req)
	s.NoError(err)
}

func (s *HandlerTestSuite) TestNotifyTaskStatusUpdate() {
	var c uint64
	rm_task.InitTaskTracker(tally.NoopScope)
	handler := &ServiceHandler{
		metrics:   NewMetrics(tally.NoopScope),
		maxOffset: &c,
		rmTracker: rm_task.GetTracker(),
	}
	jobID := "test"
	rm_task.InitTaskTracker(tally.NoopScope)
	uuidStr := uuid.NewUUID().String()
	var events []*pb_eventstream.Event
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil,
		&pb_respool.ResourcePoolConfig{
			Name:      "respool1",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		})
	for i := 0; i < 100; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos_v1.TaskState_TASK_FINISHED
		status := &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		event := pb_eventstream.Event{
			Offset:          uint64(1000 + i),
			MesosTaskStatus: status,
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
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
		}, nil, resp, &rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	}
	req := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}
	response, _ := handler.NotifyTaskUpdates(context.Background(), req)
	assert.Equal(s.T(), uint64(1099), response.PurgeOffset)
	assert.Nil(s.T(), response.Error)
}

func (s *HandlerTestSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}

func (s *HandlerTestSuite) TestGetActiveTasks() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: s.getPlacements(),
	}
	for _, placement := range setReq.Placements {
		for _, taskID := range placement.Tasks {
			rmTask := s.handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
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
	s.Equal(54, totalTasks)
}

func (s *HandlerTestSuite) TestGetPreemptibleTasks() {
	defer s.handler.rmTracker.Clear()

	mockPreemptor := mocks.NewMockPreemptor(s.ctrl)
	s.handler.preemptor = mockPreemptor

	// Mock tasks in RUNNING state
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil, nil)
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
			&rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
			})
		rmTask := s.handler.rmTracker.GetTask(taskID)
		rmTask.TransitTo(task.TaskState_PENDING.String())
		rmTask.TransitTo(task.TaskState_PLACED.String())
		rmTask.TransitTo(task.TaskState_LAUNCHING.String())
		rmTask.TransitTo(task.TaskState_RUNNING.String())
	}

	var calls []*gomock.Call
	for _, et := range expectedTasks {
		calls = append(calls, mockPreemptor.EXPECT().DequeueTask(gomock.Any()).Return(et, nil))
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
	s.Equal(5, len(res.Tasks))
}

func (s *HandlerTestSuite) TestRequeueInvalidatedTasks() {
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
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
		})
	rmtask := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)

	// Marking this task to Invalidate
	// It will not invalidate as its in Lunching state
	s.rmTaskTracker.MarkItInvalid(s.pendingGang0().Tasks[0].Id)

	// Tasks should be removed from Tracker
	taskget := s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	s.Nil(taskget)

	// Testing to see if we can send same task in the enqueue
	// after invalidating the task
	node.SetEntitlement(s.getEntitlement())
	enqResp, err := s.handler.EnqueueGangs(s.context, enqReq)
	s.NoError(err)
	s.Nil(enqResp.GetError())
	// Waiting for scheduler to kick in, As the task was not
	// in initialized and pending state it will not be invalidate
	// and should be able to requeue and get to READY state
	time.Sleep(timeout)
	rmtask = s.rmTaskTracker.GetTask(s.pendingGang0().Tasks[0].Id)
	s.EqualValues(rmtask.GetCurrentState(), task.TaskState_READY)
}

func (s *HandlerTestSuite) TestGetPendingTasks() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	err = node.EnqueueGang(s.pendingGang2())
	s.NoError(err)

	req := &resmgrsvc.GetPendingTasksRequest{
		RespoolID: &peloton.ResourcePoolID{
			Value: "respool3",
		},
		Limit: uint32(1),
	}

	resp, err := s.handler.GetPendingTasks(s.context, req)
	s.NoError(err)

	s.Equal(1, len(resp.Tasks))
	for _, g := range resp.Tasks {
		s.Equal(2, len(g.TaskID))
		for i, tid := range g.TaskID {
			s.Equal(fmt.Sprintf("job2-%d", i+1), tid)
		}
	}
}
