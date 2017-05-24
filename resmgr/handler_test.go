package resmgr

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
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

func (suite *HandlerTestSuite) SetupSuite() {
	suite.ctrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.ctrl)
	mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(suite.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(suite.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.ctrl)

	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)
	suite.resTree = respool.GetTree()
	// Initializing the resmgr state machine
	rm_task.InitTaskTracker()
	suite.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(1*time.Second, suite.rmTaskTracker)
	suite.taskScheduler = rm_task.GetScheduler()

	suite.handler = &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: respool.GetTree(),
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(&resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: suite.rmTaskTracker,
	}
	suite.handler.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
}

func (suite *HandlerTestSuite) TearDownSuite() {
	suite.ctrl.Finish()
}

func (suite *HandlerTestSuite) SetupTest() {
	suite.context = context.Background()
	err := suite.resTree.Start()
	suite.NoError(err)

	err = suite.taskScheduler.Start()
	suite.NoError(err)

}

func (suite *HandlerTestSuite) TearDownTest() {
	log.Info("tearing down")

	err := respool.GetTree().Stop()
	suite.NoError(err)
	err = rm_task.GetScheduler().Stop()
	suite.NoError(err)
}

func TestResManagerHandler(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (suite *HandlerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

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

func (suite *HandlerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := pb_respool.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "root",
			Parent:    nil,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (suite *HandlerTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = []*resmgr.Task{
		{
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
		},
	}
	return &gang
}

func (suite *HandlerTestSuite) pendingGang1() *resmgrsvc.Gang {
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
		},
	}
	return &gang
}

func (suite *HandlerTestSuite) pendingGang2() *resmgrsvc.Gang {
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
		},
	}
	return &gang
}

func (suite *HandlerTestSuite) pendingGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = suite.pendingGang0()
	gangs[1] = suite.pendingGang1()
	gangs[2] = suite.pendingGang2()
	return gangs
}

func (suite *HandlerTestSuite) expectedTasks() []*resmgr.Task {
	return []*resmgr.Task{
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
		},
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
		},
		{
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
		},
	}
}

func (suite *HandlerTestSuite) TestEnqueueDequeueTasksOneResPool() {
	log.Info("TestEnqueueDequeueTasksOneResPool called")

	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: &pb_respool.ResourcePoolID{Value: "respool3"},
		Gangs:   suite.pendingGangs(),
	}
	node, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool3"})
	suite.NoError(err)
	node.SetEntitlement(suite.getEntitlement())
	enqResp, err := suite.handler.EnqueueGangs(suite.context, enqReq)

	suite.NoError(err)
	suite.Nil(enqResp.GetError())

	deqReq := &resmgrsvc.DequeueTasksRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	deqResp, err := suite.handler.DequeueTasks(suite.context, deqReq)
	suite.NoError(err)
	suite.Nil(deqResp.GetError())
	suite.Equal(suite.expectedTasks(), deqResp.GetTasks())

	log.Info("TestEnqueueDequeueTasksOneResPool returned")
}

func (suite *HandlerTestSuite) TestEnqueueGangsResPoolNotFound() {
	log.Info("TestEnqueueGangsResPoolNotFound called")
	respool.InitTree(tally.NoopScope, nil, nil, nil)

	respoolID := &pb_respool.ResourcePoolID{Value: "respool10"}
	enqReq := &resmgrsvc.EnqueueGangsRequest{
		ResPool: respoolID,
		Gangs:   suite.pendingGangs(),
	}
	enqResp, err := suite.handler.EnqueueGangs(suite.context, enqReq)
	suite.NoError(err)
	log.Infof("%v", enqResp)
	notFound := &resmgrsvc.ResourcePoolNotFound{
		Id:      respoolID,
		Message: "Resource pool (respool10) not found",
	}
	suite.Equal(notFound, enqResp.GetError().GetNotFound())
	log.Info("TestEnqueueGangsResPoolNotFound returned")
}

func (suite *HandlerTestSuite) TestEnqueueGangsFailure() {
	// TODO: Mock ResPool.Enqueue task to simulate task enqueue failures
	suite.True(true)
}

func (suite *HandlerTestSuite) getPlacements() []*resmgr.Placement {
	var placements []*resmgr.Placement
	for i := 0; i < 10; i++ {
		var tasks []*peloton.TaskID
		for j := 0; j < 5; j++ {
			task := &peloton.TaskID{
				Value: fmt.Sprintf("task-%d-%d", i, j),
			}
			tasks = append(tasks, task)
		}
		placement := &resmgr.Placement{
			Tasks:    tasks,
			Hostname: fmt.Sprintf("host-%d", i),
		}
		placements = append(placements, placement)
	}
	return placements
}

func (suite *HandlerTestSuite) TestSetAndGetPlacementsSuccess() {
	handler := &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: nil,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: suite.rmTaskTracker,
	}
	handler.eventStreamHandler = suite.handler.eventStreamHandler

	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: suite.getPlacements(),
	}
	setResp, err := handler.SetPlacements(suite.context, setReq)
	suite.NoError(err)
	suite.Nil(setResp.GetError())

	getReq := &resmgrsvc.GetPlacementsRequest{
		Limit:   10,
		Timeout: 1 * 1000, // 1 sec
	}
	getResp, err := handler.GetPlacements(suite.context, getReq)
	suite.NoError(err)
	suite.Nil(getResp.GetError())
	suite.Equal(suite.getPlacements(), getResp.GetPlacements())
}

func (suite *HandlerTestSuite) TestNotifyTaskStatusUpdate() {
	var c uint64
	handler := &ServiceHandler{
		metrics:   NewMetrics(tally.NoopScope),
		maxOffset: &c,
	}
	jobID := "test"
	rm_task.InitTaskTracker()
	uuidStr := uuid.NewUUID().String()
	var events []*pb_eventstream.Event
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
	}
	req := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}
	response, _ := handler.NotifyTaskUpdates(context.Background(), req)
	assert.Equal(suite.T(), uint64(1099), response.PurgeOffset)
	assert.Nil(suite.T(), response.Error)
}

func (suite *HandlerTestSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}
