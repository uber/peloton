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
	rm_task.InitTaskTracker(tally.NoopScope)
	suite.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(tally.NoopScope, 1*time.Second, suite.rmTaskTracker)
	suite.taskScheduler = rm_task.GetScheduler()

	suite.handler = &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: respool.GetTree(),
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
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
	suite.rmTaskTracker.Clear()
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

func (suite *HandlerTestSuite) expectedGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 3)
	gangs[0] = suite.pendingGang2()
	gangs[1] = suite.pendingGang1()
	gangs[2] = suite.pendingGang0()
	return gangs
}

func (suite *HandlerTestSuite) TestEnqueueDequeueGangsOneResPool() {
	log.Info("TestEnqueueDequeueGangsOneResPool called")

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

	deqReq := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)

	deqResp, err := suite.handler.DequeueGangs(suite.context, deqReq)
	suite.NoError(err)
	suite.Nil(deqResp.GetError())
	suite.Equal(suite.expectedGangs(), deqResp.GetGangs())

	log.Info("TestEnqueueDequeueGangsOneResPool returned")
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
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil, nil)
	for i := 0; i < 10; i++ {
		var tasks []*peloton.TaskID
		for j := 0; j < 5; j++ {
			task := &peloton.TaskID{
				Value: fmt.Sprintf("task-%d-%d", i, j),
			}
			tasks = append(tasks, task)
			suite.rmTaskTracker.AddTask(&resmgr.Task{
				Id: task,
			}, nil, resp)
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
	for _, placement := range setReq.Placements {
		for _, taskID := range placement.Tasks {
			rmTask := handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
		}
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

func (suite *HandlerTestSuite) TestGetTasksByHosts() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: suite.getPlacements(),
	}
	hostnames := make([]string, 0, len(setReq.Placements))
	for _, placement := range setReq.Placements {
		hostnames = append(hostnames, placement.Hostname)
		for _, taskID := range placement.Tasks {
			rmTask := suite.handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
		}
	}
	setResp, err := suite.handler.SetPlacements(suite.context, setReq)
	suite.NoError(err)
	suite.Nil(setResp.GetError())

	req := &resmgrsvc.GetTasksByHostsRequest{
		Hostnames: hostnames,
	}
	res, err := suite.handler.GetTasksByHosts(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(res)
	suite.Equal(len(hostnames), len(res.HostTasksMap))
	for _, hostname := range hostnames {
		_, exists := res.HostTasksMap[hostname]
		suite.True(exists)
	}
	for _, placement := range setReq.Placements {
		suite.Equal(len(placement.Tasks), len(res.HostTasksMap[placement.Hostname].Tasks))
	}
}

func (suite *HandlerTestSuite) TestRemoveTasksFromPlacement() {
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
	var tasks []*peloton.TaskID
	resp, _ := respool.NewRespool(tally.NoopScope, "respool-1", nil, nil)
	for j := 0; j < 5; j++ {
		task := &peloton.TaskID{
			Value: fmt.Sprintf("task-1-%d", j),
		}
		tasks = append(tasks, task)
		suite.rmTaskTracker.AddTask(&resmgr.Task{
			Id: task,
		}, nil, resp)
	}
	placement := &resmgr.Placement{
		Tasks:    tasks,
		Hostname: fmt.Sprintf("host-%d", 1),
	}
	suite.Equal(len(placement.Tasks), 5)
	var taskstoremove []*peloton.TaskID
	for j := 0; j < 2; j++ {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("task-1-%d", j),
		}
		taskstoremove = append(taskstoremove, taskID)
	}
	newPlacement := handler.removeTasksFromPlacements(placement, taskstoremove)
	suite.NotNil(newPlacement)
	suite.Equal(len(newPlacement.Tasks), 3)
}

func (suite *HandlerTestSuite) TestNotifyTaskStatusUpdate() {
	var c uint64
	handler := &ServiceHandler{
		metrics:   NewMetrics(tally.NoopScope),
		maxOffset: &c,
	}
	jobID := "test"
	rm_task.InitTaskTracker(tally.NoopScope)
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

func (suite *HandlerTestSuite) TestGetActiveTasks() {
	setReq := &resmgrsvc.SetPlacementsRequest{
		Placements: suite.getPlacements(),
	}
	for _, placement := range setReq.Placements {
		for _, taskID := range placement.Tasks {
			rmTask := suite.handler.rmTracker.GetTask(taskID)
			rmTask.TransitTo(task.TaskState_PENDING.String())
			rmTask.TransitTo(task.TaskState_READY.String())
			rmTask.TransitTo(task.TaskState_PLACING.String())
		}
	}
	setResp, err := suite.handler.SetPlacements(suite.context, setReq)
	suite.NoError(err)
	suite.Nil(setResp.GetError())

	req := &resmgrsvc.GetActiveTasksRequest{}
	res, err := suite.handler.GetActiveTasks(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(res)
	suite.Equal(54, len(res.TaskStatesMap))
}
