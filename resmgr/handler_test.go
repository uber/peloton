package resmgr

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
)

type HandlerTestSuite struct {
	suite.Suite
	handler       *serviceHandler
	context       context.Context
	resTree       respool.Tree
	taskScheduler rm_task.Scheduler
	ctrl          *gomock.Controller
	rmTaskTracker rm_task.Tracker
}

func (suite *HandlerTestSuite) SetupSuite() {
	suite.ctrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.ctrl)
	mockResPoolStore.EXPECT().GetAllResourcePools().
		Return(suite.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(suite.ctrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetAllJobs().Return(nil, nil).Times(4),
	)

	mockTaskStore := store_mocks.NewMockTaskStore(suite.ctrl)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)
	suite.resTree = respool.GetTree()
	// Initializing the resmgr state machine
	rm_task.InitTaskTracker()
	suite.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(1*time.Second, suite.rmTaskTracker)
	suite.taskScheduler = rm_task.GetScheduler()

	suite.handler = &serviceHandler{
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

func (suite *HandlerTestSuite) pendingTasks() []*resmgr.Task {
	return []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: "job1-1"},
		},
		{
			Name:     "job1-1",
			Priority: 1,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: "job1-2"},
		},
		{
			Name:         "job2-1",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-1"},
		},
		{
			Name:         "job2-2",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-2"},
		},
	}
}

func (suite *HandlerTestSuite) expectedTasks() []*resmgr.Task {
	return []*resmgr.Task{
		{
			Name:         "job2-1",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-1"},
		},
		{
			Name:         "job2-2",
			Priority:     2,
			MinInstances: 2,
			JobId:        &peloton.JobID{Value: "job2"},
			Id:           &peloton.TaskID{Value: "job2-2"},
		},
		{
			Name:     "job1-1",
			Priority: 1,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: "job1-2"},
		},
		{
			Name:     "job1-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: "job1-1"},
		},
	}
}

func (suite *HandlerTestSuite) TestEnqueueDequeueTasksOneResPool() {
	log.Info("TestEnqueueDequeueTasksOneResPool called")

	enqReq := &resmgrsvc.EnqueueTasksRequest{
		ResPool: &pb_respool.ResourcePoolID{Value: "respool3"},
		Tasks:   suite.pendingTasks(),
	}
	enqResp, _, err := suite.handler.EnqueueTasks(suite.context, nil, enqReq)
	suite.NoError(err)
	suite.Nil(enqResp.GetError())

	deqReq := &resmgrsvc.DequeueTasksRequest{
		Limit:   10,
		Timeout: 2 * 1000, // 2 sec
	}
	deqResp, _, err := suite.handler.DequeueTasks(suite.context, nil, deqReq)
	suite.NoError(err)
	suite.Nil(deqResp.GetError())
	suite.Equal(suite.expectedTasks(), deqResp.GetTasks())

	log.Info("TestEnqueueDequeueTasksOneResPool returned")
}

func (suite *HandlerTestSuite) TestEnqueueTasksResPoolNotFound() {
	log.Info("TestEnqueueTasksResPoolNotFound called")
	respool.InitTree(tally.NoopScope, nil, nil, nil)

	respoolID := &pb_respool.ResourcePoolID{Value: "respool10"}
	enqReq := &resmgrsvc.EnqueueTasksRequest{
		ResPool: respoolID,
		Tasks:   suite.pendingTasks(),
	}
	enqResp, _, err := suite.handler.EnqueueTasks(suite.context, nil, enqReq)
	suite.NoError(err)
	log.Infof("%v", enqResp)
	notFound := &resmgrsvc.ResourcePoolNotFound{
		Id:      respoolID,
		Message: "Resource pool (respool10) not found",
	}
	suite.Equal(notFound, enqResp.GetError().GetNotFound())
	log.Info("TestEnqueueTasksResPoolNotFound returned")
}

func (suite *HandlerTestSuite) TestEnqueueTasksFailure() {
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
	handler := &serviceHandler{
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
	setResp, _, err := handler.SetPlacements(suite.context, nil, setReq)
	suite.NoError(err)
	suite.Nil(setResp.GetError())

	getReq := &resmgrsvc.GetPlacementsRequest{
		Limit:   10,
		Timeout: 1 * 1000, // 1 sec
	}
	getResp, _, err := handler.GetPlacements(suite.context, nil, getReq)
	suite.NoError(err)
	suite.Nil(getResp.GetError())
	suite.Equal(suite.getPlacements(), getResp.GetPlacements())
}
