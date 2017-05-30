package task

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type TaskSchedulerTestSuite struct {
	suite.Suite
	resTree       respool.Tree
	readyQueue    queue.Queue
	taskSched     *scheduler
	mockCtrl      *gomock.Controller
	rmTaskTracker Tracker
}

func (suite *TaskSchedulerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools().Return(suite.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetAllJobs().Return(nil, nil).AnyTimes(),
	)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)

	suite.resTree = respool.GetTree()
	suite.readyQueue = queue.NewQueue(
		"ready-queue",
		reflect.TypeOf(resmgr.Task{}),
		maxReadyQueueSize,
	)
	// Initializing the resmgr state machine
	InitTaskTracker()
	suite.rmTaskTracker = GetTracker()

	suite.taskSched = &scheduler{
		resPoolTree:      suite.resTree,
		runningState:     runningStateNotStarted,
		schedulingPeriod: time.Duration(1) * time.Second,
		stopChan:         make(chan struct{}, 1),
		readyQueue:       suite.readyQueue,
		rmTaskTracker:    suite.rmTaskTracker,
	}
}

func (suite *TaskSchedulerTestSuite) SetupTest() {
	fmt.Println("setting up")
	suite.resTree.Start()
	suite.taskSched.Start()
	suite.AddTasks()
}

func (suite *TaskSchedulerTestSuite) TearDownTest() {
	fmt.Println("tearing down")
	err := suite.resTree.Stop()
	suite.NoError(err)
	err = suite.taskSched.Stop()
	suite.NoError(err)
	suite.mockCtrl.Finish()
}

func TestTaskScheduler(t *testing.T) {
	suite.Run(t, new(TaskSchedulerTestSuite))
}

func (suite *TaskSchedulerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

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

// Returns resource pools
func (suite *TaskSchedulerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

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

func (suite *TaskSchedulerTestSuite) AddTasks() {

	tasks := []*resmgr.Task{
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
			Name:     "job2-1",
			Priority: 2,
			JobId:    &peloton.JobID{Value: "job2"},
			Id:       &peloton.TaskID{Value: "job2-1"},
		},
		{
			Name:     "job2-2",
			Priority: 2,
			JobId:    &peloton.JobID{Value: "job2"},
			Id:       &peloton.TaskID{Value: "job2-2"},
		},
	}

	for _, task := range tasks {
		resPool, err := suite.resTree.Get(&pb_respool.ResourcePoolID{
			Value: "respool11",
		})
		suite.NoError(err)
		resPool.EnqueueGang(resPool.MakeTaskGang(task))
	}
}

func (suite *TaskSchedulerTestSuite) validateReadyQueue() {
	expectedTaskIDs := []string{
		"job2-1",
		"job2-2",
		"job1-2",
		"job1-1",
	}
	for i := 0; i < 4; i++ {
		item, err := suite.readyQueue.Dequeue(1 * time.Millisecond)
		suite.NoError(err)
		task := item.(*resmgr.Task)
		suite.Equal(expectedTaskIDs[i], task.Id.Value)
	}
}

func (suite *TaskSchedulerTestSuite) TestMovingToReadyQueue() {
	time.Sleep(2000 * time.Millisecond)
	suite.validateReadyQueue()
}

func (suite *TaskSchedulerTestSuite) TestMovingTasks() {
	suite.taskSched.scheduleTasks()
	suite.validateReadyQueue()
}
