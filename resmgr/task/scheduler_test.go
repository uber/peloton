package task

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"peloton/api/job"
	pb_respool "peloton/api/respool"
	"peloton/api/task"
	"peloton/private/resmgr"
)

type TaskSchedulerTestSuite struct {
	suite.Suite
	resTree    respool.Tree
	resPools   map[string]*pb_respool.ResourcePoolConfig
	allNodes   map[string]*respool.ResPool
	root       *respool.ResPool
	readyQueue queue.Queue
	taskSched  *scheduler
}

func (suite *TaskSchedulerTestSuite) SetupTest() {
	respool.InitTree(tally.NoopScope, nil)
	suite.resTree = respool.GetTree()
	suite.setupResPools()
	suite.root = suite.resTree.CreateTree(nil, respool.RootResPoolID, suite.resPools, suite.allNodes)
	suite.resTree.SetAllNodes(&suite.allNodes)
	suite.AddTasks()
	suite.readyQueue = queue.NewQueue(
		"ready-queue",
		reflect.TypeOf(resmgr.Task{}),
		maxReadyQueueSize,
	)
	suite.taskSched = &scheduler{
		resPoolTree:      suite.resTree,
		runningState:     runningStateNotStarted,
		schedulingPeriod: time.Duration(1) * time.Second,
		stopChan:         make(chan struct{}, 1),
		readyQueue:       suite.readyQueue,
	}
}

func (suite *TaskSchedulerTestSuite) TearDownTest() {
	fmt.Println("tearing down")
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

func (suite *TaskSchedulerTestSuite) setupResPools() {
	rootID := pb_respool.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	suite.allNodes = make(map[string]*respool.ResPool)
	suite.resPools = map[string]*pb_respool.ResourcePoolConfig{
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
			JobId:    &job.JobID{Value: "job1"},
			Id:       &task.TaskID{Value: "job1-1"},
		},
		{
			Name:     "job1-1",
			Priority: 1,
			JobId:    &job.JobID{Value: "job1"},
			Id:       &task.TaskID{Value: "job1-2"},
		},
		{
			Name:     "job2-1",
			Priority: 2,
			JobId:    &job.JobID{Value: "job2"},
			Id:       &task.TaskID{Value: "job2-1"},
		},
		{
			Name:     "job2-2",
			Priority: 2,
			JobId:    &job.JobID{Value: "job2"},
			Id:       &task.TaskID{Value: "job2-2"},
		},
	}

	for _, task := range tasks {
		suite.allNodes["respool11"].EnqueueTask(task)
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

func (suite *TaskSchedulerTestSuite) TestMovingtoReadyQueue() {
	// TODO: Test start and stop differently
	suite.taskSched.Start()
	time.Sleep(2000 * time.Millisecond)
	suite.validateReadyQueue()
}

func (suite *TaskSchedulerTestSuite) TestMovingTasks() {
	suite.taskSched.scheduleTasks()
	suite.validateReadyQueue()
}
