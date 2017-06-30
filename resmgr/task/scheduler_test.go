package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type TaskSchedulerTestSuite struct {
	suite.Suite
	resTree       respool.Tree
	readyQueue    *queue.MultiLevelList
	taskSched     *scheduler
	mockCtrl      *gomock.Controller
	rmTaskTracker Tracker
}

func (suite *TaskSchedulerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools(context.Background()).Return(suite.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetJobsByStates(context.Background(),
			gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)

	suite.resTree = respool.GetTree()
	suite.readyQueue = queue.NewMultiLevelList("ready-queue", maxReadyQueueSize)
	// Initializing the resmgr state machine
	InitTaskTracker(tally.NoopScope)
	suite.rmTaskTracker = GetTracker()

	suite.taskSched = &scheduler{
		resPoolTree:      suite.resTree,
		runningState:     runningStateNotStarted,
		schedulingPeriod: time.Duration(1) * time.Second,
		stopChan:         make(chan struct{}, 1),
		readyQueue:       suite.readyQueue,
		rmTaskTracker:    suite.rmTaskTracker,
		metrics:          NewMetrics(tally.NoopScope),
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
			Name:     "job2-1",
			Priority: 2,
			JobId:    &peloton.JobID{Value: "job2"},
			Id:       &peloton.TaskID{Value: "job2-1"},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
		},
		{
			Name:     "job2-2",
			Priority: 2,
			JobId:    &peloton.JobID{Value: "job2"},
			Id:       &peloton.TaskID{Value: "job2-2"},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
		},
	}
	resPool, err := suite.resTree.Get(&pb_respool.ResourcePoolID{
		Value: "respool11",
	})
	resPool.SetEntitlement(suite.getEntitlement())

	for _, task := range tasks {
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
		item, err := suite.readyQueue.Pop(suite.readyQueue.Levels()[0])
		suite.NoError(err)
		gang := item.(*resmgrsvc.Gang)
		task := gang.Tasks[0]
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

func (suite *TaskSchedulerTestSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}
