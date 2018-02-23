package preemption

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/queue"
	res_common "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type PreemptorTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	preemptor          preemptor
	tracker            rm_task.Tracker
	eventStreamHandler *eventstream.Handler
}

var (
	_taskResources = &task.ResourceConfig{
		CpuLimit:    2,
		DiskLimitMb: 150,
		GpuLimit:    1,
		MemLimitMb:  100,
	}
)

func (suite *PreemptorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())

	rm_task.InitTaskTracker(tally.NoopScope)
	suite.tracker = rm_task.GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.preemptor = preemptor{
		resTree:                      nil,
		runningState:                 res_common.RunningStateNotStarted,
		preemptionPeriod:             1 * time.Second,
		sustainedOverAllocationCount: 5,
		stopChan:                     make(chan struct{}, 1),
		preemptionQueue: queue.NewQueue(
			"preemption-queue",
			reflect.TypeOf(resmgr.Task{}),
			10000,
		),
		respoolState: make(map[string]int),
		ranker:       newStatePriorityRuntimeRanker(rm_task.GetTracker()),
		tracker:      rm_task.GetTracker(),
		scope:        tally.NoopScope,
		m:            make(map[string]*Metrics),
	}
}

func (suite *PreemptorTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *PreemptorTestSuite) TestPreemptor_StartEnabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = true
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.Equal(suite.preemptor.runningState, int32(res_common.RunningStateRunning))
}

func (suite *PreemptorTestSuite) TestPreemptor_StartDisabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = false
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.Equal(suite.preemptor.runningState, int32(res_common.RunningStateNotStarted))
}

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsState() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	tt := []struct {
		entitlement          *scalar.Resources
		allocation           *scalar.Resources
		eligibleRespoolCount int
	}{
		{ // allocation > entitlement
			entitlement: &scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			},
			allocation: &scalar.Resources{
				CPU:    21,
				MEMORY: 220,
				DISK:   2300,
				GPU:    0,
			},
			eligibleRespoolCount: 1,
		},
		{ // Only 1 resource (CPU) is more than entitlement
			entitlement: &scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			},
			allocation: &scalar.Resources{
				CPU:    21,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			},
			eligibleRespoolCount: 1,
		},
		{ // entitlement = allocation
			entitlement: &scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			},
			allocation: &scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			},
			eligibleRespoolCount: 0,
		},
	}

	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	l := list.New()
	l.PushBack(mockResPool)
	mockResTree.EXPECT().GetAllNodes(true).Return(
		l,
	).AnyTimes()
	suite.preemptor.resTree = mockResTree
	suite.preemptor.sustainedOverAllocationCount = 5

	for _, t := range tt {
		mockResPool.EXPECT().GetEntitlement().Return(t.entitlement).
			Times(6)
		mockResPool.EXPECT().GetTotalAllocatedResources().Return(t.allocation).
			Times(6)

		for i := 0; i < 6; i++ {
			suite.preemptor.updateResourcePoolsState()
		}

		respools := suite.preemptor.getEligibleResPools()
		suite.Equal(t.eligibleRespoolCount, len(respools))
		if len(respools) == 1 {
			suite.Equal("respool-1", respools[0])
		}
	}
}

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsState_MarkProcessed() {
	respoolID := "respool-1"
	p.respoolState[respoolID] = 1
	p.markProcessed(respoolID)
	suite.Equal(0, p.respoolState[respoolID])
}

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsState_Reset() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}).AnyTimes()

	// mock allocation going down on compared to the entitlement once
	gomock.InOrder(
		mockResPool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}),
		// allocation goes down
		mockResPool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
			CPU:    10,
			MEMORY: 100,
			DISK:   1000,
			GPU:    0,
		}),
	)

	mockResPools := list.New()
	mockResPools.PushBack(mockResPool)
	mockResTree.EXPECT().GetAllNodes(true).Return(
		mockResPools,
	).AnyTimes()

	suite.preemptor.resTree = mockResTree
	suite.preemptor.sustainedOverAllocationCount = 5

	// run it 5 times
	for i := 0; i < 5; i++ {
		suite.preemptor.updateResourcePoolsState()
	}

	// no respools should be added since allocation becomes less than entitlement once
	respools := suite.preemptor.getEligibleResPools()
	suite.Equal(0, len(respools))
}

func (suite *PreemptorTestSuite) TestPreemptor_ProcessResourcePoolForRunningTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    1,
	}).AnyTimes()
	allocation := &scalar.Resources{
		CPU:    25,
		MEMORY: 500,
		DISK:   2450,
		GPU:    1,
	}
	mockResPool.EXPECT().GetTotalAllocatedResources().Return(allocation).AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1")

	numRunningTasks := 3
	tasks := suite.createTasks(numRunningTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToRunning(t.Id)
	}

	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.LessThanOrEqual(mockResPool.GetEntitlement()))

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)
	// there should be 3 tasks in the preemption queue
	suite.Equal(numRunningTasks, suite.preemptor.preemptionQueue.Length())

	suite.Equal(0, p.respoolState["respool-1"])
}

func (suite *PreemptorTestSuite) TestReconciler_ProcessResourcePoolForReadyTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    1,
	}).AnyTimes()
	allocation := &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.TotalAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			}}}
	mockResPool.EXPECT().GetTotalAllocatedResources().Return(allocation.
		GetByType(scalar.TotalAllocation)).AnyTimes()
	mockResPool.EXPECT().SubtractFromAllocation(gomock.Any()).Do(
		func(res *scalar.Allocation) {
			allocation = allocation.Subtract(res)
		}).Return(nil).AnyTimes()
	mockResPool.EXPECT().EnqueueGang(gomock.Any()).Return(nil).AnyTimes()

	demand := scalar.ZeroResource
	mockResPool.EXPECT().AddToDemand(gomock.Any()).Do(
		func(res *scalar.Resources) {
			demand = demand.Add(res)
		}).Return(nil).AnyTimes()

	numReadyTasks := 3
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToReady(t.Id)
	}
	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.
		GetByType(scalar.TotalAllocation).LessThanOrEqual(mockResPool.GetEntitlement()))
	// Check demand is zero
	suite.Equal(demand, scalar.ZeroResource)

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	// Check allocation <= entitlement after
	suite.True(allocation.
		GetByType(scalar.TotalAllocation).LessThanOrEqual(mockResPool.GetEntitlement()))
	// Check demand includes resources for all READY tasks
	suite.Equal(demand, &scalar.Resources{
		CPU:    _taskResources.CpuLimit * float64(numReadyTasks),
		MEMORY: _taskResources.MemLimitMb * float64(numReadyTasks),
		DISK:   _taskResources.DiskLimitMb * float64(numReadyTasks),
		GPU:    _taskResources.GpuLimit * float64(numReadyTasks),
	}, demand)
}

func (suite *PreemptorTestSuite) TestReconciler_ProcessResourcePoolForPlacingTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    1,
	}).AnyTimes()
	allocation := &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.TotalAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			}}}
	mockResPool.EXPECT().GetTotalAllocatedResources().Return(allocation.
		GetByType(scalar.TotalAllocation)).
		AnyTimes()
	mockResPool.EXPECT().SubtractFromAllocation(gomock.Any()).Do(
		func(res *scalar.Allocation) {
			allocation = allocation.Subtract(res)
		}).Return(nil).AnyTimes()
	mockResPool.EXPECT().EnqueueGang(gomock.Any()).Return(nil).AnyTimes()

	demand := scalar.ZeroResource
	mockResPool.EXPECT().AddToDemand(gomock.Any()).Do(
		func(res *scalar.Resources) {
			demand = demand.Add(res)
		}).Return(nil).AnyTimes()

	numReadyTasks := 3
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToPlacing(t.Id)
	}
	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.GetByType(scalar.TotalAllocation).LessThanOrEqual(
		mockResPool.
			GetEntitlement()))
	// Check demand is zero
	suite.Equal(demand, scalar.ZeroResource)

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	// Check allocation <= entitlement after
	suite.True(allocation.GetByType(scalar.TotalAllocation).
		LessThanOrEqual(mockResPool.GetEntitlement()))
	// Check demand includes resources for all READY tasks
	suite.Equal(demand, &scalar.Resources{
		CPU:    _taskResources.CpuLimit * float64(numReadyTasks),
		MEMORY: _taskResources.MemLimitMb * float64(numReadyTasks),
		DISK:   _taskResources.DiskLimitMb * float64(numReadyTasks),
		GPU:    _taskResources.GpuLimit * float64(numReadyTasks),
	}, demand)
}

func (suite *PreemptorTestSuite) TestPreemptor_Init() {
	suite.initResourceTree()
	InitPreemptor(tally.NoopScope, &Config{
		Enabled:                      true,
		TaskPreemptionPeriod:         100 * time.Hour,
		SustainedOverAllocationCount: 100,
	}, suite.tracker)
	suite.NotNil(GetPreemptor())
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}

func (suite *PreemptorTestSuite) initResourceTree() {
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
}

// Returns resource pools
func (suite *PreemptorTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {
	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:   "root",
			Parent: nil,
			Resources: []*pb_respool.ResourceConfig{
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
			},
			Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		},
	}
}

func (suite *PreemptorTestSuite) createTasks(numTasks int,
	mockResPool *mocks.MockResPool) []*resmgr.Task {
	var tasks []*resmgr.Task
	for i := 0; i < numTasks; i++ {
		t := suite.createTask(i, uint32(i))
		tasks = append(tasks, t)
		suite.tracker.AddTask(t, suite.eventStreamHandler, mockResPool,
			&rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
			})
	}
	return tasks
}

func (suite *PreemptorTestSuite) createTask(instance int, priority uint32) *resmgr.Task {
	taskID := fmt.Sprintf("job1-%d", instance)
	return &resmgr.Task{
		Name:     taskID,
		Priority: priority,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: taskID},
		Hostname: "hostname",
		Resource: _taskResources,
	}
}

type mockRanker struct {
	tasks []*rm_task.RMTask
}

func newMockRanker(tasks []*rm_task.RMTask) ranker {
	return &mockRanker{
		tasks: tasks,
	}
}

func (mr *mockRanker) GetTasksToEvict(respoolID string,
	resourcesLimit *scalar.Resources) []*rm_task.RMTask {
	return mr.tasks
}

// Returns a mock ranker with the tasks to evict
func (suite *PreemptorTestSuite) getMockRanker(tasks []*resmgr.Task) ranker {
	var tasksToEvict []*rm_task.RMTask
	for _, t := range tasks {
		tasksToEvict = append(tasksToEvict, suite.tracker.GetTask(t.Id))
	}
	return newMockRanker(tasksToEvict)
}

func (suite *PreemptorTestSuite) transitToPlacing(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	err := rmTask.TransitTo(task.TaskState_PENDING.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACING.String(), "")
	suite.NoError(err)
}

func (suite *PreemptorTestSuite) transitToReady(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	err := rmTask.TransitTo(task.TaskState_PENDING.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String(), "")
	suite.NoError(err)
}

func (suite *PreemptorTestSuite) transitToRunning(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	err := rmTask.TransitTo(task.TaskState_PENDING.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String(), "")
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_RUNNING.String(), "")
	suite.NoError(err)
}
