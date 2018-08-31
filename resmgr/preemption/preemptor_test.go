package preemption

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/common/queue"
	qmock "code.uber.internal/infra/peloton/common/queue/mocks"
	"code.uber.internal/infra/peloton/common/stringset"
	res_common "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/resmgr/tasktestutil"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type PreemptorTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	preemptor          Preemptor
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

	rm_task.InitTaskTracker(
		tally.NoopScope,
		tasktestutil.CreateTaskConfig())
	suite.tracker = rm_task.GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.preemptor = Preemptor{
		resTree:                      nil,
		preemptionPeriod:             1 * time.Second,
		sustainedOverAllocationCount: 5,
		preemptionQueue: queue.NewQueue(
			"preemption-queue",
			reflect.TypeOf(resmgr.PreemptionCandidate{}),
			10000,
		),
		taskSet:      stringset.New(),
		respoolState: make(map[string]int),
		ranker:       newStatePriorityRuntimeRanker(rm_task.GetTracker()),
		tracker:      rm_task.GetTracker(),
		scope:        tally.NoopScope,
		m:            make(map[string]*Metrics),
		lifeCycle:    lifecycle.NewLifeCycle(),
	}
}

func (suite *PreemptorTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *PreemptorTestSuite) TestPreemptorStartEnabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = true
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.NotNil(suite.preemptor.lifeCycle.StopCh())
}

func (suite *PreemptorTestSuite) TestPreemptorStartDisabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = false
	err := suite.preemptor.Start()
	suite.NoError(err)
	_, ok := <-suite.preemptor.lifeCycle.StopCh()
	suite.False(ok)
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
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()

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

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsStateReset() {
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
		mockResPool.EXPECT().GetTotalAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetTotalAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}),
		// allocation goes down
		mockResPool.EXPECT().GetTotalAllocatedResources().Return(
			&scalar.Resources{
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

func (suite *PreemptorTestSuite) TestProcessResourcePoolForRunningTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()
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
	mockResPool.EXPECT().GetTotalAllocatedResources().Return(allocation).
		AnyTimes()
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

	suite.Equal(0, suite.preemptor.respoolState["respool-1"])
}

func (suite *PreemptorTestSuite) TestProcessResourcePoolForReadyTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()
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

func (suite *PreemptorTestSuite) TestProcessResourcePoolForPlacingTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
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

func (suite *PreemptorTestSuite) TestProcessResourceGetError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	fakeGetError := fmt.Errorf("fake Get error")

	mockResTree := mocks.NewMockTree(ctr)
	mockResTree.
		EXPECT().
		Get(gomock.Any()).
		Return(nil, fakeGetError)
	suite.preemptor.resTree = mockResTree

	// Test Respool Get error
	err := suite.preemptor.processResourcePool("respool-1")
	suite.NotNil(err)
	suite.True(strings.Contains(err.Error(), fakeGetError.Error()))
}

var (
	_entitlement = &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    1,
	}

	_allocation = &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.TotalAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			},
		},
	}
)

func (suite *PreemptorTestSuite) TestProcessRunningTaskEnqueueError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	fakeEnqueueError := fmt.Errorf("fake EnqueueGang error")

	mockPQueue := qmock.NewMockQueue(ctr)
	mockPQueue.
		EXPECT().
		Enqueue(gomock.Any()).
		Return(fakeEnqueueError)
	suite.preemptor.preemptionQueue = mockPQueue

	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		GetPath().
		Return("/path").
		AnyTimes()
	// Add 1 task in the tracker with id job1-0
	tasks := suite.createTasks(1, mockResPool)
	for _, t := range tasks {
		suite.transitToRunning(t.Id)
	}

	// Get the added RMTask
	t := suite.tracker.GetTask(&peloton.TaskID{Value: "job1-0"})
	suite.preemptor.taskSet.Clear()

	err := suite.preemptor.processRunningTask(
		t,
		resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE,
	)
	suite.NotNil(err)
	suite.True(strings.Contains(err.Error(), fakeEnqueueError.Error()))
	suite.Equal(0, len(suite.preemptor.taskSet.ToSlice()))
}

func (suite *PreemptorTestSuite) TestProcessResourcePoolEnqueueGangError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	// Test EnqueueGang error
	fakeEnqueueError := fmt.Errorf("fake EnqueueGang error")

	//Mock resource pool
	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		ID().
		Return("respool-1")
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetEntitlement().
		Return(_entitlement)
	mockResPool.
		EXPECT().
		GetTotalAllocatedResources().
		Return(_allocation.GetByType(scalar.TotalAllocation))
	mockResPool.
		EXPECT().
		EnqueueGang(gomock.Any()).
		Return(fakeEnqueueError)

		// Mock resource pool tree
	mockResTree := mocks.NewMockTree(ctr)
	mockResTree.
		EXPECT().
		Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	suite.preemptor.resTree = mockResTree

	// Add tasks in the tracker in READY state
	numReadyTasks := 1
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	suite.preemptor.ranker = suite.getMockRanker(tasks)
	for _, t := range tasks {
		suite.transitToReady(t.Id)
	}
	defer suite.tracker.Clear()

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NotNil(err)
	suite.True(strings.Contains(err.Error(), fakeEnqueueError.Error()))
}

func (suite *PreemptorTestSuite) TestProcessResourcePoolAddDemandError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	// Test AddToDemand error
	fakeDemandError := fmt.Errorf("fake AddToDemand error")

	//Mock resource pool
	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		ID().
		Return("respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetEntitlement().
		Return(_entitlement)
	mockResPool.
		EXPECT().
		GetTotalAllocatedResources().
		Return(_allocation.GetByType(scalar.TotalAllocation))
	mockResPool.
		EXPECT().
		EnqueueGang(gomock.Any()).
		Return(nil)
	mockResPool.
		EXPECT().
		AddToDemand(gomock.Any()).
		Return(fakeDemandError)

		// Mock resource pool tree
	mockResTree := mocks.NewMockTree(ctr)
	mockResTree.
		EXPECT().
		Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	suite.preemptor.resTree = mockResTree

	// Add tasks in the tracker in READY state
	numReadyTasks := 1
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	suite.preemptor.ranker = suite.getMockRanker(tasks)
	for _, t := range tasks {
		suite.transitToReady(t.Id)
	}
	defer suite.tracker.Clear()

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NotNil(err)
	suite.True(strings.Contains(err.Error(), fakeDemandError.Error()))
}

func (suite *PreemptorTestSuite) TestProcessResourcePoolSubtractAllocationError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	fakeAllocationError := fmt.Errorf("fake AddToDemand error")

	//Mock resource pool
	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		ID().
		Return("respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetEntitlement().
		Return(_entitlement).AnyTimes()
	mockResPool.
		EXPECT().
		GetTotalAllocatedResources().
		Return(
			_allocation.GetByType(scalar.TotalAllocation)).
		AnyTimes()
	mockResPool.
		EXPECT().
		SubtractFromAllocation(gomock.Any()).
		Return(fakeAllocationError)
	mockResPool.
		EXPECT().
		AddToDemand(gomock.Any()).
		Return(nil)
	mockResPool.
		EXPECT().
		EnqueueGang(gomock.Any()).
		Return(nil)

	// Mock resource pool tree
	mockResTree := mocks.NewMockTree(ctr)
	mockResTree.
		EXPECT().
		Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	suite.preemptor.resTree = mockResTree

	// Add tasks in the tracker in READY state
	numReadyTasks := 1
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	suite.preemptor.ranker = suite.getMockRanker(tasks)
	for _, t := range tasks {
		suite.transitToReady(t.Id)
	}
	defer suite.tracker.Clear()

	// Test Allocation error
	err := suite.preemptor.processResourcePool("respool-1")
	suite.True(strings.Contains(err.Error(), fakeAllocationError.Error()))
}

func (suite *PreemptorTestSuite) TestPreemptorEnqueue() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	// Add 1 task in the tracker with id job1-0
	tasks := suite.createTasks(1, mockResPool)
	for _, t := range tasks {
		suite.transitToRunning(t.Id)
	}

	// clear the task set before the test
	suite.preemptor.taskSet.Clear()

	// get the task from the tracker
	t := suite.tracker.GetTask(&peloton.TaskID{Value: "job1-0"})

	err := suite.preemptor.EnqueueTasks(
		[]*rm_task.RMTask{t},
		resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE,
	)
	suite.NoError(err)
	suite.Equal(1, len(suite.preemptor.taskSet.ToSlice()))
}

func (suite *PreemptorTestSuite) TestNewPreemptor() {
	suite.initResourceTree()
	p := NewPreemptor(tally.NoopScope, &res_common.PreemptionConfig{
		Enabled:                      true,
		TaskPreemptionPeriod:         100 * time.Hour,
		SustainedOverAllocationCount: 100,
	}, suite.tracker)
	suite.NotNil(p)
}

func (suite *PreemptorTestSuite) TestPreemptionQueueDuplicateTasks() {
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

	numRunningTasks := 1
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

	//there should be 'numRunningTasks' in the preemption queue
	suite.Equal(numRunningTasks, suite.preemptor.preemptionQueue.Length())

	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().GetPath().Return("/respool-1")
	err = suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	//there should still be 'numRunningTasks' in the preemption queue
	suite.Equal(numRunningTasks, suite.preemptor.preemptionQueue.Length())
}

func (suite *PreemptorTestSuite) TestPreemptorDequeueTask() {
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

	numRunningTasks := 1
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

	//there should be 'numRunningTasks' in the preemption queue
	suite.Equal(numRunningTasks, suite.preemptor.preemptionQueue.Length())

	_, err = suite.preemptor.DequeueTask(1 * time.Second)
	suite.NoError(err)

	// Time-out error
	_, err = suite.preemptor.DequeueTask(1 * time.Second)
	suite.Error(err)
}

func (suite *PreemptorTestSuite) TestPreemptorDequeueTaskError() {
	fakeErr := errors.New("fake error")

	tt := []struct {
		wantErr error
	}{
		{
			wantErr: queue.DequeueTimeOutError{},
		},
		{
			wantErr: fakeErr,
		},
	}

	for _, t := range tt {
		mockQ := qmock.NewMockQueue(suite.mockCtrl)
		mockQ.EXPECT().Dequeue(gomock.Any()).Return(nil,
			t.wantErr)

		p := &Preemptor{
			preemptionQueue: mockQ,
		}

		i, err := p.DequeueTask(1 * time.Second)
		suite.EqualError(t.wantErr, err.Error(), "unexpected error")
		suite.Nil(i)
	}
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}

// Have to do this because the preemptor depends on the resTree
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
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{
			Enabled: true,
		})
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
			tasktestutil.CreateTaskConfig())
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
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
			task.TaskState_PLACING,
		})
}

func (suite *PreemptorTestSuite) transitToReady(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
		})
}

func (suite *PreemptorTestSuite) transitToRunning(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_PLACED,
			task.TaskState_LAUNCHING,
			task.TaskState_RUNNING,
		})
}
