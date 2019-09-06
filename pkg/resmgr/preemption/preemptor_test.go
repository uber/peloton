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

package preemption

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/queue"
	qmock "github.com/uber/peloton/pkg/common/queue/mocks"
	"github.com/uber/peloton/pkg/common/stringset"
	res_common "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/respool/mocks"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"
	"github.com/uber/peloton/pkg/resmgr/tasktestutil"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type preemptorTestSuite struct {
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

func (suite *preemptorTestSuite) SetupTest() {
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

func (suite *preemptorTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *preemptorTestSuite) TestPreemptorStartEnabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = true
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.NotNil(suite.preemptor.lifeCycle.StopCh())
}

func (suite *preemptorTestSuite) TestPreemptorStartDisabled() {
	defer suite.preemptor.Stop()
	suite.preemptor.enabled = false
	err := suite.preemptor.Start()
	suite.NoError(err)
	_, ok := <-suite.preemptor.lifeCycle.StopCh()
	suite.False(ok)
}

func (suite *preemptorTestSuite) TestUpdateResourcePoolsState() {
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
		mockResPool.EXPECT().
			GetNonSlackEntitlement().
			Return(t.entitlement).
			Times(6)
		mockResPool.EXPECT().
			GetNonSlackAllocatedResources().
			Return(t.allocation).
			Times(6)
		mockResPool.EXPECT().GetSlackEntitlement().
			Return(scalar.ZeroResource).
			AnyTimes()
		mockResPool.EXPECT().GetSlackAllocatedResources().
			Return(scalar.ZeroResource).
			AnyTimes()

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

func (suite *preemptorTestSuite) TestUpdateResourcePoolsStateReset() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()
	mockResPool.EXPECT().GetNonSlackEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}).AnyTimes()
	mockResPool.EXPECT().GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()

	// mock allocation going down on compared to the entitlement once
	gomock.InOrder(
		mockResPool.EXPECT().GetNonSlackAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetNonSlackAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetNonSlackAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}), mockResPool.EXPECT().GetNonSlackAllocatedResources().Return(
			&scalar.Resources{
				CPU:    20,
				MEMORY: 200,
				DISK:   2000,
				GPU:    0,
			}),
		// allocation goes down
		mockResPool.EXPECT().GetNonSlackAllocatedResources().Return(
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

func (suite *preemptorTestSuite) TestProcessResourcePoolForRunningTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
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
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
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
	suite.False(allocation.LessThanOrEqual(
		mockResPool.GetNonSlackEntitlement()))

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)
	// there should be 3 tasks in the preemption queue
	suite.Equal(numRunningTasks, suite.preemptor.preemptionQueue.Length())

	suite.Equal(0, suite.preemptor.respoolState["respool-1"])
}

func (suite *preemptorTestSuite) TestProcessResourcePoolForReadyTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetPath().Return("/respool-1").AnyTimes()
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    1,
		}).AnyTimes()
	allocation := &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.NonSlackAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			}}}
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation.GetByType(scalar.NonSlackAllocation)).
		AnyTimes()
	mockResPool.EXPECT().SubtractFromAllocation(gomock.Any()).Do(
		func(res *scalar.Allocation) {
			allocation = allocation.Subtract(res)
		}).Return(nil).AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().EnqueueGang(gomock.Any()).Return(nil).AnyTimes()

	numReadyTasks := 3
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToReady(t.Id)
	}
	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.GetByType(scalar.NonSlackAllocation).
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	// Check allocation <= entitlement after
	suite.True(allocation.GetByType(scalar.NonSlackAllocation).
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))
}

func (suite *preemptorTestSuite) TestProcessResourcePoolForPlacingTasks() {
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
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    1,
		}).AnyTimes()
	allocation := &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.NonSlackAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			}}}
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation.GetByType(scalar.NonSlackAllocation)).
		AnyTimes()
	mockResPool.EXPECT().SubtractFromAllocation(gomock.Any()).
		Do(func(res *scalar.Allocation) {
			allocation = allocation.Subtract(res)
		}).Return(nil).AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().EnqueueGang(gomock.Any()).Return(nil).AnyTimes()

	numReadyTasks := 3
	tasks := suite.createTasks(numReadyTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToPlacing(t.Id)
	}
	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.GetByType(scalar.NonSlackAllocation).
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))

	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	// Check allocation <= entitlement after
	suite.True(allocation.GetByType(scalar.NonSlackAllocation).
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))
}

func (suite *preemptorTestSuite) TestRevocableTaskPreemptionOnly() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()
	mockResTree := mocks.NewMockTree(ctr)
	mockResPool := mocks.NewMockResPool(ctr)

	mockResTree.EXPECT().
		Get(&peloton.ResourcePoolID{Value: "respool-1"}).
		Return(mockResPool, nil)
	mockResPool.EXPECT().
		ID().
		Return("respool-1").
		AnyTimes()
	mockResPool.EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 500,
			DISK:   2500,
			GPU:    0,
		}).AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(&scalar.Resources{
			CPU:    10,
			MEMORY: 0,
			DISK:   1000,
			GPU:    0,
		}).
		AnyTimes()
	allocation := &scalar.Allocation{
		Value: map[scalar.AllocationType]*scalar.Resources{
			scalar.NonSlackAllocation: {
				CPU:    20,
				MEMORY: 500,
				DISK:   2450,
				GPU:    0,
			},
			scalar.SlackAllocation: {
				CPU:    20,
				MEMORY: 600,
				DISK:   1000,
				GPU:    0,
			},
		}}
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation.GetByType(scalar.NonSlackAllocation)).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(allocation.GetByType(scalar.SlackAllocation)).
		AnyTimes()

	mockResPool.EXPECT().SubtractFromAllocation(gomock.Any()).Do(
		func(res *scalar.Allocation) {
			allocation = allocation.Subtract(res)
		}).Return(nil).AnyTimes()

	mockResPool.EXPECT().EnqueueGang(gomock.Any()).Return(nil).AnyTimes()

	// Add non-revocable tasks in all states
	tasks := suite.createTasks(6, mockResPool)
	for i, t := range tasks {
		if i == 0 || i == 1 {
			suite.transitToReady(t.Id)
		} else if i == 2 || i == 3 {
			suite.transitToPlacing(t.Id)
		} else {
			suite.transitToRunning(t.Id)
		}
	}

	// Add revocable tasks in all states
	tasks = suite.createRevocableTasks(6, mockResPool)
	for i, t := range tasks {
		if i == 0 || i == 1 {
			suite.transitToReady(t.Id)
		} else if i == 2 || i == 3 {
			suite.transitToPlacing(t.Id)
		} else {
			suite.transitToRunning(t.Id)
		}
	}

	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Non-Revocable Alloction < Non-Revocable Entitlement
	// Revocable Allocation > Non-Revocable Entitlement
	// Only preempt revocable tasks
	err := suite.preemptor.processResourcePool("respool-1")
	suite.NoError(err)

	// Two running tasks added preemption queue
	suite.Equal(suite.preemptor.preemptionQueue.Length(), 2)
	// Each Task Requirement, CPU: 2, Mem: 100, Disk: 150
	// 4 tasks removed in ready or placing state
	// slack allocation goes down
	suite.Equal(allocation.GetByType(scalar.SlackAllocation).GetCPU(), float64(12))
	suite.Equal(allocation.GetByType(scalar.SlackAllocation).GetMem(), float64(200))
	suite.Equal(allocation.GetByType(scalar.SlackAllocation).GetDisk(), float64(400))

	// non-slack allocation remain as-is
	suite.Equal(allocation.GetByType(scalar.NonSlackAllocation), mockResPool.GetNonSlackAllocatedResources())
}

func (suite *preemptorTestSuite) TestProcessResourceGetError() {
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
			scalar.NonSlackAllocation: {
				CPU:    25,
				MEMORY: 500,
				DISK:   2450,
				GPU:    1,
			},
		},
	}
)

func (suite *preemptorTestSuite) TestProcessRunningTaskEnqueueError() {
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

func (suite *preemptorTestSuite) TestProcessResourcePoolEnqueueGangError() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	// Test EnqueueGang error
	fakeEnqueueError := fmt.Errorf("fake EnqueueGang error")

	//Mock resource pool
	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.
		EXPECT().
		ID().
		Return("respool-1").AnyTimes()
	mockResPool.
		EXPECT().
		GetPath().
		Return("/respool-1").
		AnyTimes()
	mockResPool.
		EXPECT().
		GetNonSlackEntitlement().
		Return(_entitlement).
		AnyTimes()
	mockResPool.
		EXPECT().
		GetNonSlackAllocatedResources().
		Return(_allocation.GetByType(scalar.NonSlackAllocation)).
		AnyTimes()
	mockResPool.
		EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.
		EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
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

func (suite *preemptorTestSuite) TestProcessResourcePoolSubtractAllocationError() {
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
		GetNonSlackEntitlement().
		Return(_entitlement).AnyTimes()
	mockResPool.
		EXPECT().
		GetNonSlackAllocatedResources().
		Return(_allocation.GetByType(scalar.NonSlackAllocation)).AnyTimes()
	mockResPool.
		EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.
		EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.
		EXPECT().
		SubtractFromAllocation(gomock.Any()).
		Return(fakeAllocationError)
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

func (suite *preemptorTestSuite) TestPreemptorEnqueue() {
	ctr := gomock.NewController(suite.T())
	defer ctr.Finish()

	mockResPool := mocks.NewMockResPool(ctr)
	mockResPool.EXPECT().ID().
		Return("respool-1").
		AnyTimes()
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

func (suite *preemptorTestSuite) TestNewPreemptor() {
	p := NewPreemptor(tally.NoopScope, &res_common.PreemptionConfig{
		Enabled:                      true,
		TaskPreemptionPeriod:         100 * time.Hour,
		SustainedOverAllocationCount: 100,
	},
		suite.tracker,
		suite.getResourceTree(),
	)
	suite.NotNil(p)
}

func (suite *preemptorTestSuite) TestPreemptionQueueDuplicateTasks() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
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
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().GetPath().
		Return("/respool-1").
		AnyTimes()

	numRunningTasks := 1
	tasks := suite.createTasks(numRunningTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToRunning(t.Id)
	}

	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))

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

func (suite *preemptorTestSuite) TestPreemptorDequeueTask() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResTree.EXPECT().Get(&peloton.ResourcePoolID{Value: "respool-1"}).Return(mockResPool, nil)
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().
		GetNonSlackEntitlement().
		Return(&scalar.Resources{
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
	mockResPool.EXPECT().
		GetNonSlackAllocatedResources().
		Return(allocation).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackEntitlement().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().
		GetSlackAllocatedResources().
		Return(scalar.ZeroResource).
		AnyTimes()
	mockResPool.EXPECT().GetPath().
		Return("/respool-1").
		AnyTimes()

	numRunningTasks := 1
	tasks := suite.createTasks(numRunningTasks, mockResPool)
	for _, t := range tasks {
		suite.transitToRunning(t.Id)
	}

	suite.preemptor.resTree = mockResTree
	suite.preemptor.ranker = suite.getMockRanker(tasks)

	// Check allocation > entitlement before
	suite.False(allocation.
		LessThanOrEqual(mockResPool.GetNonSlackEntitlement()))

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

func (suite *preemptorTestSuite) TestPreemptorDequeueTaskError() {
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
	suite.Run(t, new(preemptorTestSuite))
}

// Have to do this because the preemptor depends on the resTree
func (suite *preemptorTestSuite) getResourceTree() respool.Tree {
	mockResPoolOps := objectmocks.NewMockResPoolOps(suite.mockCtrl)
	gomock.InOrder(
		mockResPoolOps.EXPECT().
			GetAll(context.Background()).Return(suite.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	return respool.NewTree(tally.NoopScope, mockResPoolOps, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{
			Enabled: true,
		})
}

// Returns resource pools
func (suite *preemptorTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {
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

func (suite *preemptorTestSuite) createTasks(
	numTasks int,
	mockResPool *mocks.MockResPool) []*resmgr.Task {
	var tasks []*resmgr.Task
	for i := 0; i < numTasks; i++ {
		t := suite.createTask(i, uint32(i))
		tasks = append(tasks, t)
		suite.tracker.AddTask(
			t,
			suite.eventStreamHandler,
			mockResPool,
			tasktestutil.CreateTaskConfig())
	}
	return tasks
}

func (suite *preemptorTestSuite) createTask(
	instance int,
	priority uint32) *resmgr.Task {
	taskID := fmt.Sprintf("job1-%d", instance)
	mesosTaskID := fmt.Sprintf("%s-1", taskID)
	return &resmgr.Task{
		Name:        taskID,
		Priority:    priority,
		JobId:       &peloton.JobID{Value: "job1"},
		Id:          &peloton.TaskID{Value: taskID},
		TaskId:      &mesos_v1.TaskID{Value: &mesosTaskID},
		Hostname:    "hostname",
		Resource:    _taskResources,
		Preemptible: true,
	}
}

func (suite *preemptorTestSuite) createRevocableTasks(
	numTasks int,
	mockResPool *mocks.MockResPool) []*resmgr.Task {
	var tasks []*resmgr.Task
	for i := 0; i < numTasks; i++ {
		t := suite.createRevocableTask(i, uint32(i))
		tasks = append(tasks, t)
		suite.tracker.AddTask(
			t,
			suite.eventStreamHandler,
			mockResPool,
			tasktestutil.CreateTaskConfig())
	}
	return tasks
}

func (suite *preemptorTestSuite) createRevocableTask(
	instance int,
	priority uint32) *resmgr.Task {
	taskID := fmt.Sprintf("job1-%d", instance)
	mesosTaskID := fmt.Sprintf("%s-1", taskID)
	return &resmgr.Task{
		Name:        taskID,
		Priority:    priority,
		JobId:       &peloton.JobID{Value: "job1"},
		Id:          &peloton.TaskID{Value: taskID},
		TaskId:      &mesos_v1.TaskID{Value: &mesosTaskID},
		Hostname:    "hostname",
		Resource:    _taskResources,
		Revocable:   true,
		Preemptible: true,
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

func (mr *mockRanker) GetTasksToEvict(
	respoolID string,
	slackResourcesToFree, nonSlackResourcesToFree *scalar.Resources) []*rm_task.RMTask {
	return mr.tasks
}

// Returns a mock ranker with the tasks to evict
func (suite *preemptorTestSuite) getMockRanker(tasks []*resmgr.Task) ranker {
	var tasksToEvict []*rm_task.RMTask
	for _, t := range tasks {
		tasksToEvict = append(tasksToEvict, suite.tracker.GetTask(t.Id))
	}
	return newMockRanker(tasksToEvict)
}

func (suite *preemptorTestSuite) transitToPlacing(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
			task.TaskState_PLACING,
		})
}

func (suite *preemptorTestSuite) transitToReady(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
		})
}

func (suite *preemptorTestSuite) transitToRunning(taskID *peloton.TaskID) {
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
