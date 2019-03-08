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

package respool

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/queue"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

var (
	_testResPoolName = "respool_1"
	_rootResPoolID   = peloton.ResourcePoolID{Value: common.RootResPoolID}
)

type ResPoolSuite struct {
	root ResPool
	suite.Suite
	cfg rc.PreemptionConfig
}

func (s *ResPoolSuite) SetupSuite() {
	// root resource pool
	rootConfig := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	s.cfg = rc.PreemptionConfig{Enabled: false}
	rootResPool, err := NewRespool(tally.NoopScope, common.RootResPoolID, nil,
		rootConfig, s.cfg)
	s.NoError(err)
	s.True(rootResPool.IsRoot())
	s.root = rootResPool
}

// makeTaskGang forms a gang from a single task
func makeTaskGang(task *resmgr.Task) *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = append(gang.Tasks, task)
	return &gang
}

func (s *ResPoolSuite) getResources() []*pb_respool.ResourceConfig {
	return []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
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
}

func (s *ResPoolSuite) getTasks() []*resmgr.Task {
	return []*resmgr.Task{
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
			Preemptible: true,
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
			Preemptible: true,
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
			Preemptible: true,
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
			Preemptible: true,
		},
	}
}

func (s *ResPoolSuite) getRevocableTask() *resmgr.Task {
	return &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: "job1-1"},
		Resource: &task.ResourceConfig{
			CpuLimit:    10,
			DiskLimitMb: 2,
			GpuLimit:    0,
			MemLimitMb:  10,
		},
		Revocable: true,
	}
}

func (s *ResPoolSuite) getEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 1000,
		DISK:   100,
		GPU:    2,
	}
}

func (s *ResPoolSuite) getNonSlackEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 800,
		DISK:   80,
		GPU:    2,
	}
}

func (s *ResPoolSuite) getSlackEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    80,
		MEMORY: 200,
		DISK:   20,
		GPU:    0,
	}
}

func (s *ResPoolSuite) getDemand() *scalar.Resources {
	return &scalar.Resources{
		CPU:    float64(100),
		GPU:    float64(1),
		MEMORY: float64(100),
		DISK:   float64(1000),
	}
}

func (s *ResPoolSuite) getSlackDemand() *scalar.Resources {
	return &scalar.Resources{
		CPU:    float64(10),
		GPU:    float64(0),
		MEMORY: float64(10),
		DISK:   float64(10),
	}
}

func (s *ResPoolSuite) getAllocation() *scalar.Resources {
	return &scalar.Resources{
		CPU:    float64(100),
		GPU:    float64(1),
		MEMORY: float64(100),
		DISK:   float64(1000),
	}
}

func (s *ResPoolSuite) createTestResourcePool() ResPool {
	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root,
		poolConfig, s.cfg)
	s.NoError(err)
	return resPoolNode
}

func (s *ResPoolSuite) TestNewResPool() {
	tt := []struct {
		poolConfig *pb_respool.ResourcePoolConfig
		err        string
	}{
		{
			poolConfig: &pb_respool.ResourcePoolConfig{
				Name:      _testResPoolName,
				Parent:    &_rootResPoolID,
				Resources: s.getResources(),
				Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
			},
		},
		{
			poolConfig: &pb_respool.ResourcePoolConfig{
				Name:      _testResPoolName,
				Parent:    &_rootResPoolID,
				Resources: s.getResources(),
				Policy:    pb_respool.SchedulingPolicy_UNKNOWN,
			},
			err: "error creating resource pool %s: invalid queue type",
		},
		{
			poolConfig: &pb_respool.ResourcePoolConfig{
				Name:      _testResPoolName,
				Parent:    &_rootResPoolID,
				Resources: s.getResources(),
			},
			err: "error creating resource pool %s: invalid queue type",
		},
		{
			poolConfig: nil,
			err:        "error creating resource pool %s; ResourcePoolConfig is nil",
		},
	}

	for _, t := range tt {
		id := uuid.New()
		resPool, err := NewRespool(tally.NoopScope, id, s.root, t.poolConfig, s.cfg)
		if err != nil {
			s.Equal(fmt.Sprintf(t.err, id), err.Error())
			s.Nil(resPool)
			continue
		}
		s.Nil(err)
		s.Equal(id, resPool.ID())
		s.NotNil(resPool.Parent())
		s.Equal(t.poolConfig.Parent.Value, resPool.Parent().ID())
		s.True(resPool.Children().Len() == 0)
		s.True(resPool.IsLeaf())
		s.Equal(t.poolConfig, resPool.ResourcePoolConfig())
		s.Equal(t.poolConfig.Name, resPool.Name())
		s.Equal(resPool.GetPath(), "/"+_testResPoolName)
		s.False(resPool.IsRoot())
	}
}

func (s *ResPoolSuite) TestResPoolEnqueue() {

	tt := []struct {
		respoolNode ResPool
		isLeaf      bool
		emptyGang   bool
		err         error
	}{
		{
			respoolNode: s.createTestResourcePool(),
			isLeaf:      true,
			emptyGang:   false,
			err:         nil,
		},
		{
			respoolNode: s.createTestResourcePool(),
			isLeaf:      true,
			emptyGang:   true,
			err:         fmt.Errorf("gang has no elements"),
		},
		{
			respoolNode: s.root,
			isLeaf:      false,
			emptyGang:   false,
			err: fmt.Errorf("resource pool %s is not a leaf node",
				_rootResPoolID.Value),
		},
	}

	addChildren := func(respool ResPool) ResPool {
		children := list.New()
		childResPool := s.createTestResourcePool()
		children.PushBack(childResPool)
		respool.SetChildren(children)
		return respool
	}

out:
	for _, t := range tt {
		respool := t.respoolNode
		if !t.isLeaf {
			respool = addChildren(respool)
		}

		for _, task := range s.getTasks() {
			gang := makeTaskGang(task)
			if t.emptyGang {
				gang = nil
			}
			err := respool.EnqueueGang(gang)
			if t.err != nil {
				s.EqualError(t.err, err.Error())
				// checking once is good enough
				continue out
			}
			s.NoError(err)
		}

		resPool, ok := respool.(*resPool)
		s.True(ok)

		// SchedulingPolicy_PriorityFIFO uses PriorityQueue
		priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
		s.True(ok)
		s.Equal(2, priorityQueue.Len(2))
		s.Equal(1, priorityQueue.Len(1))
		s.Equal(1, priorityQueue.Len(0))
	}

}

func (s *ResPoolSuite) TestResPoolDequeue() {
	resPoolNode := s.createTestResourcePool()
	resPoolNode.SetNonSlackEntitlement(s.getEntitlement())

	for _, t := range s.getTasks() {
		resPoolNode.EnqueueGang(makeTaskGang(t))
	}

	dequeuedGangs, err := resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	resPool, ok := resPoolNode.(*resPool)
	s.True(ok)

	// SchedulingPolicy_PriorityFIFO uses PriorityQueue
	priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
	s.True(ok)

	// 1 task should've been dequeued
	s.Equal(1, priorityQueue.Len(2))

	dequeuedGangs, err = resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	// 1 task should've been dequeued
	s.Equal(0, priorityQueue.Len(2))
}

func (s *ResPoolSuite) TestResPoolDequeueNonLeaf() {
	resPoolNode := s.createTestResourcePool()
	children := list.New()
	children.PushBack(resPoolNode)
	s.root.SetChildren(children)
	dequeuedGangs, err := s.root.DequeueGangs(1)
	s.Error(err)
	s.EqualError(err, "resource pool root is not a leaf node")
	s.Nil(dequeuedGangs)
}

func (s *ResPoolSuite) TestResPoolTaskCanBeDequeued() {
	resPoolNode := s.createTestResourcePool()
	resPoolNode.SetNonSlackEntitlement(s.getEntitlement())

	for _, t := range s.getTasks() {
		resPoolNode.EnqueueGang(makeTaskGang(t))
	}

	dequeuedGangs, err := resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	resPool, ok := resPoolNode.(*resPool)
	s.True(ok)

	// SchedulingPolicy_PriorityFIFO uses PriorityQueue
	priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
	s.True(ok)

	// 1 task should've been deququeued
	s.Equal(1, priorityQueue.Len(2))

	dequeuedGangs, err = resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	// 1 task should've been deququeued
	s.Equal(0, priorityQueue.Len(2))

	// Adding task which has more resources then resource pool
	bigtask := &resmgr.Task{
		Name:     "job3-1",
		Priority: 3,
		JobId:    &peloton.JobID{Value: "job3"},
		Id:       &peloton.TaskID{Value: "job3-1"},
		Resource: &task.ResourceConfig{
			CpuLimit:    200,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
	}
	resPoolNode.EnqueueGang(makeTaskGang(bigtask))
	dequeuedGangs, err = resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Nil(dequeuedGangs)
	resPoolNode.SetNonSlackEntitlement(
		&scalar.Resources{
			CPU:    500,
			MEMORY: 500,
			GPU:    4,
			DISK:   500,
		})
	dequeuedGangs, err = resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))
}

func (s *ResPoolSuite) TestEntitlement() {
	resPoolNode := s.createTestResourcePool()
	resPoolNode.UpdateResourceMetrics()
	resPoolNode.SetNonSlackEntitlement(s.getEntitlement())
	expectedEntitlement := &scalar.Resources{
		CPU:    float64(100),
		MEMORY: float64(1000),
		DISK:   float64(100),
		GPU:    float64(2),
	}
	s.Equal(expectedEntitlement, resPoolNode.GetNonSlackEntitlement())

	expectedEntitlement = &scalar.Resources{
		CPU:    float64(100),
		MEMORY: float64(1000),
		DISK:   float64(100),
		GPU:    float64(3),
	}
	resPoolNode.SetSlackEntitlement(expectedEntitlement)
	s.Equal(expectedEntitlement, resPoolNode.GetSlackEntitlement())
}

func (s *ResPoolSuite) TestSlackEntitlement() {
	resPoolNode := s.createTestResourcePool()
	resPoolNode.SetSlackEntitlement(s.getSlackEntitlement())
	expectedEntitlement := &scalar.Resources{
		CPU:    float64(80),
		MEMORY: float64(200),
		DISK:   float64(20),
		GPU:    float64(0),
	}
	s.Equal(expectedEntitlement, resPoolNode.GetSlackEntitlement())

	expectedEntitlement = &scalar.Resources{
		CPU:    float64(1000),
		MEMORY: float64(1000),
		DISK:   float64(100),
		GPU:    float64(3),
	}
	resPoolNode.SetSlackEntitlement(expectedEntitlement)
	s.Equal(expectedEntitlement, resPoolNode.GetSlackEntitlement())
}

func (s *ResPoolSuite) TestAllocation() {
	resPoolNode := s.createTestResourcePool()
	resPoolNode.SetNonSlackEntitlement(s.getNonSlackEntitlement())

	totalAlloc := scalar.NewAllocation()
	for _, t := range s.getTasks() {
		resPoolNode.EnqueueGang(makeTaskGang(t))
		totalAlloc = totalAlloc.Add(scalar.GetTaskAllocation(t))
	}
	dequeuedGangs, err := resPoolNode.DequeueGangs(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	resourceAlloc := resPoolNode.GetTotalAllocatedResources()
	s.NotNil(resourceAlloc)
	s.Equal(float64(1), resourceAlloc.CPU)
	s.Equal(float64(100), resourceAlloc.MEMORY)
	s.Equal(float64(10), resourceAlloc.DISK)
	s.Equal(float64(0), resourceAlloc.GPU)

	err = resPoolNode.SubtractFromAllocation(totalAlloc)
	s.NoError(err)
	newAllocation := resPoolNode.GetTotalAllocatedResources()
	s.NotNil(newAllocation)
	s.Equal(float64(0), newAllocation.CPU)
	s.Equal(float64(0), newAllocation.MEMORY)
	s.Equal(float64(0), newAllocation.DISK)
	s.Equal(float64(0), newAllocation.GPU)

	resPoolNode.AddToAllocation(totalAlloc)
	newAllocation = resPoolNode.GetTotalAllocatedResources()
	s.NotNil(newAllocation)
	s.Equal(float64(1), resourceAlloc.CPU)
	s.Equal(float64(100), resourceAlloc.MEMORY)
	s.Equal(float64(10), resourceAlloc.DISK)
	s.Equal(float64(0), resourceAlloc.GPU)
}

func (s *ResPoolSuite) TestCalculateAllocation() {
	rootID := peloton.ResourcePoolID{Value: "root"}
	respool1ID := peloton.ResourcePoolID{Value: "respool1"}
	respool2ID := peloton.ResourcePoolID{Value: "respool2"}
	respool11ID := peloton.ResourcePoolID{Value: "respool11"}
	respool12ID := peloton.ResourcePoolID{Value: "respool12"}
	respool21ID := peloton.ResourcePoolID{Value: "respool21"}

	poolConfigroot := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolRoot, err := NewRespool(tally.NoopScope, rootID.Value, nil,
		poolConfigroot, s.cfg)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value,
		resPoolRoot, poolConfig1, s.cfg)
	s.NoError(err)
	resPoolNode1.SetNonSlackEntitlement(s.getEntitlement())

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value,
		resPoolRoot, poolConfig2, s.cfg)
	s.NoError(err)
	resPoolNode2.SetNonSlackEntitlement(s.getEntitlement())

	rootChildrenList := list.New()
	rootChildrenList.PushBack(resPoolNode1)
	rootChildrenList.PushBack(resPoolNode2)
	resPoolRoot.SetChildren(rootChildrenList)

	poolConfig11 := &pb_respool.ResourcePoolConfig{
		Name:      "respool11",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode11, err := NewRespool(tally.NoopScope, respool11ID.Value,
		resPoolNode1, poolConfig11, s.cfg)
	s.NoError(err)
	resPoolNode11.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode11.SetTotalAllocatedResources(s.getAllocation())

	poolConfig12 := &pb_respool.ResourcePoolConfig{
		Name:      "respool12",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode12, err := NewRespool(tally.NoopScope, respool12ID.Value,
		resPoolNode1, poolConfig12, s.cfg)
	s.NoError(err)
	resPoolNode12.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode12.SetTotalAllocatedResources(s.getAllocation())

	node1ChildrenList := list.New()
	node1ChildrenList.PushBack(resPoolNode11)
	node1ChildrenList.PushBack(resPoolNode12)
	resPoolNode1.SetChildren(node1ChildrenList)

	poolConfig21 := &pb_respool.ResourcePoolConfig{
		Name:      "respool21",
		Parent:    &respool2ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode21, err := NewRespool(tally.NoopScope, respool21ID.Value,
		resPoolNode2, poolConfig21, s.cfg)
	s.NoError(err)
	resPoolNode21.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode21.SetTotalAllocatedResources(s.getAllocation())
	node2ChildrenList := list.New()
	node2ChildrenList.PushBack(resPoolNode21)
	resPoolNode2.SetChildren(node2ChildrenList)
	resPoolRoot.CalculateTotalAllocatedResources()

	allocationroot := resPoolRoot.GetTotalAllocatedResources()
	s.NotNil(allocationroot)
	s.Equal(float64(300), allocationroot.CPU)
	s.Equal(float64(300), allocationroot.MEMORY)
	s.Equal(float64(3000), allocationroot.DISK)
	s.Equal(float64(3), allocationroot.GPU)

	allocation1 := resPoolNode1.GetTotalAllocatedResources()
	s.NotNil(allocation1)
	s.Equal(float64(200), allocation1.CPU)
	s.Equal(float64(200), allocation1.MEMORY)
	s.Equal(float64(2000), allocation1.DISK)
	s.Equal(float64(2), allocation1.GPU)

	allocation2 := resPoolNode2.GetTotalAllocatedResources()
	s.NotNil(allocation2)
	s.Equal(float64(100), allocation2.CPU)
	s.Equal(float64(100), allocation2.MEMORY)
	s.Equal(float64(1000), allocation2.DISK)
	s.Equal(float64(1), allocation2.GPU)

	allocation11 := resPoolNode11.GetTotalAllocatedResources()
	s.NotNil(allocation11)
	s.Equal(float64(100), allocation11.CPU)
	s.Equal(float64(100), allocation11.MEMORY)
	s.Equal(float64(1000), allocation11.DISK)
	s.Equal(float64(1), allocation11.GPU)
}

func (s *ResPoolSuite) TestCalculateDemand() {
	respool1ID := peloton.ResourcePoolID{Value: "respool1"}
	respool2ID := peloton.ResourcePoolID{Value: "respool2"}
	respool11ID := peloton.ResourcePoolID{Value: "respool11"}
	respool12ID := peloton.ResourcePoolID{Value: "respool12"}
	respool21ID := peloton.ResourcePoolID{Value: "respool21"}

	poolConfigroot := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolroot, err := NewRespool(tally.NoopScope, _rootResPoolID.Value,
		nil, poolConfigroot, s.cfg)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value,
		resPoolroot, poolConfig1, s.cfg)
	s.NoError(err)
	resPoolNode1.SetNonSlackEntitlement(s.getEntitlement())

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value,
		resPoolroot, poolConfig2, s.cfg)
	s.NoError(err)
	resPoolNode2.SetNonSlackEntitlement(s.getEntitlement())

	rootChildrenList := list.New()
	rootChildrenList.PushBack(resPoolNode1)
	rootChildrenList.PushBack(resPoolNode2)
	resPoolroot.SetChildren(rootChildrenList)

	poolConfig11 := &pb_respool.ResourcePoolConfig{
		Name:      "respool11",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode11, err := NewRespool(tally.NoopScope, respool11ID.Value,
		resPoolNode1, poolConfig11, s.cfg)
	s.NoError(err)
	resPoolNode11.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode11.SetTotalAllocatedResources(s.getAllocation())
	resPoolNode11.AddToDemand(s.getDemand())
	resPoolNode11.AddToSlackDemand(s.getSlackDemand())

	poolConfig12 := &pb_respool.ResourcePoolConfig{
		Name:      "respool12",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode12, err := NewRespool(tally.NoopScope, respool12ID.Value,
		resPoolNode1, poolConfig12, s.cfg)
	s.NoError(err)
	resPoolNode12.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode12.SetTotalAllocatedResources(s.getAllocation())
	resPoolNode12.AddToDemand(s.getDemand())

	node1ChildrenList := list.New()
	node1ChildrenList.PushBack(resPoolNode11)
	node1ChildrenList.PushBack(resPoolNode12)
	resPoolNode1.SetChildren(node1ChildrenList)

	poolConfig21 := &pb_respool.ResourcePoolConfig{
		Name:      "respool21",
		Parent:    &respool2ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode21, err := NewRespool(tally.NoopScope, respool21ID.Value,
		resPoolNode2, poolConfig21, s.cfg)
	s.NoError(err)
	resPoolNode21.SetNonSlackEntitlement(s.getEntitlement())
	resPoolNode21.SetTotalAllocatedResources(s.getAllocation())
	resPoolNode21.AddToDemand(s.getDemand())
	node2ChildrenList := list.New()
	node2ChildrenList.PushBack(resPoolNode21)
	resPoolNode2.SetChildren(node2ChildrenList)

	resPoolroot.CalculateDemand()
	resPoolroot.CalculateSlackDemand()

	demandRoot := resPoolroot.GetDemand()
	s.NotNil(demandRoot)
	s.Equal(float64(300), demandRoot.CPU)
	s.Equal(float64(300), demandRoot.MEMORY)
	s.Equal(float64(3000), demandRoot.DISK)
	s.Equal(float64(3), demandRoot.GPU)

	demand1 := resPoolNode1.GetDemand()
	s.NotNil(demand1)
	s.Equal(float64(200), demand1.CPU)
	s.Equal(float64(200), demand1.MEMORY)
	s.Equal(float64(2000), demand1.DISK)
	s.Equal(float64(2), demand1.GPU)

	demand2 := resPoolNode2.GetDemand()
	s.NotNil(demand2)
	s.Equal(float64(100), demand2.CPU)
	s.Equal(float64(100), demand2.MEMORY)
	s.Equal(float64(1000), demand2.DISK)
	s.Equal(float64(1), demand2.GPU)

	demand11 := resPoolNode11.GetDemand()
	s.NotNil(demand11)
	s.Equal(float64(100), demand11.CPU)
	s.Equal(float64(100), demand11.MEMORY)
	s.Equal(float64(1000), demand11.DISK)
	s.Equal(float64(1), demand11.GPU)

	slackDemand11 := resPoolNode11.GetSlackDemand()
	s.NotNil(slackDemand11)
	s.Equal(float64(10), slackDemand11.CPU)
	s.Equal(float64(10), slackDemand11.MEMORY)
	s.Equal(float64(10), slackDemand11.DISK)
	s.Equal(float64(0), slackDemand11.GPU)

	demand12 := resPoolNode12.GetDemand()
	s.NotNil(demand12)
	s.Equal(float64(100), demand12.CPU)
	s.Equal(float64(100), demand12.MEMORY)
	s.Equal(float64(1000), demand12.DISK)
	s.Equal(float64(1), demand12.GPU)

	err = resPoolNode11.SubtractFromDemand(demand11)
	s.NoError(err)
	resPoolroot.CalculateDemand()
	demand11 = resPoolNode11.GetDemand()
	s.NotNil(demand11)
	s.Equal(float64(0), demand11.CPU)
	s.Equal(float64(0), demand11.MEMORY)
	s.Equal(float64(0), demand11.DISK)
	s.Equal(float64(0), demand11.GPU)

	demandRoot = resPoolroot.GetDemand()
	s.NotNil(demandRoot)
	s.Equal(float64(200), demandRoot.CPU)
	s.Equal(float64(200), demandRoot.MEMORY)
	s.Equal(float64(2000), demandRoot.DISK)
	s.Equal(float64(2), demandRoot.GPU)

	demand1 = resPoolNode1.GetDemand()
	s.NotNil(demand1)
	s.Equal(float64(100), demand1.CPU)
	s.Equal(float64(100), demand1.MEMORY)
	s.Equal(float64(1000), demand1.DISK)
	s.Equal(float64(1), demand1.GPU)

	demand2 = resPoolNode2.GetDemand()
	s.NotNil(demand2)
	s.Equal(float64(100), demand2.CPU)
	s.Equal(float64(100), demand2.MEMORY)
	s.Equal(float64(1000), demand2.DISK)
	s.Equal(float64(1), demand2.GPU)

	resPoolNode11.SubtractFromSlackDemand(&scalar.Resources{
		CPU:    1,
		MEMORY: 1,
		DISK:   1,
		GPU:    0})
	expectedSlackDemand := &scalar.Resources{
		CPU:    9,
		MEMORY: 9,
		DISK:   9,
		GPU:    0,
	}
	s.Equal(resPoolNode11.GetSlackDemand().String(), expectedSlackDemand.String())
}

func (s *ResPoolSuite) TestResPoolDequeueError() {
	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root,
		poolConfig, s.cfg)
	s.NoError(err)

	for _, t := range s.getTasks() {
		resPoolNode.EnqueueGang(makeTaskGang(t))
	}

	_, err = resPoolNode.DequeueGangs(0)
	s.EqualError(
		err,
		"limit 0 is not valid",
	)
	s.Error(err)
}

func (s *ResPoolSuite) TestGetLimits() {
	resourceConfigs := make(map[string]*pb_respool.ResourceConfig)
	for _, config := range s.getResources() {
		resourceConfigs[config.Kind] = config
	}

	resources := getLimits(resourceConfigs)
	s.Equal(float64(1000), resources.GetCPU())
	s.Equal(float64(4), resources.GetGPU())
	s.Equal(float64(1000), resources.GetDisk())
	s.Equal(float64(1000), resources.GetMem())
}

func (s *ResPoolSuite) TestGetShare() {
	resourceConfigs := make(map[string]*pb_respool.ResourceConfig)
	for _, config := range s.getResources() {
		resourceConfigs[config.Kind] = config
	}

	resources := getShare(resourceConfigs)
	s.Equal(float64(1), resources.GetCPU())
	s.Equal(float64(1), resources.GetGPU())
	s.Equal(float64(1), resources.GetDisk())
	s.Equal(float64(1), resources.GetMem())
}

func (s *ResPoolSuite) TestGetGangResources() {
	rmTasks := []*resmgr.Task{
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
	}
	gang := &resmgrsvc.Gang{
		Tasks: rmTasks,
	}
	res := scalar.GetGangResources(gang)
	s.Equal(float64(1), res.CPU)
	s.Equal(float64(0), res.GPU)
	s.Equal(float64(100), res.MEMORY)
	s.Equal(float64(10), res.DISK)
}

func (s *ResPoolSuite) TestTaskValidation() {
	resPoolNode1 := s.createTestResourcePool()
	s.Equal(resPoolNode1.GetEntitlement().String(), scalar.ZeroResource.String())

	resPoolNode1.SetNonSlackEntitlement(s.getEntitlement())

	for _, t := range s.getTasks() {
		resPoolNode1.EnqueueGang(makeTaskGang(t))
		resPoolNode1.AddInvalidTask(t.Id)
	}
	demand := resPoolNode1.GetDemand()
	s.NotNil(demand)
	s.Equal(float64(4), demand.CPU)
	s.Equal(float64(400), demand.MEMORY)
	s.Equal(float64(40), demand.DISK)
	s.Equal(float64(0), demand.GPU)

	gangs, err := resPoolNode1.DequeueGangs(4)
	s.NoError(err)
	s.Equal(0, len(gangs))
	newDemand := resPoolNode1.GetDemand()
	s.NotNil(newDemand)
	s.Equal(float64(0), newDemand.CPU)
	s.Equal(float64(0), newDemand.MEMORY)
	s.Equal(float64(0), newDemand.DISK)
	s.Equal(float64(0), newDemand.GPU)
}

func (s *ResPoolSuite) TestSetParent() {
	resPool := s.createTestResourcePool()
	s.Equal(resPool.GetPath(), "/"+_testResPoolName)

	newParentConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	newParentPool, err := NewRespool(tally.NoopScope, "respool2", s.root,
		newParentConfig, s.cfg)
	s.NoError(err)
	resPool.SetParent(newParentPool)
	s.Equal("/respool2/"+_testResPoolName, resPool.GetPath())
}

func (s *ResPoolSuite) TestSetResourcePoolConfig() {
	respool := s.createTestResourcePool()
	expectedPoolConfig := &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	expectedResourcesMap := map[string]*pb_respool.ResourceConfig{
		"cpu": {
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		"memory": {
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
			Limit:       1000,
		},
		"disk": {
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		"gpu": {
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	s.Equal(expectedPoolConfig, respool.ResourcePoolConfig())
	s.Equal(expectedResourcesMap, respool.Resources())

	newCPUConfig := &pb_respool.ResourceConfig{
		Share:       2,
		Kind:        "cpu",
		Reservation: 1000,
		Limit:       1000,
	}
	resources := []*pb_respool.ResourceConfig{
		newCPUConfig,
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
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

	expectedResourcesMap["cpu"] = newCPUConfig
	newPoolConfig := &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: resources,
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	respool.SetResourcePoolConfig(newPoolConfig)
	s.Equal(newPoolConfig, respool.ResourcePoolConfig())
	s.Equal(expectedResourcesMap, respool.Resources())
}

func (s *ResPoolSuite) TestToResourcePoolInfo() {
	respoolNode := s.createTestResourcePool()
	info := respoolNode.ToResourcePoolInfo()
	s.Equal(info.GetConfig(), &pb_respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	})
	for _, resUsage := range info.GetUsage() {
		s.Equal(float64(0), resUsage.GetAllocation())
	}
	s.Equal(0, len(info.GetChildren()))
	s.Equal(&_rootResPoolID, info.GetParent())
	s.Equal("/"+_testResPoolName, info.GetPath().GetValue())
}

func (s *ResPoolSuite) TestAggregatedChildrenReservations() {
	respool1ID := peloton.ResourcePoolID{Value: "respool1"}
	respool2ID := peloton.ResourcePoolID{Value: "respool2"}

	poolConfigroot := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolroot, err := NewRespool(tally.NoopScope, _rootResPoolID.Value,
		nil, poolConfigroot, s.cfg)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value,
		resPoolroot, poolConfig1, s.cfg)
	s.NoError(err)

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value,
		resPoolroot, poolConfig2, s.cfg)
	s.NoError(err)

	children := list.New()
	children.PushBack(resPoolNode1)
	children.PushBack(resPoolNode2)
	resPoolroot.SetChildren(children)

	ar, err := resPoolroot.AggregatedChildrenReservations()
	s.NoError(err)
	s.Equal(map[string]float64{
		"cpu":    float64(200),
		"disk":   float64(200),
		"gpu":    float64(4),
		"memory": float64(2000),
	}, ar)

	ar, err = resPoolNode1.AggregatedChildrenReservations()
	s.NoError(err)
	s.Equal(map[string]float64{}, ar)
	ar, err = resPoolNode2.AggregatedChildrenReservations()
	s.NoError(err)
	s.Equal(map[string]float64{}, ar)
}

func (s *ResPoolSuite) TestResPoolPeekGangs() {

	for _, qt := range []QueueType{
		PendingQueue,
		ControllerQueue,
		NonPreemptibleQueue,
		RevocableQueue,
	} {
		respool := s.createTestResourcePool()

		// no gangs yet
		gangs, err := respool.PeekGangs(qt, 10)
		s.Error(err)
		s.EqualError(err, "peek failed, queue is empty",
			"queue type failed:%v", qt)

		// enqueue 4 gangs
		for _, t := range s.getTasks() {
			gang := makeTaskGang(t)
			resPool, ok := respool.(*resPool)
			s.True(ok)
			err := resPool.queue(qt).Enqueue(gang)
			s.NoError(err)
		}

		gangs, err = respool.PeekGangs(qt, 10)
		s.NoError(err, "queue type failed:%v", qt)
		s.Equal(4, len(gangs), "queue type failed:%v", qt)
	}
}

func (s *ResPoolSuite) TestResPoolControllerLimit() {
	rootConfig := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		ControllerLimit: &pb_respool.ControllerLimit{
			MaxPercent: 1, // 1 percent
		},
	}

	node := s.createTestResourcePool()
	resPool, ok := node.(*resPool)
	s.True(ok)

	resPool.initialize(rootConfig)
	// 1 % of reservation
	s.Equal(float64(1), resPool.controllerLimit.CPU)
	s.Equal(float64(0.02), resPool.controllerLimit.GPU)
	s.Equal(float64(10), resPool.controllerLimit.MEMORY)
	s.Equal(float64(1), resPool.controllerLimit.CPU)
}

func (s *ResPoolSuite) TestAggregateQueue() {
	respool1ID := peloton.ResourcePoolID{Value: "respool1"}
	respool2ID := peloton.ResourcePoolID{Value: "respool2"}
	respool11ID := peloton.ResourcePoolID{Value: "respool11"}
	respool12ID := peloton.ResourcePoolID{Value: "respool12"}
	respool21ID := peloton.ResourcePoolID{Value: "respool21"}

	poolConfigroot := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolroot, err := NewRespool(tally.NoopScope, _rootResPoolID.Value,
		nil, poolConfigroot, s.cfg)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value,
		resPoolroot, poolConfig1, s.cfg)
	s.NoError(err)

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value,
		resPoolroot, poolConfig2, s.cfg)
	s.NoError(err)

	rootChildrenList := list.New()
	rootChildrenList.PushBack(resPoolNode1)
	rootChildrenList.PushBack(resPoolNode2)
	resPoolroot.SetChildren(rootChildrenList)

	poolConfig11 := &pb_respool.ResourcePoolConfig{
		Name:      "respool11",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode11, err := NewRespool(tally.NoopScope, respool11ID.Value,
		resPoolNode1, poolConfig11, s.cfg)
	s.NoError(err)

	poolConfig12 := &pb_respool.ResourcePoolConfig{
		Name:      "respool12",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode12, err := NewRespool(tally.NoopScope, respool12ID.Value,
		resPoolNode1, poolConfig12, s.cfg)
	s.NoError(err)

	node1ChildrenList := list.New()
	node1ChildrenList.PushBack(resPoolNode11)
	node1ChildrenList.PushBack(resPoolNode12)
	resPoolNode1.SetChildren(node1ChildrenList)

	for _, t := range s.getTasks() {
		gang := makeTaskGang(t)
		resPool, ok := resPoolNode11.(*resPool)
		s.True(ok)
		err := resPool.queue(PendingQueue).Enqueue(gang)
		s.NoError(err)
		err = resPool.queue(ControllerQueue).Enqueue(gang)
		s.NoError(err)
	}

	for _, t := range s.getTasks() {
		gang := makeTaskGang(t)
		resPool, ok := resPoolNode12.(*resPool)
		s.True(ok)
		err = resPool.queue(NonPreemptibleQueue).Enqueue(gang)
		s.NoError(err)
		err = resPool.queue(RevocableQueue).Enqueue(gang)
		s.NoError(err)
	}

	poolConfig21 := &pb_respool.ResourcePoolConfig{
		Name:      "respool21",
		Parent:    &respool2ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode21, err := NewRespool(tally.NoopScope, respool21ID.Value,
		resPoolNode2, poolConfig21, s.cfg)
	s.NoError(err)
	node2ChildrenList := list.New()
	node2ChildrenList.PushBack(resPoolNode21)
	resPoolNode2.SetChildren(node2ChildrenList)

	for _, t := range s.getTasks() {
		gang := makeTaskGang(t)
		resPool, ok := resPoolNode21.(*resPool)
		s.True(ok)
		err := resPool.queue(PendingQueue).Enqueue(gang)
		s.NoError(err)
		err = resPool.queue(NonPreemptibleQueue).Enqueue(gang)
		s.NoError(err)
	}

	resPoolRoot := resPoolroot.(*resPool)

	s.Equal(resPoolRoot.aggregateQueueByType(PendingQueue), 8)
	s.Equal(resPoolRoot.aggregateQueueByType(NonPreemptibleQueue), 8)
	s.Equal(resPoolRoot.aggregateQueueByType(ControllerQueue), 4)
	s.Equal(resPoolRoot.aggregateQueueByType(RevocableQueue), 4)
}

func TestResPoolSuite(t *testing.T) {
	suite.Run(t, new(ResPoolSuite))
}
