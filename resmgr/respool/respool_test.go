package respool

import (
	"container/list"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ResPoolSuite struct {
	root ResPool
	suite.Suite
}

func (s *ResPoolSuite) SetupSuite() {
	// root resource pool
	rootConfig := &pb_respool.ResourcePoolConfig{
		Name:      "root",
		Parent:    nil,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	rootResPool, err := NewRespool(tally.NoopScope, RootResPoolID, nil, rootConfig)
	s.NoError(err)
	s.True(rootResPool.IsRoot())
	s.root = rootResPool
}

func TestResPoolSuite(t *testing.T) {
	suite.Run(t, new(ResPoolSuite))
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
}

func (s *ResPoolSuite) TestResPool() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	id := uuid.New()
	resPool, err := NewRespool(tally.NoopScope, id, s.root, poolConfig)
	s.NoError(err)

	s.Equal(id, resPool.ID())
	s.NotNil(resPool.Parent())
	s.True(resPool.Children().Len() == 0)
	s.True(resPool.IsLeaf())
	s.Equal(poolConfig, resPool.ResourcePoolConfig())
	s.Equal("respool1", resPool.Name())
	s.Equal(resPool.GetPath(), "/respool1")
	s.False(resPool.IsRoot())

	resPool, err = NewRespool(tally.NoopScope, id, s.root, nil)
	s.Error(err)

	poolConfig.Policy = pb_respool.SchedulingPolicy_UNKNOWN
	resPool, err = NewRespool(tally.NoopScope, id, s.root, poolConfig)
	s.Error(err)
}

func (s *ResPoolSuite) TestResPoolError() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
	}

	id := uuid.New()
	resPool, err := NewRespool(tally.NoopScope, id, s.root, poolConfig)

	s.EqualError(
		err,
		fmt.Sprintf(
			"error creating resource pool %s: Invalid queue Type",
			id),
	)
	s.Nil(resPool)

}

func (s *ResPoolSuite) TestResPoolEnqueue() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)

	for _, task := range s.getTasks() {
		resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(task))
	}

	resPool, ok := resPoolNode.(*resPool)
	s.True(ok)

	// SchedulingPolicy_PriorityFIFO uses PriorityQueue
	priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
	s.True(ok)

	s.Equal(2, priorityQueue.Len(2))
	s.Equal(1, priorityQueue.Len(1))
	s.Equal(1, priorityQueue.Len(0))
}

func (s *ResPoolSuite) TestResPoolEnqueueError() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)

	err = resPoolNode.EnqueueGang(nil)

	s.EqualError(
		err,
		"gang has no elements",
	)
}

func (s *ResPoolSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}

func (s *ResPoolSuite) TestResPoolDequeue() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)
	resPoolNode.SetEntitlement(s.getEntitlement())

	for _, task := range s.getTasks() {
		resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(task))
	}

	dequeuedGangs, err := resPoolNode.DequeueGangList(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	resPool, ok := resPoolNode.(*resPool)
	s.True(ok)

	// SchedulingPolicy_PriorityFIFO uses PriorityQueue
	priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
	s.True(ok)

	// 1 task should've been deququeued
	s.Equal(1, priorityQueue.Len(2))

	dequeuedGangs, err = resPoolNode.DequeueGangList(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	// 1 task should've been deququeued
	s.Equal(0, priorityQueue.Len(2))
}

func (s *ResPoolSuite) TestResPoolTaskCanBeDequeued() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)
	resPoolNode.SetEntitlement(s.getEntitlement())

	for _, task := range s.getTasks() {
		resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(task))
	}

	dequeuedGangs, err := resPoolNode.DequeueGangList(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))

	resPool, ok := resPoolNode.(*resPool)
	s.True(ok)

	// SchedulingPolicy_PriorityFIFO uses PriorityQueue
	priorityQueue, ok := resPool.pendingQueue.(*queue.PriorityQueue)
	s.True(ok)

	// 1 task should've been deququeued
	s.Equal(1, priorityQueue.Len(2))

	dequeuedGangs, err = resPoolNode.DequeueGangList(1)
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
	resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(bigtask))
	dequeuedGangs, err = resPoolNode.DequeueGangList(1)
	s.Error(err)
	s.Nil(dequeuedGangs)
	resPoolNode.SetEntitlementByKind(common.CPU, float64(500))
	dequeuedGangs, err = resPoolNode.DequeueGangList(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))
}

func (s *ResPoolSuite) TestAllocation() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)
	resPoolNode.SetEntitlement(s.getEntitlement())

	for _, task := range s.getTasks() {
		resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(task))
	}
	dequeuedGangs, err := resPoolNode.DequeueGangList(1)
	s.NoError(err)
	s.Equal(1, len(dequeuedGangs))
	allocation := resPoolNode.GetAllocation()
	s.NotNil(allocation)
	s.Equal(float64(1), allocation.CPU)
	s.Equal(float64(100), allocation.MEMORY)
	s.Equal(float64(10), allocation.DISK)
	s.Equal(float64(0), allocation.GPU)

	err = resPoolNode.SubtractFromAllocation(allocation)
	s.NoError(err)
	allocation = resPoolNode.GetAllocation()
	s.NotNil(allocation)
	s.Equal(float64(0), allocation.CPU)
	s.Equal(float64(0), allocation.MEMORY)
	s.Equal(float64(0), allocation.DISK)
	s.Equal(float64(0), allocation.GPU)
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

	resPoolroot, err := NewRespool(tally.NoopScope, rootID.Value, nil, poolConfigroot)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value, resPoolroot, poolConfig1)
	s.NoError(err)
	resPoolNode1.SetEntitlement(s.getEntitlement())

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value, resPoolroot, poolConfig2)
	s.NoError(err)
	resPoolNode2.SetEntitlement(s.getEntitlement())

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

	resPoolNode11, err := NewRespool(tally.NoopScope, respool11ID.Value, resPoolNode1, poolConfig11)
	s.NoError(err)
	resPoolNode11.SetEntitlement(s.getEntitlement())
	resPoolNode11.SetAllocation(s.getAllocation())

	poolConfig12 := &pb_respool.ResourcePoolConfig{
		Name:      "respool12",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode12, err := NewRespool(tally.NoopScope, respool12ID.Value, resPoolNode1, poolConfig12)
	s.NoError(err)
	resPoolNode12.SetEntitlement(s.getEntitlement())
	resPoolNode12.SetAllocation(s.getAllocation())

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

	resPoolNode21, err := NewRespool(tally.NoopScope, respool21ID.Value, resPoolNode2, poolConfig21)
	s.NoError(err)
	resPoolNode21.SetEntitlement(s.getEntitlement())
	resPoolNode21.SetAllocation(s.getAllocation())
	node2ChildrenList := list.New()
	node2ChildrenList.PushBack(resPoolNode21)
	resPoolNode2.SetChildren(node2ChildrenList)
	resPoolroot.CalculateAllocation()

	allocationroot := resPoolroot.GetAllocation()
	s.NotNil(allocationroot)
	s.Equal(float64(300), allocationroot.CPU)
	s.Equal(float64(300), allocationroot.MEMORY)
	s.Equal(float64(3000), allocationroot.DISK)
	s.Equal(float64(3), allocationroot.GPU)

	allocation1 := resPoolNode1.GetAllocation()
	s.NotNil(allocation1)
	s.Equal(float64(200), allocation1.CPU)
	s.Equal(float64(200), allocation1.MEMORY)
	s.Equal(float64(2000), allocation1.DISK)
	s.Equal(float64(2), allocation1.GPU)

	allocation2 := resPoolNode2.GetAllocation()
	s.NotNil(allocation2)
	s.Equal(float64(100), allocation2.CPU)
	s.Equal(float64(100), allocation2.MEMORY)
	s.Equal(float64(1000), allocation2.DISK)
	s.Equal(float64(1), allocation2.GPU)

	allocation11 := resPoolNode11.GetAllocation()
	s.NotNil(allocation11)
	s.Equal(float64(100), allocation11.CPU)
	s.Equal(float64(100), allocation11.MEMORY)
	s.Equal(float64(1000), allocation11.DISK)
	s.Equal(float64(1), allocation11.GPU)
}

func (s *ResPoolSuite) getAllocation() *scalar.Resources {
	return &scalar.Resources{
		CPU:    float64(100),
		GPU:    float64(1),
		MEMORY: float64(100),
		DISK:   float64(1000),
	}
}

func (s *ResPoolSuite) TestCalculateDemand() {
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

	resPoolroot, err := NewRespool(tally.NoopScope, rootID.Value, nil, poolConfigroot)
	s.NoError(err)

	poolConfig1 := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode1, err := NewRespool(tally.NoopScope, respool1ID.Value, resPoolroot, poolConfig1)
	s.NoError(err)
	resPoolNode1.SetEntitlement(s.getEntitlement())

	poolConfig2 := &pb_respool.ResourcePoolConfig{
		Name:      "respool2",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode2, err := NewRespool(tally.NoopScope, respool2ID.Value, resPoolroot, poolConfig2)
	s.NoError(err)
	resPoolNode2.SetEntitlement(s.getEntitlement())

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

	resPoolNode11, err := NewRespool(tally.NoopScope, respool11ID.Value, resPoolNode1, poolConfig11)
	s.NoError(err)
	resPoolNode11.SetEntitlement(s.getEntitlement())
	resPoolNode11.SetAllocation(s.getAllocation())
	resPoolNode11.AddToDemand(s.getDemand())

	poolConfig12 := &pb_respool.ResourcePoolConfig{
		Name:      "respool12",
		Parent:    &respool1ID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode12, err := NewRespool(tally.NoopScope, respool12ID.Value, resPoolNode1, poolConfig12)
	s.NoError(err)
	resPoolNode12.SetEntitlement(s.getEntitlement())
	resPoolNode12.SetAllocation(s.getAllocation())
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

	resPoolNode21, err := NewRespool(tally.NoopScope, respool21ID.Value, resPoolNode2, poolConfig21)
	s.NoError(err)
	resPoolNode21.SetEntitlement(s.getEntitlement())
	resPoolNode21.SetAllocation(s.getAllocation())
	resPoolNode21.AddToDemand(s.getDemand())
	node2ChildrenList := list.New()
	node2ChildrenList.PushBack(resPoolNode21)
	resPoolNode2.SetChildren(node2ChildrenList)

	resPoolroot.CalculateDemand()

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
}

func (s *ResPoolSuite) getDemand() *scalar.Resources {
	return &scalar.Resources{
		CPU:    float64(100),
		GPU:    float64(1),
		MEMORY: float64(100),
		DISK:   float64(1000),
	}
}

func (s *ResPoolSuite) TestResPoolDequeueError() {
	rootID := peloton.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, poolConfig)
	s.NoError(err)

	for _, task := range s.getTasks() {
		resPoolNode.EnqueueGang(resPoolNode.MakeTaskGang(task))
	}

	_, err = resPoolNode.DequeueGangList(0)
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

func (s *ResPoolSuite) TestGetReservation() {
	resourceConfigs := make(map[string]*pb_respool.ResourceConfig)
	for _, config := range s.getResources() {
		resourceConfigs[config.Kind] = config
	}

	resources := getReservations(resourceConfigs)
	s.Equal(float64(100), resources.GetCPU())
	s.Equal(float64(2), resources.GetGPU())
	s.Equal(float64(100), resources.GetDisk())
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
