package respool

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/queue"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ResPoolSuite struct {
	suite.Suite
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
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	id := uuid.New()
	resPool, err := NewRespool(tally.NoopScope, id, nil, poolConfig)
	s.NoError(err)

	s.Equal(id, resPool.ID())
	s.Equal(nil, resPool.Parent())
	s.True(resPool.Children().Len() == 0)
	s.True(resPool.IsLeaf())
	s.Equal(poolConfig, resPool.ResourcePoolConfig())
	s.Equal("respool1", resPool.Name())

	resPool, err = NewRespool(tally.NoopScope, id, nil, nil)
	s.Error(err)

	poolConfig.Policy = pb_respool.SchedulingPolicy_UNKNOWN
	resPool, err = NewRespool(tally.NoopScope, id, nil, poolConfig)
	s.Error(err)
}

func (s *ResPoolSuite) TestResPoolError() {
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
	}

	id := uuid.New()
	resPool, err := NewRespool(tally.NoopScope, id, nil, poolConfig)

	s.EqualError(
		err,
		fmt.Sprintf(
			"error creating resource pool %s: Invalid queue Type",
			id),
	)
	s.Nil(resPool)

}

func (s *ResPoolSuite) TestResPoolEnqueue() {
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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

	err = resPoolNode.MarkItDone(allocation)
	s.NoError(err)
	allocation = resPoolNode.GetAllocation()
	s.NotNil(allocation)
	s.Equal(float64(0), allocation.CPU)
	s.Equal(float64(0), allocation.MEMORY)
	s.Equal(float64(0), allocation.DISK)
	s.Equal(float64(0), allocation.GPU)
}

func (s *ResPoolSuite) TestResPoolDequeueError() {
	rootID := pb_respool.ResourcePoolID{Value: "root"}

	poolConfig := &pb_respool.ResourcePoolConfig{
		Name:      "respool1",
		Parent:    &rootID,
		Resources: s.getResources(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), nil, poolConfig)
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
