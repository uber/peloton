package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
)

func (s *ResPoolSuite) respoolWithConfig(config *respool.ResourcePoolConfig) ResPool {
	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root, config)
	s.NoError(err)
	return resPoolNode
}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitValidationFail() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	task := s.getTasks()[0]
	gang := resPool.MakeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	resPool.AddToDemand(scalar.GetGangResources(gang))
	s.NoError(err)

	// check demand before
	s.Equal(float64(1), resPool.GetDemand().CPU)
	s.Equal(float64(100), resPool.GetDemand().MEMORY)
	s.Equal(float64(10), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)

	// mark the task is invalid
	resPool.AddInvalidTask(task.Id)

	// try and admit
	err = pending.TryAdmit(gang, resPool)
	s.Equal(err, errGangInvalid)

	// demand should be removed
	s.Equal(float64(0), resPool.GetDemand().CPU)
	s.Equal(float64(0), resPool.GetDemand().MEMORY)
	s.Equal(float64(0), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)

}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitSuccess() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	resPool.SetEntitlement(s.getEntitlement())

	task := s.getTasks()[0]
	gang := resPool.MakeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	resPool.AddToDemand(scalar.GetGangResources(gang))
	s.NoError(err)

	// allocation should be zero
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)

	err = pending.TryAdmit(gang, resPool)
	s.NoError(err)
	s.Equal(0, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(1), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(100), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(10), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)
}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitFailure() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	task := s.getTasks()[0]
	gang := resPool.MakeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	resPool.AddToDemand(scalar.GetGangResources(gang))
	s.NoError(err)

	// allocation should be zero
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)

	err = pending.TryAdmit(gang, resPool)
	s.Equal(err, errResourcePoolFull)
	s.Equal(1, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)
}

func (s *ResPoolSuite) TestBatchAdmissionController_PendingQueueAdmitter() {
	poolConfig := &respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    respool.SchedulingPolicy_PriorityFIFO,
		ControllerLimit: &respool.ControllerLimit{
			MaxPercent: 10,
		},
	}

	tt := []struct {
		canAdmit   bool
		err        error
		controller bool
	}{
		{
			// Tests a batch task at the head of queue which can be admitted
			canAdmit:   true,
			err:        nil,
			controller: false,
		},
		{
			// Tests a batch task at the head of queue which can not be admitted
			canAdmit:   false,
			err:        errResourcePoolFull,
			controller: false,
		},
		{
			// Tests a controller task at the head of queue which can be admitted
			canAdmit:   true,
			err:        nil,
			controller: true,
		},
		{
			// Tests a controller task at the head of queue which can not be
			// admitted
			canAdmit:   false,
			err:        errControllerTask,
			controller: true,
		},
	}

	for _, t := range tt {
		rp := s.respoolWithConfig(poolConfig)
		resPool, ok := rp.(*resPool)
		s.True(ok)
		if t.canAdmit {
			resPool.SetEntitlement(s.getEntitlement())
		} else if t.controller {
			// set the limit to zero
			resPool.controllerLimit = scalar.ZeroResource
		}

		task := s.getTasks()[0]
		task.Controller = t.controller
		gang := resPool.MakeTaskGang(task)
		err := resPool.EnqueueGang(gang)
		s.NoError(err)

		err = pending.TryAdmit(gang, resPool)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, t.controller)
		}
	}
}

func (s *ResPoolSuite) TestBatchAdmissionController_ControllerAdmitter() {
	poolConfig := &respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    respool.SchedulingPolicy_PriorityFIFO,
		ControllerLimit: &respool.ControllerLimit{
			MaxPercent: 10,
		},
	}

	tt := []struct {
		canAdmit bool
		err      error
	}{
		{ // Tests a controller task at the head of queue which can be admitted
			canAdmit: true,
			err:      nil,
		},
		{ // Tests a batch task at the head of queue which can nit be admitted
			canAdmit: false,
			err:      errResourcePoolFull,
		},
	}

	for _, t := range tt {
		rp := s.respoolWithConfig(poolConfig)
		resPool, ok := rp.(*resPool)
		s.True(ok)
		if t.canAdmit {
			resPool.SetEntitlement(s.getEntitlement())
		} else {
			// set the limit to zero
			resPool.controllerLimit = scalar.ZeroResource
		}

		task := s.getTasks()[0]
		task.Controller = true
		gang := resPool.MakeTaskGang(task)
		err := resPool.controllerQueue.Enqueue(gang)
		s.NoError(err)

		err = controller.TryAdmit(gang, resPool)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, true)
		}
	}
}

func assertFailedAdmission(s *ResPoolSuite, resPool *resPool, controller bool) {
	// gang resources shouldn't account for respool allocation
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.TotalAllocation))
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.ControllerAllocation))
	if controller {
		// gang should be moved to controller queue
		s.Equal(0, resPool.pendingQueue.Size())
		s.Equal(1, resPool.controllerQueue.Size())
	} else {
		// gang should remain in pending queue
		s.Equal(1, resPool.pendingQueue.Size())
	}
}

func assertAdmittedSuccessfully(s *ResPoolSuite, task *resmgr.Task, resPool *resPool) {
	// gang resource should account for respool's total allocation
	s.Equal(scalar.GetTaskAllocation(task).GetByType(scalar.TotalAllocation),
		resPool.allocation.GetByType(scalar.TotalAllocation))
	// gang resource should account for respool's controller allocation
	s.Equal(scalar.GetTaskAllocation(task).GetByType(scalar.ControllerAllocation),
		resPool.allocation.GetByType(scalar.ControllerAllocation))
	// gang resource should be removed from pending queue
	s.Equal(0, resPool.pendingQueue.Size())
	s.Equal(0, resPool.controllerQueue.Size())
}
