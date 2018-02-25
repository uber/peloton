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
	respool := s.createTestResourcePool()
	resPool, ok := respool.(*resPool)
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
	respool := s.createTestResourcePool()
	resPool, ok := respool.(*resPool)
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
	respool := s.createTestResourcePool()
	resPool, ok := respool.(*resPool)
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
		canAdmit bool
		tt       resmgr.TaskType
		err      error
	}{
		{ // Tests a batch task at the head of queue which can be admitted
			canAdmit: true,
			tt:       resmgr.TaskType_BATCH,
			err:      nil,
		},
		{ // Tests a batch task at the head of queue which can not be admitted
			canAdmit: false,
			tt:       resmgr.TaskType_BATCH,
			err:      errResourcePoolFull,
		},
		{ // Tests a controller task at the head of queue which can be admitted
			canAdmit: true,
			tt:       resmgr.TaskType_CONTROLLER,
			err:      nil,
		},
		{ // Tests a batch task at the head of queue which can nit be admitted
			canAdmit: false,
			tt:       resmgr.TaskType_CONTROLLER,
			err:      errControllerTask,
		},
	}

	for _, t := range tt {
		rp := s.respoolWithConfig(poolConfig)
		resPool, ok := rp.(*resPool)
		s.True(ok)
		if t.canAdmit {
			resPool.SetEntitlement(s.getEntitlement())
		} else if t.tt == resmgr.TaskType_CONTROLLER {
			// set the limit to zero
			resPool.controllerLimit = scalar.ZeroResource
		}

		task := s.getTasks()[0]
		task.Type = t.tt
		gang := resPool.MakeTaskGang(task)
		err := resPool.EnqueueGang(gang)
		s.NoError(err)

		err = pending.TryAdmit(gang, resPool)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, t.tt)
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
		tt       resmgr.TaskType
		err      error
	}{
		{ // Tests a controller task at the head of queue which can be admitted
			canAdmit: true,
			tt:       resmgr.TaskType_CONTROLLER,
			err:      nil,
		},
		{ // Tests a batch task at the head of queue which can nit be admitted
			canAdmit: false,
			tt:       resmgr.TaskType_CONTROLLER,
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
		task.Type = t.tt
		gang := resPool.MakeTaskGang(task)
		err := resPool.controllerQueue.Enqueue(gang)
		s.NoError(err)

		err = controller.TryAdmit(gang, resPool)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, t.tt)
		}
	}
}

func assertFailedAdmission(s *ResPoolSuite, resPool *resPool, tt resmgr.TaskType) {
	// gang resources shouldn't account for respool allocation
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.TotalAllocation))
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.ControllerAllocation))
	if tt == resmgr.TaskType_BATCH {
		// gang should remain in pending queue
		s.Equal(1, resPool.pendingQueue.Size())
	}
	if tt == resmgr.TaskType_CONTROLLER {
		// gang should be moved to controller queue
		s.Equal(0, resPool.pendingQueue.Size())
		s.Equal(1, resPool.controllerQueue.Size())
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
