package respool

import (
	"code.uber.internal/infra/peloton/resmgr/scalar"
)

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
	ok, err = pending.TryAdmit(gang, resPool)
	s.Equal(err, errGangInvalid)

	// admission should fail because of validation
	s.False(ok)

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
	s.Equal(float64(0), resPool.GetAllocation().CPU)
	s.Equal(float64(0), resPool.GetAllocation().MEMORY)
	s.Equal(float64(0), resPool.GetAllocation().DISK)
	s.Equal(float64(0), resPool.GetAllocation().GPU)

	ok, err = pending.TryAdmit(gang, resPool)
	s.NoError(err)
	s.True(ok)
	s.Equal(0, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(1), resPool.GetAllocation().CPU)
	s.Equal(float64(100), resPool.GetAllocation().MEMORY)
	s.Equal(float64(10), resPool.GetAllocation().DISK)
	s.Equal(float64(0), resPool.GetAllocation().GPU)
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
	s.Equal(float64(0), resPool.GetAllocation().CPU)
	s.Equal(float64(0), resPool.GetAllocation().MEMORY)
	s.Equal(float64(0), resPool.GetAllocation().DISK)
	s.Equal(float64(0), resPool.GetAllocation().GPU)

	ok, err = pending.TryAdmit(gang, resPool)
	s.NoError(err)
	s.False(ok)
	s.Equal(1, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(0), resPool.GetAllocation().CPU)
	s.Equal(float64(0), resPool.GetAllocation().MEMORY)
	s.Equal(float64(0), resPool.GetAllocation().DISK)
	s.Equal(float64(0), resPool.GetAllocation().GPU)
}
