package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
)

func (s *ResPoolSuite) respoolWithConfig(respoolConfig *respool.ResourcePoolConfig) ResPool {
	resPoolNode, err := NewRespool(tally.NoopScope, uuid.New(), s.root,
		respoolConfig, common.PreemptionConfig{
			Enabled: true})
	s.NoError(err)
	return resPoolNode
}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitValidationFail() {

	s.False(isRevocable(nil))
	s.False(isPreemptible(nil))

	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	task := s.getTasks()[0]
	gang := makeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	s.NoError(err)

	// check demand before
	s.Equal(float64(1), resPool.GetDemand().CPU)
	s.Equal(float64(100), resPool.GetDemand().MEMORY)
	s.Equal(float64(10), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)

	// mark the task is invalid
	resPool.AddInvalidTask(task.Id)

	// try and admit
	err = admission.TryAdmit(nil, resPool, PendingQueue)
	s.Equal(err, errGangInvalid)
	err = admission.TryAdmit(gang, resPool, PendingQueue)
	s.Equal(err, errGangInvalid)

	// demand should be removed
	s.Equal(float64(0), resPool.GetDemand().CPU)
	s.Equal(float64(0), resPool.GetDemand().MEMORY)
	s.Equal(float64(0), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)

	// Add a valid task and invalid task in a gang
	task = s.getTasks()[0]
	gang = makeTaskGang(task)

	task1 := s.getTasks()[1]
	gang.Tasks = append(gang.Tasks, task1)

	err = resPool.EnqueueGang(gang)
	s.NoError(err)

	// Demand of invalid + valid task
	s.Equal(float64(2), resPool.GetDemand().CPU)
	s.Equal(float64(200), resPool.GetDemand().MEMORY)
	s.Equal(float64(20), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)

	// mark one task as invalid
	resPool.AddInvalidTask(task.Id)
	resPool.SetEntitlement(s.getEntitlement())

	// gang will be re-enqueued with one task (removing invalid task)
	err = admission.TryAdmit(gang, resPool, PendingQueue)
	s.Equal(err, errGangInvalid)

	s.Equal(resPool.GetTotalAllocatedResources().String(), scalar.ZeroResource.String())

	// valid task is re-enqueued
	s.Equal(1, resPool.pendingQueue.Size())

	// demand is added back for valid task
	s.Equal(float64(1), resPool.GetDemand().CPU)
	s.Equal(float64(100), resPool.GetDemand().MEMORY)
	s.Equal(float64(10), resPool.GetDemand().DISK)
	s.Equal(float64(0), resPool.GetDemand().GPU)
}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitSuccess() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	resPool.SetEntitlement(s.getEntitlement())

	task := s.getTasks()[0]
	gang := makeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	s.NoError(err)

	// allocation should be zero
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)

	s.Equal(float64(1), resPool.GetDemand().GetCPU())
	s.Equal(float64(100), resPool.GetDemand().GetMem())
	s.Equal(float64(10), resPool.GetDemand().GetDisk())
	s.Equal(float64(0), resPool.GetDemand().GetGPU())

	err = admission.TryAdmit(gang, resPool, PendingQueue)
	s.NoError(err)
	s.Equal(0, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(1), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(100), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(10), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)

	s.Equal(float64(0), resPool.GetDemand().GetCPU())
	s.Equal(float64(0), resPool.GetDemand().GetMem())
	s.Equal(float64(0), resPool.GetDemand().GetDisk())
	s.Equal(float64(0), resPool.GetDemand().GetGPU())
}

func (s *ResPoolSuite) TestBatchAdmissionController_TryAdmitFailure() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	task := s.getTasks()[0]
	gang := makeTaskGang(task)

	err := resPool.EnqueueGang(gang)
	s.NoError(err)

	// allocation should be zero
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)

	err = admission.TryAdmit(gang, resPool, PendingQueue)
	s.Equal(err, errResourcePoolFull)
	s.Equal(1, resPool.pendingQueue.Size())

	// check allocation after
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().DISK)
	s.Equal(float64(0), resPool.GetTotalAllocatedResources().GPU)
}

// Test adds 9 revocable tasks and 2 non-revocable tasks.
// 8 revocable and 2 non-revocable tasks are admitted based,
// on their entitlement for the resource pool.
func (s *ResPoolSuite) TestServiceTypeAdmission() {
	pool := s.createTestResourcePool()
	resPool, ok := pool.(*resPool)
	s.True(ok)

	resPool.SetEntitlement(s.getEntitlement())
	resPool.SetSlackEntitlement(s.getSlackEntitlement())

	// validate entitlement
	s.Equal(float64(80), resPool.GetSlackEntitlement().CPU)

	s.Equal(float64(100), resPool.GetEntitlement().CPU)
	s.Equal(float64(1000), resPool.GetEntitlement().MEMORY)
	s.Equal(float64(100), resPool.GetEntitlement().DISK)

	for i := 0; i < 11; i++ {
		task := s.getRevocableTask()
		gang := makeTaskGang(task)

		if i == 1 || i == 3 {
			task = s.getTasks()[i]
			gang = makeTaskGang(task)
			err := resPool.EnqueueGang(gang)
			s.NoError(err)
			continue
		}

		err := resPool.EnqueueGang(gang)
		s.NoError(err)
	}

	// validate demand
	// 9 revocable tasks
	s.Equal(float64(90), resPool.GetSlackDemand().CPU)
	s.Equal(float64(90), resPool.GetSlackDemand().MEMORY)
	s.Equal(float64(18), resPool.GetSlackDemand().DISK)

	// 2 non-revocable tasks
	s.Equal(float64(2), resPool.GetDemand().CPU)
	s.Equal(float64(200), resPool.GetDemand().MEMORY)
	s.Equal(float64(20), resPool.GetDemand().DISK)

	gangs, err := resPool.DequeueGangs(100)
	s.NoError(err)
	s.Equal(10, len(gangs))
	s.Equal(resPool.revocableQueue.Size(), 1)

	gangs, err = resPool.revocableQueue.Peek(1)
	s.Equal(len(gangs), 1)
	s.Equal(len(gangs[0].Tasks), 1)

	task := gangs[0].Tasks[0]
	s.True(task.GetRevocable())

	// validate allocated resources to revocable and non-revocable
	// 8 revocable tasks admitted out of 9
	s.Equal(float64(80), resPool.GetSlackAllocatedResources().CPU)
	s.Equal(float64(80), resPool.GetSlackAllocatedResources().MEMORY)
	s.Equal(float64(16), resPool.GetSlackAllocatedResources().DISK)

	// 8 revocable + 2 non-revocable tasks admitted
	s.Equal(float64(82), resPool.GetTotalAllocatedResources().CPU)
	s.Equal(float64(280), resPool.GetTotalAllocatedResources().MEMORY)
	s.Equal(float64(36), resPool.GetTotalAllocatedResources().DISK)
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
		canAdmit    bool
		err         error
		controller  bool
		preemptible bool
	}{
		{
			// Tests a batch task at the head of queue which can be admitted
			canAdmit:    true,
			err:         nil,
			controller:  false,
			preemptible: true,
		},
		{
			// Tests a batch task at the head of queue which can not be admitted
			canAdmit:    false,
			err:         errResourcePoolFull,
			controller:  false,
			preemptible: true,
		},
		{
			// Tests a controller task at the head of queue which can be admitted
			canAdmit:    true,
			err:         nil,
			controller:  true,
			preemptible: true,
		},
		{
			// Tests a controller task at the head of queue which can not be
			// admitted
			canAdmit:    false,
			err:         errSkipControllerGang,
			controller:  true,
			preemptible: true,
		},
		{
			// Tests a non preemptible gang at the head of queue which can be
			// admitted
			canAdmit:    true,
			err:         nil,
			controller:  false,
			preemptible: false,
		},
		{
			// Tests a non preemptible task at the head of queue which can not be
			// admitted
			canAdmit:    false,
			err:         errSkipNonPreemptibleGang,
			controller:  false,
			preemptible: false,
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
		} else if !t.preemptible {
			// set the reservation to zero
			resPool.reservation = scalar.ZeroResource
		}

		task := s.getTasks()[0]
		task.Controller = t.controller
		task.Preemptible = t.preemptible
		gang := makeTaskGang(task)
		err := resPool.EnqueueGang(gang)
		s.NoError(err)

		err = admission.TryAdmit(gang, resPool, PendingQueue)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, t.controller, t.preemptible)
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
		gang := makeTaskGang(task)
		err := resPool.controllerQueue.Enqueue(gang)
		s.NoError(err)

		err = admission.TryAdmit(gang, resPool, ControllerQueue)
		s.Equal(t.err, err)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, true, true)
		}
	}
}

func (s *ResPoolSuite) TestBatchAdmissionController_NPAdmitter() {
	poolConfig := &respool.ResourcePoolConfig{
		Name:      _testResPoolName,
		Parent:    &_rootResPoolID,
		Resources: s.getResources(),
		Policy:    respool.SchedulingPolicy_PriorityFIFO,
	}

	tt := []struct {
		canAdmit          bool
		wantErr           error
		preemptionEnabled bool
	}{
		{
			// Tests a non preemptible task at the head of queue which can be
			// admitted when preemption is enabled
			canAdmit:          true,
			wantErr:           nil,
			preemptionEnabled: true,
		},
		{
			// Tests a non preemptible task at the head of queue which can not
			// be admitted when preemption is enabled
			canAdmit:          false,
			wantErr:           errResourcePoolFull,
			preemptionEnabled: true,
		},
		{
			// Tests a non preemptible task at the head of queue which can be
			// admitted when preemption is disabled
			canAdmit:          true,
			wantErr:           nil,
			preemptionEnabled: false,
		},
	}

	for _, t := range tt {
		rp := s.respoolWithConfig(poolConfig)
		resPool, ok := rp.(*resPool)
		s.True(ok)

		resPool.preemptionCfg.Enabled = t.preemptionEnabled

		if t.canAdmit {
			if !t.preemptionEnabled {
				// set the reservation to zero
				resPool.reservation = scalar.ZeroResource
			}
			resPool.SetEntitlement(s.getEntitlement())
		} else {
			// set the reservation to zero
			resPool.reservation = scalar.ZeroResource
		}

		task := s.getTasks()[3]
		task.Preemptible = false
		gang := makeTaskGang(task)
		err := resPool.npQueue.Enqueue(gang)
		s.NoError(err)

		err = admission.TryAdmit(gang, resPool, NonPreemptibleQueue)
		s.Equal(t.wantErr, err, "failed test case:%v", t)

		if t.canAdmit {
			assertAdmittedSuccessfully(s, task, resPool)
		} else {
			assertFailedAdmission(s, resPool, false, false)
		}
	}
}

func assertFailedAdmission(s *ResPoolSuite, resPool *resPool,
	controller bool, preemptible bool) {
	// gang resources shouldn't account for respool allocation
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.TotalAllocation))
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.ControllerAllocation))
	s.Equal(scalar.ZeroResource, resPool.allocation.GetByType(scalar.NonPreemptibleAllocation))
	if controller {
		// gang should be moved to controller queue
		s.Equal(0, resPool.pendingQueue.Size())
		s.Equal(1, resPool.controllerQueue.Size())
		s.Equal(0, resPool.npQueue.Size())
		return
	}

	if !preemptible {
		// gang should be moved to np queue
		s.Equal(0, resPool.pendingQueue.Size())
		s.Equal(0, resPool.controllerQueue.Size())
		s.Equal(1, resPool.npQueue.Size())
		return
	}

	// gang should remain in pending queue
	s.Equal(1, resPool.pendingQueue.Size())
	s.Equal(0, resPool.controllerQueue.Size())
	s.Equal(0, resPool.npQueue.Size())
}

func assertAdmittedSuccessfully(s *ResPoolSuite, task *resmgr.Task, resPool *resPool) {
	// gang resource should account for respool's total allocation
	s.Equal(scalar.GetTaskAllocation(task).GetByType(scalar.TotalAllocation),
		resPool.allocation.GetByType(scalar.TotalAllocation))
	// gang resource should account for respool's controller allocation
	s.Equal(scalar.GetTaskAllocation(task).GetByType(scalar.ControllerAllocation),
		resPool.allocation.GetByType(scalar.ControllerAllocation))
	// gang resource should account for respool's non preemptible allocation
	s.Equal(scalar.GetTaskAllocation(task).GetByType(scalar.NonPreemptibleAllocation),
		resPool.allocation.GetByType(scalar.NonPreemptibleAllocation))
	// gang resource should be removed from all the queues
	s.Equal(0, resPool.pendingQueue.Size())
	s.Equal(0, resPool.controllerQueue.Size())
	s.Equal(0, resPool.npQueue.Size())
}
