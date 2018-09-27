package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	errGangInvalid          = errors.New("gang is invalid")
	errGangValidationFailed = errors.New("gang validation failed")
	errResourcePoolFull     = errors.New("resource pool full")

	errSkipControllerGang = errors.New(
		"skipping controller gang from admitting")
	errSkipNonPreemptibleGang = errors.New(
		"skipping non-preemptible gang from admitting")
)

// QueueType defines the different queues of the resource pool from which
// the gangs are admitted.
type QueueType int

const (
	// PendingQueue is the default queue for all incoming gangs
	PendingQueue QueueType = iota + 1
	// ControllerQueue is the queue for controller gangs
	ControllerQueue
	// NonPreemptibleQueue is the queue for non preemptible gangs
	NonPreemptibleQueue
	// RevocableQueue is the queue for revocable gangs
	// which are scheduled using slack resources [cpus]
	RevocableQueue
)

// String returns the queue name
func (qt QueueType) String() string {
	switch qt {
	case NonPreemptibleQueue:
		return "non-preemptible"
	case PendingQueue:
		return "pending"
	case ControllerQueue:
		return "controller"
	case RevocableQueue:
		return "revocable"
	}

	// should never come here
	return "undefined"
}

// returns true if the gang can be admitted to the pool
type admitter func(gang *resmgrsvc.Gang, pool *resPool) bool

//  returns true if there's enough resources in the pool to admit the gang
func entitlementAdmitter(gang *resmgrsvc.Gang, pool *resPool) bool {
	currentEntitlement := pool.entitlement
	currentAllocation := pool.allocation.GetByType(scalar.TotalAllocation)
	neededResources := scalar.GetGangResources(gang)

	log.WithFields(log.Fields{
		"entitlement":        currentEntitlement,
		"total_alloc":        currentAllocation,
		"resources_required": neededResources,
	}).Debug("checking entitlement")

	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)

}

// returns true if a controller gang can be admitted to the pool
func controllerAdmitter(gang *resmgrsvc.Gang, pool *resPool) bool {
	if !isController(gang) {
		// don't need to check controller limit
		return true
	}

	if pool.controllerLimit == nil {
		log.WithField("respool_id", pool.id).
			Debug("resource pool doesn't have a controller limit")
		return true
	}

	// check controller limit and allocation
	controllerLimit := pool.controllerLimit
	controllerAllocation := pool.allocation.GetByType(scalar.ControllerAllocation)
	neededResources := scalar.GetGangResources(gang)

	log.WithFields(log.Fields{
		"controller_limit":   controllerLimit,
		"controller_alloc":   controllerAllocation,
		"resources_required": neededResources,
	}).Debug("checking controller limit")

	return controllerAllocation.
		Add(neededResources).
		LessThanOrEqual(controllerLimit)
}

// For admission of non preemptible gangs there are 2 approaches:
// 1. Non preemptible will not be admitted if allocation > reservation
//    i.e. non-preemptible gangs should not use elastic resources.
// 2. Lower priority gangs will not be admitted if
//    (higher priority allocation) > reservation
// Peloton takes approach 1 by checking the total allocation of all
// non-preemptible gangs and the resource pool reservation.
func reservationAdmitter(gang *resmgrsvc.Gang, pool *resPool) bool {
	if !pool.isPreemptionEnabled() || isPreemptible(gang) {
		// don't need to check reservation if
		// 1. preemption is disabled or
		// 2. its a preemptible job
		return true
	}

	npAllocation := pool.allocation.GetByType(scalar.NonPreemptibleAllocation)
	neededResources := scalar.GetGangResources(gang)
	reservation := pool.reservation

	log.WithFields(log.Fields{
		"reservation":           reservation,
		"non_preemptible_alloc": npAllocation,
		"resources_required":    neededResources,
	}).Info("checking reservation")

	return npAllocation.
		Add(neededResources).
		LessThanOrEqual(reservation)
}

type admissionController struct {
	admitters []admitter
}

// the global admission controller for all resource pool
var admission = admissionController{
	admitters: []admitter{
		entitlementAdmitter,
		controllerAdmitter,
		reservationAdmitter,
	},
}

// Try admit, tries to admit the gang into the resource pool.
// Returns an error if there was some error in the admission control
func (ac admissionController) TryAdmit(
	gang *resmgrsvc.Gang,
	pool *resPool,
	qt QueueType) error {

	pool.Lock()
	defer pool.Unlock()

	ok, err := ac.validateGang(gang, pool, qt)
	if err != nil {
		return errGangValidationFailed
	}

	// Gang is invalid
	if !ok {
		return errGangInvalid
	}

	if admitted := ac.canAdmit(gang, pool); !admitted {
		if qt == PendingQueue {
			// If a gang can't be admitted from the pending queue to the resource
			// pool, then if:
			// 1. Its a non-preemptible task it is moved to the non-preemptible
			//    queue
			// 2. Its a controller task it is moved to the controller queue
			// 3. Else the resource pool is full, do nothing
			if !isPreemptible(gang) && pool.isPreemptionEnabled() {
				// only move to non preemptible queue iff both are true
				// 1. its a non preemptible gang
				// 2. preemption is enabled
				err := ac.moveToQueue(pool, NonPreemptibleQueue, gang)
				if err != nil {
					return err
				}
				return errSkipNonPreemptibleGang
			}
			if isController(gang) {
				err = ac.moveToQueue(pool, ControllerQueue, gang)
				if err != nil {
					return err
				}
				return errSkipControllerGang
			}
		}
		return errResourcePoolFull
	}

	// add to allocation and remove from source queue
	err = pool.queue(qt).Remove(gang)
	if err != nil {
		return errors.Wrapf(err,
			"failed to remove gang from queue, qtype:%d", qt)
	}

	pool.allocation = pool.allocation.Add(scalar.GetGangAllocation(gang))
	return nil
}

// moves the gang from the pending queue to one of controller queue or np queue.
func (ac admissionController) moveToQueue(pool *resPool, qt QueueType,
	gang *resmgrsvc.Gang) error {
	err := pool.pendingQueue.Remove(gang)
	if err != nil {
		return errors.Wrap(err,
			"failed to remove gang from pending queue")
	}
	err = pool.queue(qt).Enqueue(gang)
	if err != nil {
		return errors.Wrapf(err,
			"failed to enqueue gang to queue, qtype:%d", qt)
	}
	return nil
}

// validateGang checks for all/some tasks been deleted. This function will delete that
// gang from the queue if all tasks are deleted or enqueue gang back with
// rest of the tasks in the gang.
// Returns true if the gang is valid and ready for admission control,
// false if the gang is invalid and an optional error if the validation failed.
func (ac admissionController) validateGang(
	gang *resmgrsvc.Gang,
	pool *resPool,
	qt QueueType) (bool, error) {

	// return false if gang is nil
	if gang == nil {
		return false, nil
	}

	isGangValid := true

	// list of tasks which are still valid inside the gang.
	// There could be cases where only some tasks are invalidated.
	// In such cases a new gang is created with the valid tasks and enqueued
	// back to the queue.
	var validTasks []*resmgr.Task

	for _, task := range gang.GetTasks() {
		// check if the task in the gang has been invalidated
		if _, exists := pool.invalidTasks[task.Id.Value]; !exists {
			validTasks = append(validTasks, task)
		} else {
			// the task is invalid so we mark the gang as invalid
			delete(pool.invalidTasks, task.Id.Value)
			isGangValid = false
		}
	}

	// Gang is validated so ready for admission control
	if isGangValid {
		return true, nil
	}

	// Now we perform the cleanup of the invalid gang and if required requeue
	// the gang with the valid tasks back to the queue.

	// remove it from the pending queue
	log.WithFields(log.Fields{
		"gang": gang,
	}).Info("Tasks are deleted for this gang")

	// Removing the gang from queue as tasks are deleted
	// from this gang
	err := pool.queue(qt).Remove(gang)
	if err != nil {
		log.WithError(err).Error("Failed to delete" +
			"gang from pending queue")
		return false, err
	}

	// We need to remove it from the demand
	for _, task := range gang.GetTasks() {
		// We have to remove demand as we removed task from
		// pending queue.
		pool.demand = pool.demand.Subtract(
			scalar.ConvertToResmgrResource(
				task.GetResource()))
	}

	// We need to enqueue the gang if all the tasks are not
	// deleted from the gang
	if len(validTasks) != 0 {
		gang.Tasks = validTasks
		// TODO this should enqueue at the head of the queue
		err = pool.queue(qt).Enqueue(gang)
		if err != nil {
			log.WithError(err).
				WithField("new_gang", gang).
				Error("Not able to enqueue gang to pending queue")
			return false, err
		}
		pool.demand = pool.demand.Add(scalar.GetGangResources(gang))
		return false, nil
	}

	// All the tasks in this gang is been deleted
	log.WithField("gang", gang).
		Info("All tasks are killed for gang")
	return false, nil
}

// returns true if gang can be admitted to the pool
func (ac admissionController) canAdmit(gang *resmgrsvc.Gang,
	pool *resPool) bool {

	// loop through the admitters
	for _, admitter := range ac.admitters {
		if !admitter(gang, pool) {
			// bail out fast
			return false
		}
	}
	// all admitters can admit
	return true
}

// returns true if the gang is of controller task
func isController(gang *resmgrsvc.Gang) bool {
	tasks := gang.GetTasks()

	if len(tasks) > 1 {
		return false
	}

	return tasks[0].GetController()
}

// returns true if the gang is non-preemptible
func isPreemptible(gang *resmgrsvc.Gang) bool {
	tasks := gang.GetTasks()

	if len(tasks) == 0 {
		return false
	}

	// Preemption is defined at the Job level, so we can check just one task
	return tasks[0].Preemptible
}
