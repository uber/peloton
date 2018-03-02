package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	errGangInvalid          = errors.New("gang is invalid")
	errGangValidationFailed = errors.New("gang validation failed")
	errResourcePoolFull     = errors.New("resource pool full")
	errControllerTask       = errors.New("failed to admit controller task")
)

type queueType int

const (
	controllerQueue queueType = iota + 1
	pendingQueue
)

// returns true if the gang can be admitted to the pool
type admitter func(gang *resmgrsvc.Gang, pool *resPool) bool

//  returns true if there's enough resources in the pool to admit the gang
func checkEntitlement(gang *resmgrsvc.Gang,
	pool *resPool) bool {
	currentEntitlement := pool.entitlement
	currentAllocation := pool.allocation.GetByType(scalar.TotalAllocation)
	neededResources := scalar.GetGangResources(gang)

	log.WithField("entitlement", currentEntitlement).
		WithField("total_alloc", currentAllocation).
		WithField("resources_required", neededResources).Debug(
		"checking entitlement")

	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)

}

// returns true if a controller gang can be admitted to the pool
func checkControllerLimit(gang *resmgrsvc.Gang,
	pool *resPool) bool {
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
	controllerAllocation := pool.allocation.GetByType(scalar.ControllerAllocation)
	neededResources := scalar.GetGangResources(gang)

	log.WithField("controller_limit", pool.controllerLimit).
		WithField("controller_alloc", controllerAllocation).
		WithField("resources_required", neededResources).Debug(
		"checking controller limit")

	return controllerAllocation.
		Add(neededResources).
		LessThanOrEqual(pool.controllerLimit)
}

// admissionController is the interface for the admission controller for a
// resource pool
type admissionController interface {
	// Try admit, tries to admit the gang into the resource pool.
	// Returns an error if there was some error in the admission control
	TryAdmit(gang *resmgrsvc.Gang, pool *resPool) error
}

// batchAdmissionController is the implementation of admissionController for
// batch tasks
type batchAdmissionController struct {
	qt        queueType
	admitters []admitter
}

var (
	pending    = newPendingQueueAdmissionController()
	controller = newControllerQueueAdmissionController()
)

func newPendingQueueAdmissionController() admissionController {
	return batchAdmissionController{
		qt:        pendingQueue,
		admitters: []admitter{checkEntitlement, checkControllerLimit},
	}
}

func newControllerQueueAdmissionController() admissionController {
	return batchAdmissionController{
		qt:        controllerQueue,
		admitters: []admitter{checkEntitlement, checkControllerLimit},
	}
}

func (ac batchAdmissionController) TryAdmit(
	gang *resmgrsvc.Gang,
	pool *resPool) error {

	pool.Lock()
	defer pool.Unlock()

	ok, err := ac.validateGang(gang, pool)
	if err != nil {
		return errGangValidationFailed
	}

	// Gang is invalid
	if !ok {
		return errGangInvalid
	}

	if ok = ac.canAdmit(gang, pool); !ok {
		switch ac.qt {
		case pendingQueue:
			// If a gang can't be admitted from the pending queue to the resource
			// pool, then if
			// 1. its a controller task it is moved to the controller queue
			// 2. its a non-controller task do nothing
			if isController(gang) {
				err = pool.pendingQueue.Remove(gang)
				if err != nil {
					return errors.Wrapf(err,
						"failed to remove gang from pending queue")
				}

				err = pool.controllerQueue.Enqueue(gang)
				if err != nil {
					return errors.Wrapf(err,
						"failed to enqueue gang to controller queue")
				}
				return errControllerTask
			}
		}
		return errResourcePoolFull
	}

	// add to allocation and remove from source queue
	err = ac.getSourceQueue(pool).Remove(gang)
	if err != nil {
		return errors.Wrapf(err,
			"failed to remove gang from queue")
	}

	pool.allocation = pool.allocation.Add(scalar.GetGangAllocation(gang))
	return nil
}

// validateGang checks for all/some tasks been deleted. This function will delete that
// gang from the queue if all tasks are deleted or enqueue gang back with
// rest of the tasks in the gang.
// Returns true if the gang is valid and ready for admission control,
// false if the gang is invalid and an optional error if the validation failed.
func (ac batchAdmissionController) validateGang(gang *resmgrsvc.Gang, pool *resPool) (bool,
	error) {

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
	err := ac.getSourceQueue(pool).Remove(gang)
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
		err = ac.getSourceQueue(pool).Enqueue(gang)
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
	log.WithField("gang", gang).Info("All tasks are killed " +
		"for gang")
	return false, nil
}

// returns true if gang can be admitted to the pool
func (ac batchAdmissionController) canAdmit(gang *resmgrsvc.Gang,
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

// getSourceQueue returns the source queue depending on the type of the batch
// task
func (ac batchAdmissionController) getSourceQueue(pool *resPool) queue.Queue {
	if ac.qt == controllerQueue {
		return pool.controllerQueue
	}
	return pool.pendingQueue
}
