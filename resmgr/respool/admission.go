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
)

// admissionController is the interface for the admission controller for a
// resource pool
type admissionController interface {
	// Try admit, tries to admit the gang into the resource pool.
	// Returns a bool to indicate if the admission was successful or not and an
	// error if there was some error in the admission control
	TryAdmit(gang *resmgrsvc.Gang, pool *resPool) (bool, error)
}

// admissionHandler is the handler for the admission controller
type admissionHandler func(gang *resmgrsvc.Gang, pool *resPool) (bool, error)

// batchAdmissionController is the implementation of admissionController for
// batch tasks
type batchAdmissionController struct {
	tt        resmgr.TaskType
	onSuccess admissionHandler
	onFailure admissionHandler
}

func (ac batchAdmissionController) TryAdmit(
	gang *resmgrsvc.Gang,
	pool *resPool) (ok bool, err error) {

	ok, err = ac.validateGang(gang, pool)
	if err != nil {
		return false, errGangValidationFailed
	}

	// Gang is invalid
	if !ok {
		return false, errGangInvalid
	}

	// TODO avyas add admission control for controller tasks
	if ac.canAdmit(gang, pool) {
		return ac.onSuccess(gang, pool)
	}

	return ac.onFailure(gang, pool)
}

// validateGang checks for all/some tasks been deleted. This function will delete that
// gang from the queue if all tasks are deleted or enqueue gang back with
// rest of the tasks in the gang.
// Returns true if the gang is valid and ready for admission control,
// false if the gang is invalid and an optional error if the validation failed.
func (ac batchAdmissionController) validateGang(gang *resmgrsvc.Gang, pool *resPool) (bool,
	error) {

	pool.Lock()
	defer pool.Unlock()

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

func (ac batchAdmissionController) canAdmit(gang *resmgrsvc.Gang,
	pool *resPool) bool {
	pool.Lock()
	defer pool.Unlock()

	currentEntitlement := pool.entitlement
	currentAllocation := pool.allocation.GetByType(scalar.TotalAllocation)
	neededResources := scalar.GetGangResources(gang)

	log.WithField("entitlement", currentEntitlement).
		WithField("total_alloc", currentAllocation).Debug(
		"Current Resources")

	log.WithField("resources_required", neededResources).Debug(
		"Resources required")

	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)
}

var (
	controller = controllerQueueAdmitter()
	pending    = pendingQueueAdmitter()
)

// controllerQueueAdmitter performs admission of gangs from the controller
// queue of the resource pool
func controllerQueueAdmitter() admissionController {
	return batchAdmissionController{

		// If a gang can be admitted from the controller queue,
		// it is removed from the controller queue and the gangs resources are
		// added to the resource pool's controller allocation.
		onSuccess: func(gang *resmgrsvc.Gang, pool *resPool) (bool, error) {
			err := pool.controllerQueue.Remove(gang)
			if err != nil {
				return false, errors.Wrapf(err,
					"failed to remove from controller queue")
			}
			pool.Lock()
			// TODO avyas add to controller allocation
			pool.Unlock()

			return true, nil
		},

		// If a gang can't be admitted from the pending queue to the resource
		// pool then do nothing
		onFailure: func(gang *resmgrsvc.Gang, pool *resPool) (bool, error) {
			// no-op
			return false, nil
		},
		tt: resmgr.TaskType_CONTROLLER,
	}
}

// pendingQueueAdmitter performs admission of gangs from the pending queue of
// the resource pool
func pendingQueueAdmitter() admissionController {
	return batchAdmissionController{

		// If a gang can be admitted from the pending queue,
		// it is removed from the pending queue and the gangs resources are
		// added to the resource pool allocation.
		onSuccess: func(gang *resmgrsvc.Gang, pool *resPool) (bool, error) {
			err := pool.pendingQueue.Remove(gang)
			if err != nil {
				return false, errors.Wrapf(err,
					"failed to remove from pending queue")
			}
			pool.Lock()
			pool.allocation = pool.allocation.Add(scalar.GetGangAllocation(gang))
			pool.Unlock()

			return true, nil
		},

		// If a gang can't be admitted from the pending queue to the resource
		// pool, then if
		// 1. its a controller task it is moved to the controller queue
		// 2. its a non-controller task do nothing
		onFailure: func(gang *resmgrsvc.Gang, pool *resPool) (bool, error) {
			pool.Lock()
			defer pool.Unlock()

			//TODO avyas: add failure handler for controller task
			return false, nil
		},
		tt: resmgr.TaskType_BATCH,
	}
}

// getSourceQueue returns the source queue depending on the type of the batch
// task
func (ac batchAdmissionController) getSourceQueue(pool *resPool) queue.Queue {
	switch ac.tt {
	case resmgr.TaskType_CONTROLLER:
		return pool.controllerQueue
	default:
		return pool.pendingQueue
	}
}
