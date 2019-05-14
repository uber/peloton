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
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/resmgr/scalar"

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
	errSkipRevocableGang = errors.New(
		"skipping revocable gang from admitting")
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

// returns true iff there's enough resources in the pool to admit the gang
func entitlementAdmitter(gang *resmgrsvc.Gang, pool *resPool) bool {
	var currentAllocation, currentEntitlement *scalar.Resources
	if !isRevocable(gang) {
		currentEntitlement = pool.nonSlackEntitlement
		currentAllocation = pool.allocation.GetByType(scalar.TotalAllocation).Subtract(
			pool.allocation.GetByType(scalar.SlackAllocation))
	} else {
		currentEntitlement = pool.slackEntitlement
		currentAllocation = pool.allocation.GetByType(scalar.SlackAllocation)
	}

	neededResources := scalar.GetGangResources(gang)
	log.WithFields(log.Fields{
		"respool_id":         pool.id,
		"entitlement":        currentEntitlement,
		"allocation":         currentAllocation,
		"resources_required": neededResources,
	}).Debug("checking entitlement")

	return currentAllocation.
		Add(neededResources).
		LessThanOrEqual(currentEntitlement)
}

// returns true if a controller gang can be admitted to the pool
func controllerAdmitter(gang *resmgrsvc.Gang, pool *resPool) bool {
	// ignore check on admission for non-controller tasks,
	// and revocable tasks (can not be of controller type)
	if !isController(gang) || isRevocable(gang) {
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
		"respool_id":         pool.id,
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
	if !pool.isPreemptionEnabled() ||
		isPreemptible(gang) ||
		isRevocable(gang) {
		// don't need to check reservation if
		// 1. preemption is disabled or
		// 2. its a preemptible job
		return true
	}

	npAllocation := pool.allocation.GetByType(scalar.NonPreemptibleAllocation)
	neededResources := scalar.GetGangResources(gang)
	reservation := pool.reservation

	log.WithFields(log.Fields{
		"respool_id":            pool.id,
		"reservation":           reservation,
		"non_preemptible_alloc": npAllocation,
		"resources_required":    neededResources,
	}).Debug("checking reservation")

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

// TryAdmit, tries to admit the gang into the resource pool.
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
			// 3. Its a revocable task it is moved to the revocable queue
			// 4. Else the resource pool is full, do nothing
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
			if isRevocable(gang) {
				err = ac.moveToQueue(pool, RevocableQueue, gang)
				if err != nil {
					return err
				}
				return errSkipRevocableGang
			}
		}
		return errResourcePoolFull
	}

	// gang is admittable,
	// 1. remove the gang from queue
	// 2. remove the demand for resource pool
	// 3. add gang resources to allocation
	if err := removeGangFromQueue(
		pool,
		qt,
		gang); err != nil {
		log.Error("failed to remove gang after successful admit")
		return err
	}

	pool.allocation = pool.allocation.Add(scalar.GetGangAllocation(gang))
	return nil
}

// moves the gang from the pending queue to
// one of (controller/np/revocable) queue
func (ac admissionController) moveToQueue(
	pool *resPool,
	qt QueueType,
	gang *resmgrsvc.Gang) error {

	// remove task from pending queue
	if err := removeGangFromQueue(
		pool,
		PendingQueue,
		gang); err != nil {
		log.Error("failed to remove from queue on move")
		return err
	}

	// adds removed gang from pending queue -> controller/np/revocable queue
	if err := addGangToQueue(
		pool,
		qt,
		gang); err != nil {
		log.Error("failed to add to queue on move")
		return err
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

	// list of tasks which are still valid inside the gang.
	// There could be cases where only some tasks are invalidated.
	// In such cases a new gang is created with the valid tasks and enqueued
	// back to the queue.
	var validTasks []*resmgr.Task
	isGangValid := true
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
	log.WithFields(log.Fields{
		"respool_name": pool.poolConfig.Name,
		"respool_id":   pool.id,
		"gang":         gang,
		"queue_type":   qt,
	}).Info("Tasks are invalidated for this gang")

	// Removing the gang from queue as tasks are deleted
	// from this gang
	if err := removeGangFromQueue(
		pool,
		qt,
		gang); err != nil {
		log.WithFields(log.Fields{
			"respool_name": pool.poolConfig.Name,
			"respool_id":   pool.id,
			"gang":         gang,
			"queue_type":   qt,
		}).WithError(err).Error("failed to remove from queue post validateGang")
		return false, err
	}

	// We need to enqueue the gang if all the tasks are not
	// deleted from the gang
	if len(validTasks) != 0 {
		gang.Tasks = validTasks
		// TODO this should enqueue at the head of the queue
		if err := addGangToQueue(
			pool,
			qt,
			gang); err != nil {
			log.WithFields(log.Fields{
				"respool_name": pool.poolConfig.Name,
				"respool_id":   pool.id,
				"gang":         gang,
				"queue_type":   qt,
			}).WithError(err).Error("failed to add gang (with valid tasks) " +
				"to queue post validateGang")
			return false, err
		}
	}

	// All the tasks in this gang are deleted
	log.WithField("gang", gang).Info("All tasks are killed for gang")
	return false, nil
}

// returns true if gang can be admitted to the pool
func (ac admissionController) canAdmit(
	gang *resmgrsvc.Gang,
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

// removeGangFromQueue removes a gang from a queue (pending/np/controller/revocable)
// and with that also reduces the demand for the respool
func removeGangFromQueue(
	pool *resPool,
	qt QueueType,
	gang *resmgrsvc.Gang) error {

	if err := pool.queue(qt).Remove(gang); err != nil {
		return err
	}

	if !isRevocable(gang) {
		pool.demand = pool.demand.Subtract(
			scalar.GetGangResources(gang))
	} else {
		pool.slackDemand = pool.slackDemand.Subtract(
			scalar.GetGangResources(gang))
	}

	return nil
}

// addGangToQueue adds a gang to a queue (pending/np/controller/revocable)
// and with that also add that gangs demand to the respool
func addGangToQueue(
	pool *resPool,
	qt QueueType,
	gang *resmgrsvc.Gang) error {

	if err := pool.queue(qt).Enqueue(gang); err != nil {
		return err
	}

	if !isRevocable(gang) {
		pool.demand = pool.demand.Add(
			scalar.GetGangResources(gang))
	} else {
		pool.slackDemand = pool.slackDemand.Add(
			scalar.GetGangResources(gang))
	}
	return nil
}

// returns true if the gang is of controller task
func isController(gang *resmgrsvc.Gang) bool {
	tasks := gang.GetTasks()

	if len(tasks) > 1 {
		return false
	}

	// controller gang has only 1 task
	return tasks[0].GetController()
}

// returns true if the gang is non-preemptible
func isPreemptible(gang *resmgrsvc.Gang) bool {
	tasks := gang.GetTasks()

	if len(tasks) == 0 {
		return false
	}

	// A gang is homogeneous w.r.t preemption policy so we can look at
	// just one task
	return tasks[0].Preemptible
}

// returns true iff the gang is revocable
func isRevocable(gang *resmgrsvc.Gang) bool {
	tasks := gang.GetTasks()

	if len(tasks) == 0 {
		return false
	}

	// Revocable is defined at Gang level,
	// all tasks in a gang are revocable or non-revocable.
	return tasks[0].GetRevocable()
}
