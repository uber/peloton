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

package reserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/hosts"
	tally_metrics "github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	models_v0 "github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/tasks"

	log "github.com/sirupsen/logrus"
)

const (
	// number of randomized hosts which will be choosen from
	// all the hosts
	_randomizedHosts = 10
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noHostsTimeoutPenalty = 1 * time.Second
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noTasksTimeoutPenalty     = 1 * time.Second
	_completedReservationQueue = "completed-ReservationQueue"
	// represents the max size of the preemption queue
	_maxReservationQueueSize = 10000
	// reservation queue name
	_reservationQueue = "reservation-queue"
)

var (
	errNoValidCompletedReservation = errors.New("no valid completed reservations found")
)

// Reserver represents a placement engine's reservation module
// It gets all the hosts based on filter passed to host manager
// it chooses the random host from the list and call reserve the
// chosen host based on the task.
type Reserver interface {
	// Adding daemon interface for Reserver
	async.Daemon

	// GetReservationQueue returns the reservation queue
	GetReservationQueue() queue.Queue

	// Reserve reserves the task to host in hostmanager
	Reserve(ctx context.Context) (time.Duration, error)

	// Places the assignments which are ready for host reservation into reservationQueue
	ProcessHostReservation(ctx context.Context, assignments []models.Task) error

	// EnqueueReservation enqueues the hostsvc.reservation to
	// the reservation queue
	EnqueueReservation(reservation *hostsvc.Reservation) error

	// GetCompletedReservation gets the completed tasks with offers
	// by that placement can be created
	GetCompletedReservation(ctx context.Context) ([]*hostsvc.CompletedReservation, error)
}

// reserver is the struct which implements Reserver interface
type reserver struct {
	lock sync.Mutex
	// Placement config for the reserver
	config *config.PlacementConfig
	// Placement engine metrics
	metrics *tally_metrics.Metrics
	// hostService for accessing the host manager for getting host list
	// as well as reserving host
	hostService hosts.Service
	// taskService for placing task on reserved host
	taskService tasks.Service
	// daemon object for making reserver a daemon process
	daemon async.Daemon
	// reservation queue of type resmgr.task which placement engine enqueues the tasks from
	// resourcemanager for reserver to make the reservation
	reservationQueue queue.Queue
	// completed reservation queue
	completedReservationQueue queue.Queue
	// task-> reservation mapping
	reservations map[string][]*models_v0.Host
	// tasks map indexed by taskID
	tasks map[string]*resmgr.Task
}

// NewReserver creates a new reserver which gets the tasks from the reservationQueue
// and based on the requirements from the task get the hosts list and randomly choose
// the host and make the reservation on that host for the task.
func NewReserver(
	metrics *tally_metrics.Metrics,
	cfg *config.PlacementConfig,
	hostsService hosts.Service,
	taskService tasks.Service) Reserver {
	reserver := &reserver{
		config:      cfg,
		hostService: hostsService,
		taskService: taskService,
		metrics:     metrics,
		reservationQueue: queue.NewQueue(
			_reservationQueue,
			reflect.TypeOf(hostsvc.Reservation{}),
			_maxReservationQueueSize,
		),
		completedReservationQueue: queue.NewQueue(
			_completedReservationQueue,
			reflect.TypeOf(hostsvc.CompletedReservation{}),
			_maxReservationQueueSize,
		),
		reservations: make(map[string][]*models_v0.Host),
		tasks:        make(map[string]*resmgr.Task),
	}
	reserver.daemon = async.NewDaemon("Placement Engine Reserver", reserver)

	return reserver
}

// Start method starts the daemon process
func (r *reserver) Start() {
	r.daemon.Start()
	r.metrics.Running.Update(1)
}

// Run method implements runnable from daemon
// this is the method which gets called while starting the
// daemon process.
func (r *reserver) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(0))
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		delay, err := r.Reserve(ctx)
		if err != nil {
			log.WithError(err).Info("tasks can't reserve hosts")
		}
		err = r.enqueueCompletedReservation(ctx)
		if err != nil {
			log.WithError(err).Debug("error finding completed reservation")
		}

		// We need to process the completed reservations
		err = r.processCompletedReservations(ctx)
		if err != nil {
			log.WithError(err).Info("error in processing completed reservations")
		}

		timer.Reset(delay)
	}
}

// Stop method will stop the daemon process.
func (r *reserver) Stop() {
	r.daemon.Stop()
	r.metrics.Running.Update(0)
}

// Reserve method is being called from Run method
// This method does following steps
//   1. Get Tasks from the reservation queue
//   2. Find the hosts list from hostmanager matching filter
//   3. choose one random host from the list
//   4. reserve the host in host manager
func (r *reserver) Reserve(ctx context.Context) (time.Duration, error) {
	// Get reservation from the reservation queue
	item, err := r.reservationQueue.Dequeue(1 * time.Second)
	if err != nil {
		log.Debug("No items in reservation queue")
		return _noTasksTimeoutPenalty, nil
	}
	reservation, ok := item.(*hostsvc.Reservation)
	if !ok || reservation.GetTask() == nil || reservation.GetTask().GetId() == nil {
		return _noTasksTimeoutPenalty, fmt.Errorf("not a valid task %s",
			reservation.GetTask())
	}
	// storing the tasks
	task := reservation.GetTask()
	r.tasks[task.GetId().Value] = task

	log.WithFields(log.Fields{
		"task": task.Id.Value,
	}).Debug("Reserving host for task")

	hostFilter := r.getHostFilter(task)
	// Find the hosts list from hostmanager matching filter
	hosts, err := r.hostService.GetHosts(ctx, task, hostFilter)
	if err != nil {
		log.WithFields(log.Fields{
			"host_filter": hostFilter,
			"task":        task.Id,
		}).Info("Couldn't acquire hosts for task")
		return _noHostsTimeoutPenalty, err
	}

	var hostToReserve []*models_v0.Host
	// choose one random host from the list
	hostToReserve = append(hostToReserve, r.findHost(hosts))
	// reserve the host in host manager
	if err := r.hostService.ReserveHost(ctx, hostToReserve, task); err != nil {
		log.WithFields(log.Fields{
			"host": hostToReserve[0].GetHost().Hostname,
			"task": task.Id.Value,
		}).Info("Host could not be reserved")
		return _noHostsTimeoutPenalty, err
	}
	//Updating the task to hosts map
	r.reservations[task.GetId().Value] = hostToReserve

	log.WithFields(log.Fields{
		"host": hostToReserve[0].GetHost().Hostname,
		"task": task.Id.Value,
	}).Info("Host reserved for task")

	return time.Duration(0), nil
}

// findHost randomly chooses the number of hosts and then
// out of those hosts choose the one which have lowest number
// of tasks running
func (r *reserver) findHost(hosts []*models_v0.Host) *models_v0.Host {
	lenHosts := len(hosts)
	lenRandomHosts := int(math.Min(
		float64(_randomizedHosts), float64(lenHosts)))

	randomHosts := make([]*models_v0.Host, lenRandomHosts)
	for i := 0; i < lenRandomHosts; i++ {
		randomHosts[i] = hosts[random(0, lenHosts)+0]
	}
	return r.findHostWithMinTasks(randomHosts)
}

// findHostWithMinTasks returns the host which has the minimun running task
// from the list of hosts provided
func (r *reserver) findHostWithMinTasks(hosts []*models_v0.Host) *models_v0.Host {
	min := taskLen(hosts[0])
	minIndex := 0
	for i, host := range hosts {
		if min >= taskLen(host) {
			min = taskLen(host)
			minIndex = i
		}
	}
	return hosts[minIndex]
}

func taskLen(host *models_v0.Host) int {
	if host.GetTasks() == nil {
		return 0
	}
	return len(host.GetTasks())
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func (r *reserver) getHostFilter(task *resmgr.Task) *hostsvc.HostFilter {
	result := &hostsvc.HostFilter{
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum:  task.Resource,
			NumPorts: task.NumPorts,
		},
	}
	if constraint := task.Constraint; constraint != nil {
		result.SchedulingConstraint = constraint
	}
	return result
}

// ProcessHostReservation places the assignments which are ready for host reservation
// into reservationQueue
func (r *reserver) ProcessHostReservation(
	ctx context.Context,
	assignments []models.Task) error {

	for _, assignment := range assignments {
		if assignment.IsReadyForHostReservation() {
			log.WithField("assignment", assignment).
				Debug("process host reservation")
			err := r.EnqueueReservation(&hostsvc.Reservation{
				Task: assignment.GetResmgrTaskV0(),
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetReservationQueue gets the reszervation queue
func (r *reserver) GetReservationQueue() queue.Queue {
	return r.reservationQueue
}

func (r *reserver) GetCompletedReservation(ctx context.Context,
) ([]*hostsvc.CompletedReservation, error) {
	var reservations []*hostsvc.CompletedReservation
	item, err := r.completedReservationQueue.Dequeue(100 * time.Millisecond)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so return
			return reservations, err
		}
		// we timed out, lets return
		return reservations, nil
	}

	res, ok := item.(*hostsvc.CompletedReservation)
	if !ok {
		// this should never happen
		return reservations, errors.New("invalid item in queue")
	}

	if res != nil {
		reservations = append(reservations, res)
	}

	return reservations, nil
}

// enqueueCompletedReservation gets out the completed reservations from
// hosts service and if any, enqueues them into completed reservation
// queue so that the handler can create placements out of them.
func (r *reserver) enqueueCompletedReservation(ctx context.Context) error {
	// Call hosts service to find the reservation
	reservations, err := r.hostService.GetCompletedReservation(ctx)
	if err != nil {
		return err
	}

	// Taking a lock here on reserver
	r.lock.Lock()
	defer r.lock.Unlock()
	// found the valid reservations
	for _, res := range reservations {
		// Check if reservation is succeeded or not
		// by looking at the offers length
		// if offers length is zero that means
		// we need to reserve
		if res.HostOffer == nil {
			err := r.reserveAgain(res.GetTask())
			if err != nil {
				log.WithError(err).
					Errorf("task %s could not be reserved, "+
						"dropping it ", res.GetTask().GetId().Value)
			}
			continue
		}
		// Valid completed reservation found
		err := r.completedReservationQueue.Enqueue(res)
		if err != nil {
			// Assuming placing timeout in hostmanager will
			// make the host available
			log.WithError(err).WithFields(log.Fields{
				"completed_reservation": res,
			}).Errorf("task %s could not be send for placement, "+
				"dropping it ", res.GetTask().GetId().Value)
			continue
		}
		// clean the completed reservation
		r.cleanReservation(res.GetTask())
	}
	return nil
}

// processCompletedReservations will be processing completed reservations
// and it will set the placements in resmgr
func (r *reserver) processCompletedReservations(ctx context.Context) error {
	reservations, err := r.GetCompletedReservation(ctx)
	if err != nil {
		return err
	}
	if len(reservations) == 0 {
		log.Debug("no valid reservations")
		return nil
	}

	now := time.Now()
	maxRounds := r.config.MaxRounds.Value(reservations[0].GetTask().Type)
	duration := r.config.MaxDurations.Value(reservations[0].GetTask().Type)
	deadline := now.Add(duration)
	desiredHostPlacementDeadline := now.Add(r.config.MaxDesiredHostPlacementDuration)

	assignments := make([]models.Task, len(reservations))
	for i, res := range reservations {
		task := models_v0.NewTask(nil,
			res.GetTask(),
			deadline,
			desiredHostPlacementDeadline,
			maxRounds,
		)
		assignments[i] = models_v0.NewAssignment(task)
		assignments[i].SetPlacement(&models_v0.HostOffers{Offer: res.HostOffer})
	}

	log.WithField("placements", assignments).
		Debug("Process completed reservations")
	r.taskService.SetPlacements(ctx, assignments, nil)
	return nil
}

func (r *reserver) reserveAgain(task *resmgr.Task) error {
	// cleaning the reservation
	r.cleanReservation(task)
	// enqueuing the task again for the reservation
	err := r.reservationQueue.Enqueue(
		&hostsvc.Reservation{
			Task: task,
		})
	if err != nil {
		return err
	}
	return nil
}

func (r *reserver) cleanReservation(task *resmgr.Task) {
	if task != nil {
		delete(r.reservations, task.GetId().Value)
		delete(r.tasks, task.GetId().Value)
	}
}

func (r *reserver) EnqueueReservation(reservation *hostsvc.Reservation) error {
	if reservation == nil {
		return errors.New("invalid reservation")
	}
	err := r.reservationQueue.Enqueue(reservation)
	if err != nil {
		return err
	}
	return nil
}
