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

package placement

import (
	"context"
	"errors"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/placement/plugins"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/hosts"
	tally_metrics "github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/offers"
	"github.com/uber/peloton/pkg/placement/reserver"
	"github.com/uber/peloton/pkg/placement/tasks"
)

const (
	// _noOffersTimeoutPenalty is the timeout value for a get offers request.
	_noOffersTimeoutPenalty = 1 * time.Second
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noTasksTimeoutPenalty = 1 * time.Second
	// error message for failed placed task
	_failedToPlaceTaskAfterTimeout = "failed to place task after timeout"
)

// Engine represents a placement engine that can be started and stopped.
type Engine interface {
	Start()
	Stop()
}

// New creates a new placement engine having one dedicated coordinator per task type.
func New(
	parent tally.Scope,
	cfg *config.PlacementConfig,
	offerService offers.Service,
	taskService tasks.Service,
	hostsService hosts.Service,
	strategy plugins.Strategy,
	pool *async.Pool) Engine {
	scope := tally_metrics.NewMetrics(
		parent.SubScope(strings.ToLower(cfg.TaskType.String())))

	engine := NewEngine(
		cfg,
		offerService,
		taskService,
		strategy,
		pool,
		scope,
		hostsService)

	return engine
}

// NewEngine creates a new placement engine.
func NewEngine(
	config *config.PlacementConfig,
	offerService offers.Service,
	taskService tasks.Service,
	strategy plugins.Strategy,
	pool *async.Pool,
	scope *tally_metrics.Metrics,
	hostsService hosts.Service) Engine {
	result := &engine{
		config:       config,
		offerService: offerService,
		taskService:  taskService,
		strategy:     strategy,
		pool:         pool,
		metrics:      scope,
		hostsService: hostsService,
	}
	result.daemon = async.NewDaemon("Placement Engine", result)
	result.reserver = reserver.NewReserver(scope, config, hostsService)
	return result
}

type engine struct {
	config           *config.PlacementConfig
	metrics          *tally_metrics.Metrics
	pool             *async.Pool
	offerService     offers.Service
	taskService      tasks.Service
	strategy         plugins.Strategy
	daemon           async.Daemon
	reservationQueue queue.Queue
	reserver         reserver.Reserver
	hostsService     hosts.Service
}

func (e *engine) Start() {
	e.daemon.Start()
	e.metrics.Running.Update(1)
}

func (e *engine) Run(ctx context.Context) error {
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
		delay := e.Place(ctx)
		timer.Reset(delay)
	}
}

func (e *engine) Stop() {
	e.daemon.Stop()
	e.metrics.Running.Update(0)
}

// Place will let the coordinator do one placement round.
// Returns the delay to start next round of placing.
func (e *engine) Place(ctx context.Context) time.Duration {
	// Try and get some tasks/assignments
	assignments := e.taskService.Dequeue(
		ctx,
		e.config.TaskType,
		e.config.TaskDequeueLimit,
		e.config.TaskDequeueTimeOut)

	if len(assignments) == 0 {
		return _noTasksTimeoutPenalty
	}

	// process revocable assignments
	e.processAssignments(
		ctx,
		assignments,
		func(assignment *models.Assignment) bool {
			return assignment.GetTask().GetTask().GetRevocable()
		})

	// process non-revocable assignments
	e.processAssignments(
		ctx,
		assignments,
		func(assignment *models.Assignment) bool {
			return !assignment.GetTask().GetTask().GetRevocable()
		})

	// We need to process the completed reservations
	err := e.processCompletedReservations(ctx)
	if err != nil {
		log.WithError(err).Info("error in processing completed reservations")
	}
	return time.Duration(0)
}

// processAssignments processes assignments by creating correct host filters and
// then finding host to place them on.
func (e *engine) processAssignments(
	ctx context.Context,
	assignments []*models.Assignment,
	f func(*models.Assignment) bool) {
	var result []*models.Assignment

	for _, assignment := range assignments {
		if f(assignment) {
			result = append(result, assignment)
		}
	}
	if len(result) == 0 {
		return
	}

	filters := e.strategy.Filters(result)
	for f, b := range filters {
		filter, batch := f, b
		// Run the placement of each batch in parallel
		e.pool.Enqueue(async.JobFunc(func(context.Context) {
			e.placeAssignmentGroup(ctx, filter, batch)
		}))
	}

	if !e.strategy.ConcurrencySafe() {
		// Wait for all batches to be processed
		e.pool.WaitUntilProcessed()
	}
}

// processCompletedReservations will be processing completed reservations
// and it will set the placements in resmgr
func (e *engine) processCompletedReservations(ctx context.Context) error {
	reservations, err := e.reserver.GetCompletedReservation(ctx)
	if err != nil {
		return err
	}
	if len(reservations) == 0 {
		return errors.New("no valid reservations")
	}

	placements := make([]*resmgr.Placement, len(reservations))
	for _, res := range reservations {
		selectedPorts := e.assignPorts(
			&models.HostOffers{Offer: res.HostOffers[0]},
			[]*models.Task{{Task: res.Task}})
		placement := &resmgr.Placement{
			Hostname: res.HostOffers[0].Hostname,
			Tasks:    []*peloton.TaskID{res.GetTask().GetId()},
			TaskIDs: []*resmgr.Placement_Task{
				{
					PelotonTaskID: res.GetTask().GetId(),
					MesosTaskID:   res.GetTask().GetTaskId(),
				},
			},
			Type:    e.config.TaskType,
			AgentId: res.HostOffers[0].AgentId,
			Ports:   selectedPorts,
		}
		placements = append(placements, placement)
	}

	e.taskService.SetPlacements(ctx, placements, nil)
	return nil
}

func (e *engine) placeAssignmentGroup(
	ctx context.Context,
	filter *hostsvc.HostFilter,
	assignments []*models.Assignment) {
	for len(assignments) > 0 {
		log.WithFields(log.Fields{
			"filter":          filter,
			"len_assignments": len(assignments),
			"assignments":     assignments,
		}).Info("placing assignment group")

		// Get hosts with available resources and tasks currently running.
		hosts, reason := e.offerService.Acquire(
			ctx,
			e.config.FetchOfferTasks,
			e.config.TaskType,
			filter)

		existing := e.findUsedHosts(assignments)
		now := time.Now()
		for !e.pastDeadline(now, assignments) && len(hosts)+len(existing) == 0 {
			time.Sleep(_noOffersTimeoutPenalty)
			hosts, reason = e.offerService.Acquire(
				ctx,
				e.config.FetchOfferTasks,
				e.config.TaskType,
				filter)
			now = time.Now()
		}

		// Add any hosts still assigned to any task so the offers will eventually be returned or used in a placement.
		hosts = append(hosts, existing...)

		// We were starved for hosts
		if len(hosts) == 0 {
			log.WithFields(log.Fields{
				"filter":      filter,
				"assignments": assignments,
			}).Info("failed to place tasks due to offer starvation")
			e.returnStarvedAssignments(ctx, assignments, reason)
			return
		}

		e.metrics.OfferGet.Inc(1)

		// PlaceOnce the tasks on the hosts by delegating to the placement strategy.
		e.strategy.PlaceOnce(assignments, hosts)

		// Filter the assignments according to if they got assigned,
		// should be retried or were unassigned.
		assigned, retryable, unassigned := e.filterAssignments(time.Now(), assignments)

		// We will retry the retryable tasks
		assignments = retryable

		log.WithFields(log.Fields{
			"filter":     filter,
			"assigned":   assigned,
			"retryable":  retryable,
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("Finshed one round placing assignment group")
		// Set placements and return unused offers and failed tasks
		e.cleanup(ctx, assigned, retryable, unassigned, hosts)
	}
}

// returns the starved assignments back to the task service
func (e *engine) returnStarvedAssignments(
	ctx context.Context,
	failedAssignments []*models.Assignment,
	reason string) {
	e.metrics.OfferStarved.Inc(1)
	// set the same reason for the failed assignments
	for _, a := range failedAssignments {
		a.Reason = reason
	}
	e.taskService.SetPlacements(ctx, nil, failedAssignments)
}

func (e *engine) assignPorts(offer *models.HostOffers, tasks []*models.Task) []uint32 {
	availablePortRanges := map[*models.PortRange]struct{}{}
	for _, resource := range offer.GetOffer().GetResources() {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			availablePortRanges[models.NewPortRange(portRange)] = struct{}{}
		}
	}
	var selectedPorts []uint32
	for _, taskEntity := range tasks {
		assignedPorts := uint32(0)
		neededPorts := taskEntity.GetTask().NumPorts
		depletedRanges := []*models.PortRange{}
		for portRange := range availablePortRanges {
			ports := portRange.TakePorts(neededPorts - assignedPorts)
			assignedPorts += uint32(len(ports))
			selectedPorts = append(selectedPorts, ports...)
			if portRange.NumPorts() == 0 {
				depletedRanges = append(depletedRanges, portRange)
			}
			if assignedPorts >= neededPorts {
				break
			}
		}
		for _, portRange := range depletedRanges {
			delete(availablePortRanges, portRange)
		}
	}
	return selectedPorts
}

// filters the assignments into three groups
// 1. assigned :  successful assignments.
// 2. retryable:  should be retried, either because we can find a
// 				  better host or we couldn't find a host.
// 3. unassigned: tried enough times, we couldn't find a host.
func (e *engine) filterAssignments(
	now time.Time,
	assignments []*models.Assignment) (
	assigned, retryable, unassigned []*models.Assignment) {
	var pending []*models.Assignment
	hostAssigned := make(map[string]struct{})
	for _, assignment := range assignments {
		task := assignment.GetTask()
		if assignment.GetHost() == nil {
			// we haven't found an assignment yet
			if task.PastDeadline(now) {
				// tried enough
				unassigned = append(unassigned, assignment)
				continue
			}
		} else {
			hostname := assignment.GetHost().GetOffer().GetHostname()
			// found a host
			task.IncRounds()
			// lets check if we can find a better one
			if e.isAssignmentGoodEnough(task, assignment.GetHost().GetOffer(), now) {
				// tried enough, this match is good enough
				assigned = append(assigned, assignment)
				hostAssigned[hostname] = struct{}{}
				continue
			}
		}
		// If we come here we have either
		// 1) found a host but we can try and find a better match
		// 2) we haven't found a host but we have time to find another one
		pending = append(pending, assignment)
	}

	for _, assignment := range pending {
		if assignment.GetHost() == nil {
			retryable = append(retryable, assignment)
		} else if _, ok := hostAssigned[assignment.GetHost().GetOffer().GetHostname()]; ok {
			if len(assignment.GetTask().GetTask().GetDesiredHost()) == 0 {
				// if the host is already used by one of the
				// assigned task, launch the task on that host.
				// Treating the assignment as retryable can be
				// problematic because the host would not be in PLACING
				// state, and if PE still decides to launch the task
				// on that host, an error would be returned
				assigned = append(assigned, assignment)
			} else {
				// for host not placed on the desired host, reset
				// host offers and add to retryable. Try to find
				// the desired host in the next rounds.
				assignment.HostOffers = nil
				retryable = append(retryable, assignment)
			}
		} else {
			retryable = append(retryable, assignment)
		}
	}

	return assigned, retryable, unassigned
}

// returns true if we have tried past max rounds or reached the deadline or
// the host is already placed on the desired host.
func (e *engine) isAssignmentGoodEnough(task *models.Task, offer *hostsvc.HostOffer, now time.Time) bool {
	if len(task.GetTask().GetDesiredHost()) == 0 {
		return task.PastMaxRounds() || task.PastDeadline(now)
	}

	// if a task has desired host, it would try to get placed on
	// the desired host until it passes desired host placement deadline
	return task.GetTask().GetDesiredHost() == offer.GetHostname() ||
		task.PastDesiredHostPlacementDeadline(now) ||
		task.PastDeadline(now)

}

// findUsedHosts will find the hosts that are used by the retryable assignments.
func (e *engine) findUsedHosts(
	retryable []*models.Assignment) []*models.HostOffers {
	var used []*models.HostOffers
	for _, assignment := range retryable {
		if offer := assignment.GetHost(); offer != nil {
			used = append(used, offer)
		}
	}
	return used
}

// findUnusedHosts will find the hosts that are unused by the assigned and retryable assignments.
func (e *engine) findUnusedHosts(
	assigned, retryable []*models.Assignment,
	hosts []*models.HostOffers) []*models.HostOffers {
	assignments := make([]*models.Assignment, 0, len(assigned)+len(retryable))
	assignments = append(assignments, assigned...)
	assignments = append(assignments, retryable...)

	// For each offer determine if any tasks where assigned to it.
	usedOffers := map[*models.HostOffers]struct{}{}
	for _, placement := range assignments {
		offer := placement.GetHost()
		if offer == nil {
			continue
		}
		usedOffers[offer] = struct{}{}
	}
	// Find the unused hosts
	unusedOffers := []*models.HostOffers{}
	for _, offer := range hosts {
		if _, used := usedOffers[offer]; !used {
			unusedOffers = append(unusedOffers, offer)
		}
	}
	return unusedOffers
}

func (e *engine) createPlacement(assigned []*models.Assignment) []*resmgr.Placement {
	createPlacementStart := time.Now()
	// For each offer find all tasks assigned to it.
	offersToTasks := map[*models.HostOffers][]*models.Task{}
	for _, placement := range assigned {
		task := placement.GetTask()
		offer := placement.GetHost()
		if offer == nil {
			continue
		}
		if _, exists := offersToTasks[offer]; !exists {
			offersToTasks[offer] = []*models.Task{}
		}
		offersToTasks[offer] = append(offersToTasks[offer], task)
	}

	// For each offer create a placement with all the tasks assigned to it.
	var resPlacements []*resmgr.Placement
	for offer, tasks := range offersToTasks {
		selectedPorts := e.assignPorts(offer, tasks)
		placement := &resmgr.Placement{
			Hostname:    offer.GetOffer().Hostname,
			AgentId:     offer.GetOffer().AgentId,
			Type:        e.config.TaskType,
			Tasks:       getTasks(tasks),
			TaskIDs:     getPlacementTasks(tasks),
			Ports:       selectedPorts,
			HostOfferID: offer.GetOffer().GetId(),
		}
		resPlacements = append(resPlacements, placement)
	}
	createPlacementDuration := time.Since(createPlacementStart)
	e.metrics.CreatePlacementDuration.Record(createPlacementDuration)
	return resPlacements
}

func (e *engine) cleanup(
	ctx context.Context,
	assigned, retryable,
	unassigned []*models.Assignment,
	offers []*models.HostOffers) {

	// Create the resource manager placements.
	e.taskService.SetPlacements(
		ctx,
		e.createPlacement(assigned),
		unassigned,
	)

	// Find the unused offers.
	unusedOffers := e.findUnusedHosts(assigned, retryable, offers)

	if len(unusedOffers) > 0 {
		// Release the unused offers.
		e.offerService.Release(ctx, unusedOffers)
	}
}

func (e *engine) pastDeadline(now time.Time, assignments []*models.Assignment) bool {
	for _, assignment := range assignments {
		if !assignment.GetTask().PastDeadline(now) {
			return false
		}
	}
	return true
}

func getTasks(tasks []*models.Task) []*peloton.TaskID {
	var taskIDs []*peloton.TaskID
	for _, task := range tasks {
		taskIDs = append(taskIDs, task.GetTask().GetId())
	}
	return taskIDs
}

func getPlacementTasks(tasks []*models.Task) []*resmgr.Placement_Task {
	var placementTasks []*resmgr.Placement_Task
	for _, task := range tasks {
		placementTasks = append(placementTasks, &resmgr.Placement_Task{
			PelotonTaskID: task.GetTask().GetId(),
			MesosTaskID:   task.GetTask().GetTaskId(),
		})
	}
	return placementTasks
}
