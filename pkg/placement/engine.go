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
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/hosts"
	tally_metrics "github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/offers"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v0"
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
	result.reserver = reserver.NewReserver(scope, config, hostsService, taskService)
	return result
}

type engine struct {
	config       *config.PlacementConfig
	metrics      *tally_metrics.Metrics
	pool         *async.Pool
	offerService offers.Service
	taskService  tasks.Service
	strategy     plugins.Strategy
	daemon       async.Daemon
	reserver     reserver.Reserver
	hostsService hosts.Service
}

func (e *engine) Start() {
	e.daemon.Start()
	e.reserver.Start()
	e.metrics.Running.Update(1)
}

func (e *engine) Run(ctx context.Context) error {
	log.WithField("dequeue_period", e.config.TaskDequeuePeriod.String()).
		WithField("dequeue_timeout", e.config.TaskDequeueTimeOut).
		WithField("dequeue_limit", e.config.TaskDequeueLimit).
		WithField("no_task_delay", _noTasksTimeoutPenalty).
		Info("Engine started")

	var unfulfilledAssignment []*models.Assignment
	var delay time.Duration
	timer := time.NewTimer(e.config.TaskDequeuePeriod)
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}

		unfulfilledAssignment, delay = e.Place(ctx, unfulfilledAssignment)
		log.WithField("delay", delay.String()).Debug("Placement delay")
		timer.Reset(delay)
	}
}

func (e *engine) Stop() {
	e.daemon.Stop()
	e.reserver.Stop()
	e.metrics.Running.Update(0)
}

// Place will let the coordinator do one placement round.
// It accepts unfulfilled assignment from last round, and
// try to process them in the current round.
// Returns the slice of assignment that cannot be fulfilled and
// the delay to start next round of placing.
func (e *engine) Place(
	ctx context.Context,
	lastRoundAssignment []*models.Assignment,
) ([]*models.Assignment, time.Duration) {
	log.Debug("Beginning placement cycle")

	// Try and get some tasks/assignments
	dequeLimit := e.config.TaskDequeueLimit - len(lastRoundAssignment)
	assignments := e.taskService.Dequeue(
		ctx,
		e.config.TaskType,
		dequeLimit,
		e.config.TaskDequeueTimeOut)

	if len(assignments)+len(lastRoundAssignment) == 0 {
		return nil, _noTasksTimeoutPenalty
	}

	// process host reservation assignments
	err := e.reserver.ProcessHostReservation(
		ctx,
		assignments,
	)
	if err != nil {
		log.WithError(err).Info("error in processing host reservations")
	}

	// add unfulfilledAssignment from last round and process
	// them in this round
	assignments = append(assignments, lastRoundAssignment...)

	// process revocable assignments
	unfulfilledAssignment := e.processAssignments(
		ctx,
		assignments,
		func(assignment *models.Assignment) bool {
			return assignment.IsRevocable() &&
				!assignment.IsReadyForHostReservation()
		})

	// process non-revocable assignments
	unfulfilledAssignment = append(
		unfulfilledAssignment,
		e.processAssignments(
			ctx,
			assignments,
			func(assignment *models.Assignment) bool {
				return !assignment.IsRevocable() &&
					!assignment.IsReadyForHostReservation()
			})...)

	// TODO: Dynamically adjust this based on some signal
	return unfulfilledAssignment, e.config.TaskDequeuePeriod
}

// processAssignments processes assignments by creating correct host filters and
// then finding host to place them on.
// It returns assignments that cannot be fulfilled.
func (e *engine) processAssignments(
	ctx context.Context,
	unassigned []*models.Assignment,
	f func(*models.Assignment) bool) []*models.Assignment {

	assignments := []*models.Assignment{}
	for _, assignment := range unassigned {
		if f(assignment) {
			assignments = append(assignments, assignment)
		}
	}
	if len(assignments) == 0 {
		return nil
	}

	unfulfilledAssignment := &concurrencySafeAssignmentSlice{}
	tasks := models.AssignmentsToTasks(assignments)
	tasksByNeeds := e.strategy.GroupTasksByPlacementNeeds(tasks)
	for _, group := range tasksByNeeds {
		batch := []*models.Assignment{}
		for _, idx := range group.Tasks {
			batch = append(batch, assignments[idx])
		}
		filter := v0_plugins.PlacementNeedsToHostFilter(group.PlacementNeeds)
		e.pool.Enqueue(async.JobFunc(func(context.Context) {
			unfulfilledAssignment.append(e.placeAssignmentGroup(ctx, filter, batch)...)
		}))
	}

	if !e.strategy.ConcurrencySafe() {
		// Wait for all batches to be processed
		e.pool.WaitUntilProcessed()
		return unfulfilledAssignment.get()
	}

	return unfulfilledAssignment.get()
}

// placeAssignmentGroup try to place the assignments,
// and return a slice of assignments that cannot be fulfilled,
// which need to be tried in the next round.
func (e *engine) placeAssignmentGroup(
	ctx context.Context,
	filter *hostsvc.HostFilter,
	assignments []*models.Assignment) []*models.Assignment {
	for len(assignments) > 0 {
		log.WithFields(log.Fields{
			"filter":          filter,
			"len_assignments": len(assignments),
			"assignments":     assignments,
		}).Info("placing assignment group")

		// Get hosts with available resources and tasks currently running.
		offers, reason := e.offerService.Acquire(
			ctx,
			e.config.FetchOfferTasks,
			e.config.TaskType,
			filter)

		existing := e.findUsedHosts(assignments)
		now := time.Now()
		for !e.pastDeadline(now, assignments) && len(offers)+len(existing) == 0 {
			time.Sleep(_noOffersTimeoutPenalty)
			offers, reason = e.offerService.Acquire(
				ctx,
				e.config.FetchOfferTasks,
				e.config.TaskType,
				filter)
			now = time.Now()
		}

		// Add any offers still assigned to any task so the offers will eventually be returned or used in a placement.
		offers = append(offers, existing...)

		// We were starved for offers
		if len(offers) == 0 {
			log.WithFields(log.Fields{
				"filter":      filter,
				"assignments": assignments,
			}).Info("failed to place tasks due to offer starvation")
			e.returnStarvedAssignments(ctx, assignments, reason)
			return nil
		}

		e.metrics.OfferGet.Inc(1)

		tasks := []plugins.Task{}
		for _, a := range assignments {
			tasks = append(tasks, a)
		}

		hosts := []plugins.Host{}
		for _, o := range offers {
			hosts = append(hosts, o)
		}

		// Delegate to the placement strategy to get the placements for these
		// tasks onto these offers.
		placements := e.strategy.GetTaskPlacements(tasks, hosts)
		for assignmentIdx, hostIdx := range placements {
			if hostIdx != -1 {
				assignments[assignmentIdx].SetHost(offers[hostIdx])
			}
		}

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
			"hosts":      offers,
		}).Debug("Finshed one round placing assignment group")
		// Set placements and return unused offers and failed tasks
		e.cleanup(ctx, assigned, retryable, unassigned, offers)

		if len(retryable) != 0 && e.shouldPlaceRetryableInNextRun(retryable) {
			log.WithFields(log.Fields{
				"retryable": retryable,
			}).Info("tasks are retried in the next run of placement")
			return retryable
		}
	}

	return nil
}

// returns if the retryable assignments should be retried in the run.
// Otherwise they would continue to be processed in the processAssignments loop.
func (e *engine) shouldPlaceRetryableInNextRun(retryable []*models.Assignment) bool {
	// if a strategy is concurrency safe there is no need to process retryable assignments
	// in the next run, because placement engine would not be blocked by any of the retryable
	// assignments.
	if e.strategy.ConcurrencySafe() {
		return false
	}

	// If all retryable assignment are:
	// 1. tasks which cannot find a host to run on, or
	// 2. tasks which have a desired host, but is not placed on the desired host
	// handle the retryable tasks in the next round of placeAssignmentGroup,
	// because these tasks may need to wait for a long time for the hosts
	// to be available, and should not block other assignments to be placed.
	count := 0
	for _, assignment := range retryable {
		if assignment.GetHost() == nil {
			count++
			continue
		}

		if len(assignment.GetTask().GetTask().GetDesiredHost()) != 0 &&
			assignment.GetTask().GetTask().GetDesiredHost() !=
				assignment.GetHost().GetOffer().GetHostname() {
			count++
			continue
		}
	}

	if count == len(retryable) {
		return true
	}

	return false
}

// returns the starved assignments back to the task service
func (e *engine) returnStarvedAssignments(
	ctx context.Context,
	failedAssignments []*models.Assignment,
	reason string) {
	e.metrics.OfferStarved.Inc(1)
	// set the same reason for the failed assignments
	for _, a := range failedAssignments {
		a.SetPlacementFailure(reason)
	}
	e.taskService.SetPlacements(ctx, nil, failedAssignments)
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
func (e *engine) isAssignmentGoodEnough(
	task *models.TaskV0,
	offer *hostsvc.HostOffer,
	now time.Time,
) bool {
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

func (e *engine) cleanup(
	ctx context.Context,
	assigned, retryable,
	unassigned []*models.Assignment,
	offers []*models.HostOffers) {

	// Create the resource manager placements.
	e.taskService.SetPlacements(
		ctx,
		assigned,
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

type concurrencySafeAssignmentSlice struct {
	sync.RWMutex
	slice []*models.Assignment
}

func (s *concurrencySafeAssignmentSlice) append(a ...*models.Assignment) {
	s.Lock()
	s.slice = append(s.slice, a...)
	s.Unlock()
}

func (s *concurrencySafeAssignmentSlice) get() []*models.Assignment {
	s.RLock()
	defer s.RUnlock()
	return s.slice
}
