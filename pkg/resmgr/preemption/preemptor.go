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

package preemption

import (
	"reflect"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/statemachine"
	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	"github.com/uber/peloton/pkg/resmgr/task"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
)

// represents the max size of the preemption queue
const maxPreemptionQueueSize = 10000

// Queue exposes APIs to interact with the preemption queue.
type Queue interface {
	// DequeueTask dequeues the RUNNING tasks from the preemption queue.
	// These tasks are then picked up by the jobmanager to be preempted.
	DequeueTask(maxWaitTime time.Duration) (*resmgr.PreemptionCandidate, error)
	// EnqueueTasks enqueues tasks either into the preemption queue for
	// RUNNING tasks or to the pending queue for NON_RUNNING tasks.
	// This API can be used by a caller when making a the decision to
	// preempt certain tasks outside of the preemptor.
	// This can include cases where a host is being taken down for maintenance.
	EnqueueTasks(tasks []*task.RMTask, event resmgr.PreemptionReason) error
}

// Preemptor preempts tasks based on either resource pool allocation or
// external sources eg host maintenance.
type Preemptor struct {
	// lifecycle manager
	lifeCycle lifecycle.LifeCycle

	// preemption can be enabled per cluster
	enabled bool
	// Defines how often does the preemptor run
	preemptionPeriod time.Duration
	// the number of consecutive cycles after which the resource pool is
	// considered for preemption,
	// eg if set to 3 then for 3 consecutive cycles the resource pool's
	// allocation should be greater than it entitlement.
	sustainedOverAllocationCount int

	// The resource pool tree
	resTree respool.Tree
	// The map of respool-id -> over allocation count
	respoolState map[string]int

	// The set of task-ids of the tasks in the preemption queue
	taskSet stringset.StringSet

	// The queue of tasks to be preempted
	preemptionQueue queue.Queue

	// the ranker ranks the tasks in the resource pool to be preempted
	ranker ranker
	// The task tracker
	tracker task.Tracker

	// The metrics scope
	scope tally.Scope
	// lazily populated map keyed by the resource pool ID
	m map[string]*Metrics
}

// NewPreemptor creates a new preemptor and returns it
func NewPreemptor(
	parent tally.Scope,
	cfg *common.PreemptionConfig,
	tracker task.Tracker,
	resTree respool.Tree,
) *Preemptor {

	return &Preemptor{
		lifeCycle:                    lifecycle.NewLifeCycle(),
		enabled:                      cfg.Enabled,
		preemptionPeriod:             cfg.TaskPreemptionPeriod,
		sustainedOverAllocationCount: cfg.SustainedOverAllocationCount,
		resTree:                      resTree,
		respoolState:                 make(map[string]int),
		taskSet:                      stringset.New(),
		preemptionQueue: queue.NewQueue(
			"preemption-queue",
			reflect.TypeOf(resmgr.PreemptionCandidate{}),
			maxPreemptionQueueSize,
		),
		ranker:  newStatePriorityRuntimeRanker(tracker),
		tracker: tracker,
		scope:   parent.SubScope("preemption"),
		m:       make(map[string]*Metrics),
	}
}

// returns per resource pool tagged metrics
func (p *Preemptor) metrics(pool respool.ResPool) *Metrics {
	metric, ok := p.m[pool.ID()]
	if !ok {
		metric = NewMetrics(p.scope.Tagged(map[string]string{
			"path": pool.GetPath(),
		}))
		p.m[pool.ID()] = metric
	}
	return metric
}

// Start starts Task Preemptor process
func (p *Preemptor) Start() error {
	if !p.enabled {
		log.Infof("Task Preemptor is not enabled to run")
		return nil
	}

	if p.lifeCycle.Start() {
		go func() {
			defer p.lifeCycle.StopComplete()

			ticker := time.NewTicker(p.preemptionPeriod)
			defer ticker.Stop()

			log.Info("Starting Task Preemptor")

			for {
				select {
				case <-p.lifeCycle.StopCh():
					log.Info("Exiting Task Preemptor")
					return
				case <-ticker.C:
					err := p.preemptOnce()
					if err != nil {
						log.WithError(err).Warn("Preemption cycle failed")
					}
				}
			}
		}()
	}
	return nil
}

// Stop stops Task Preemptor process
func (p *Preemptor) Stop() error {
	if !p.lifeCycle.Stop() {
		log.Warn("Task Preemptor is already stopped, no action will be performed")
		return nil
	}
	log.Info("Stopping Task Preemptor")

	// Wait for task Preemptor to be stopped
	p.lifeCycle.Wait()
	log.Info("Task Preemptor Stopped")
	return nil
}

// DequeueTask dequeues a running task from the preemption queue
func (p *Preemptor) DequeueTask(maxWaitTime time.Duration) (
	*resmgr.PreemptionCandidate, error) {
	item, err := p.preemptionQueue.Dequeue(maxWaitTime)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so we log the error
			log.WithError(err).
				Error("unable to dequeue task from preemption queue")
		}
		return nil, err
	}
	taskID := item.(*resmgr.PreemptionCandidate)
	// Remove task from taskSet
	p.taskSet.Remove(taskID.GetTaskId().GetValue())
	return taskID, nil
}

// EnqueueTasks enqueues tasks to be preempted
func (p *Preemptor) EnqueueTasks(
	tasks []*task.RMTask,
	reason resmgr.PreemptionReason,
) error {
	return p.processTasks(tasks, reason)
}

func (p *Preemptor) preemptOnce() error {
	// collect resource allocation from all resource pools
	p.updateResourcePoolsState()

	var combinedErr error
	// go through the resource pools which need preemption
	for _, respoolID := range p.getEligibleResPools() {
		err := p.processResourcePool(respoolID)
		if err != nil {
			combinedErr = multierr.Append(combinedErr,
				errors.Wrapf(err, "unable to preempt tasks from "+
					"resource pool :%s", respoolID))
		}
	}
	return combinedErr
}

// returns those resource pools which are eligible for preemption
func (p *Preemptor) getEligibleResPools() (resPools []string) {
	for respoolID, count := range p.respoolState {
		if count >= p.sustainedOverAllocationCount {
			resPools = append(resPools, respoolID)
		}
	}
	log.WithField("pools", resPools).Info(
		"Eligible resource pools for preemption")
	return resPools
}

// Loop through all the leaf nodes and set the count to the number consecutive of times
// the  allocation > entitlement; reset to zero otherwise
func (p *Preemptor) updateResourcePoolsState() {
	nodes := p.resTree.GetAllNodes(true)
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		resourcesAboveEntitlement := n.GetNonSlackAllocatedResources().Subtract(
			n.GetNonSlackEntitlement())

		slackResourcesAboveEntitlement := n.GetSlackAllocatedResources().Subtract(
			n.GetSlackEntitlement())

		count := 0
		if !scalar.ZeroResource.Equal(resourcesAboveEntitlement) ||
			!scalar.ZeroResource.Equal(slackResourcesAboveEntitlement) {
			// increment the count
			count = p.respoolState[n.ID()]
			count++
		}
		p.respoolState[n.ID()] = count
		p.metrics(n).OverAllocationCount.Update(float64(count))
	}
}

// processResourcePool takes a resource pool ID and performs actions
// on the tasks based on their current state
func (p *Preemptor) processResourcePool(respoolID string) error {
	resourcePool, err := p.resTree.Get(&peloton.ResourcePoolID{Value: respoolID})
	if err != nil {
		return errors.Wrap(err, "unable to get resource pool")
	}

	// Get resources to free from non-revocable tasks
	nonSlackResourcesToFree := resourcePool.GetNonSlackAllocatedResources().
		Subtract(resourcePool.GetNonSlackEntitlement())

	// Get resources to free from revocable tasks
	slackResourcesToFree := resourcePool.GetSlackAllocatedResources().
		Subtract(resourcePool.GetSlackEntitlement())

	log.WithFields(log.Fields{
		"respool_id":                      respoolID,
		"non_slack_resources_allocation":  resourcePool.GetNonSlackAllocatedResources().String(),
		"non_slack_resources_entitlement": resourcePool.GetNonSlackEntitlement().String(),
		"non_slack_resources_to_free":     nonSlackResourcesToFree.String(),
		"slack_resources_to_free":         slackResourcesToFree.String(),
		"slack_resources_allocation":      resourcePool.GetSlackAllocatedResources().String(),
		"slack_resources_entitlement":     resourcePool.GetSlackEntitlement().String(),
	}).Info("Resource to free from resource pool")

	p.metrics(resourcePool).NonSlackTotalResourcesToFree.Inc(nonSlackResourcesToFree)
	p.metrics(resourcePool).SlackTotalResourcesToFree.Inc(slackResourcesToFree)

	tasks := p.ranker.GetTasksToEvict(
		respoolID,
		slackResourcesToFree,
		nonSlackResourcesToFree)

	p.metrics(resourcePool).TasksToEvict.Update(float64(len(tasks)))
	if len(tasks) > 0 {
		var taskIDs []string
		for _, task := range tasks {
			taskIDs = append(taskIDs, task.Task().GetTaskId().GetValue())
		}

		log.WithFields(log.Fields{
			"respool_id":                  respoolID,
			"tasks_to_evict_len":          len(tasks),
			"tasks_to_evict":              taskIDs,
			"non_slack_resources_to_free": nonSlackResourcesToFree.String(),
			"slack_resources_to_free":     slackResourcesToFree.String(),
		}).Info("Resources to free and tasks to evict")
	}

	// we've processed the pool
	p.markProcessed(respoolID)
	return p.processTasks(
		tasks,
		resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
	)
}

// processes the tasks for preemption with the specified reason
func (p *Preemptor) processTasks(
	tasks []*task.RMTask,
	reason resmgr.PreemptionReason) error {

	var errs error
	for _, t := range tasks {
		state := t.GetCurrentState().State
		switch state {
		case peloton_task.TaskState_RUNNING:
			err := p.processRunningTask(t, reason)
			if err != nil {
				errs = multierr.Append(
					errs,
					errors.Wrapf(err,
						"failed to process running task:%s",
						t.Task().GetTaskId().GetValue()))
			}
		default:
			// For all non running tasks
			err := p.processNonRunningTask(t, reason)
			if err != nil {
				errs = multierr.Append(
					errs, errors.Wrapf(err,
						"failed to process non-running task:%s with "+
							"state:%s",
						t.Task().GetTaskId().GetValue(), state.String()))
			}
		}
	}
	return errs
}

func (p *Preemptor) processRunningTask(
	t *task.RMTask,
	reason resmgr.PreemptionReason) error {

	taskID := t.Task().GetTaskId()
	// Do not add to preemption queue if it already has an entry for this
	// Peloton task
	if p.taskSet.Contains(taskID.GetValue()) {
		log.
			WithField("task_id", taskID.GetValue()).
			Debug("Skipping enqueue. Task already present in " +
				"preemption queue.")
		return nil
	}

	log.
		WithField("task_id", taskID.GetValue()).
		Debug("Adding task to preemption queue")
	preemptionCandidate := &resmgr.PreemptionCandidate{
		Id:     t.Task().Id,
		TaskId: taskID,
		Reason: reason,
	}

	// Add to preemption queue
	err := p.preemptionQueue.Enqueue(preemptionCandidate)
	if err != nil {
		return errors.Wrapf(err, "unable to add task to "+
			"preemption queue task ID:%s",
			taskID.GetValue())
	}

	// Adding task to taskSet to dedupe adding the same task to the queue.
	// There could be cases where preemption is taking longer than usual
	// so we don't want to add the same task in the next preemption cycle.
	p.taskSet.Add(preemptionCandidate.GetTaskId().GetValue())

	// ToDo: ResourcesFreed are speculated to get free if preemption
	// runs uninterrupted. Fix it to track that running tasks reached
	// terminal state and then emit metrics.
	resourcesFreed := scalar.ConvertToResmgrResource(t.Task().Resource)
	if t.Task().GetRevocable() {
		p.metrics(t.Respool()).RevocableRunningTasksToPreempt.Inc(1)
		p.metrics(t.Respool()).SlackRunningTasksResourcesToFreed.Inc(resourcesFreed)
	} else {
		p.metrics(t.Respool()).NonRevocableRunningTasksToPreempt.Inc(1)
		p.metrics(t.Respool()).NonSlackRunningTasksResourcesToFreed.Inc(resourcesFreed)
	}

	p.metrics(t.Respool()).PreemptionQueueSize.Update(
		float64(p.preemptionQueue.Length()))
	log.WithField("task_id", taskID.GetValue()).
		Info("Adding running task to preemption queue")

	return nil
}

// 1) Moves the state to PENDING state
// 2) Moves the task back to the pending queue
// 3) Subtract the task resources from the resource pool allocation
// The task should be scheduled again at a later time.
func (p *Preemptor) processNonRunningTask(
	rmTask *task.RMTask,
	reason resmgr.PreemptionReason) error {
	t := rmTask.Task()
	resPool := rmTask.Respool()

	log.WithFields(log.Fields{
		"respool_id": resPool.ID(),
		"task_id":    t.GetTaskId().GetValue(),
		"state":      rmTask.GetCurrentState(),
	}).Debug("Evicting non-running task from resource pool")

	// Transit task to PENDING
	if err := rmTask.TransitTo(peloton_task.TaskState_PENDING.String(),
		statemachine.WithReason(reason.String())); err != nil {
		log.WithFields(log.Fields{
			"respool_id": resPool.ID(),
			"task_id":    t.GetTaskId().GetValue(),
			"state":      rmTask.GetCurrentState(),
		}).Debug("Unable to transit non-running task to PENDING for preemption")
		return nil
	}

	// Enqueue task to the resource pool
	// A new gang is created for each task
	if err := resPool.EnqueueGang(&resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			t,
		},
	}); err != nil {
		return errors.Wrapf(err, "unable to enqueue gang to resource pool")
	}

	// Subtract the task resources from the resource pool allocation
	err := resPool.SubtractFromAllocation(scalar.GetTaskAllocation(t))
	if err != nil {
		return errors.Wrapf(err, "unable to subtract allocation from "+
			"resource pool")
	}

	resourcesFreed := scalar.ConvertToResmgrResource(rmTask.Task().Resource)
	if rmTask.Task().GetRevocable() {
		p.metrics(resPool).RevocableNonRunningTasksToPreempt.Inc(1)
		p.metrics(resPool).SlackNonRunningTasksResourcesFreed.Inc(resourcesFreed)
	} else {
		p.metrics(resPool).NonRevocableNonRunningTasksToPreempt.Inc(1)
		p.metrics(resPool).NonSlackNonRunningTasksResourcesFreed.Inc(resourcesFreed)
	}

	log.WithFields(log.Fields{
		"respool_id": resPool.ID(),
		"task_id":    t.GetTaskId().GetValue(),
		"state":      rmTask.GetCurrentState(),
		"revocable":  t.GetRevocable(),
	}).Info("Evicted non-running task from resource pool")

	return nil
}

// Resets the state of the resource pool
func (p *Preemptor) markProcessed(respoolID string) {
	p.respoolState[respoolID] = 0
}
