package preemption

import (
	"reflect"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/common/stringset"
	"code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"code.uber.internal/infra/peloton/resmgr/task"

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
	// This API can be used by a caller whp is making a the decision to
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

	// The set of tasks in the preemption queue
	taskSet stringset.StringSet // Set containing tasks which are currently in the PreemptionQueue
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
) *Preemptor {

	return &Preemptor{
		lifeCycle:                    lifecycle.NewLifeCycle(),
		enabled:                      cfg.Enabled,
		preemptionPeriod:             cfg.TaskPreemptionPeriod,
		sustainedOverAllocationCount: cfg.SustainedOverAllocationCount,
		resTree:      respool.GetTree(),
		respoolState: make(map[string]int),
		taskSet:      stringset.New(),
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
	p.taskSet.Remove(taskID.GetId().GetValue())
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
		resourcesAboveEntitlement := n.GetTotalAllocatedResources().Subtract(
			n.GetEntitlement())
		count := 0
		if !scalar.ZeroResource.Equal(resourcesAboveEntitlement) {
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
	resourcesToFree := resourcePool.GetTotalAllocatedResources().Subtract(
		resourcePool.GetEntitlement())
	log.
		WithField("respool_id", respoolID).
		WithField("resource_to_free", resourcesToFree).
		Info("Resource to free from resource pool")

	tasks := p.ranker.GetTasksToEvict(respoolID, resourcesToFree)
	log.WithField("tasks", tasks).
		WithField("respool_id", respoolID).
		Debug("Tasks to evict from resource pool")

	// we've processed the pool
	p.markProcessed(respoolID)
	return p.processTasks(
		tasks,
		resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES,
	)
}

// processes the tasks for preemption with the specified reason
func (p *Preemptor) processTasks(
	tasks []*task.RMTask, reason resmgr.PreemptionReason) error {

	var errs error
	for _, t := range tasks {
		switch t.GetCurrentState() {
		case peloton_task.TaskState_RUNNING:
			err := p.processRunningTask(t, reason)
			if err != nil {
				errs = multierr.Append(
					errs,
					errors.Wrapf(err,
						"failed to process running task:%s",
						t.Task().GetId().Value))
			}
		default:
			// For all non running tasks
			err := p.processNonRunningTask(t, reason)
			if err != nil {
				errs = multierr.Append(
					errs, errors.Wrapf(err,
						"failed to process non-running task:%s with "+
							"state:%s",
						t.Task().GetId().Value, t.GetCurrentState().String()))
			}
		}
	}
	return errs
}

func (p *Preemptor) processRunningTask(
	t *task.RMTask, reason resmgr.PreemptionReason,
) error {
	// Do not add to preemption queue if it already has an entry for this
	// Peloton task
	if p.taskSet.Contains(t.Task().GetId().GetValue()) {
		log.
			WithField("task_id", t.Task().Id.Value).
			Debug("Skipping enqueue. Task already present in " +
				"preemption queue.")
		return nil
	}

	log.
		WithField("task_id", t.Task().Id.Value).
		Debug("Adding task to preemption queue")
	preemptionCandidate := &resmgr.PreemptionCandidate{
		Id:     t.Task().Id,
		Reason: reason,
	}

	// Add to preemption queue
	err := p.preemptionQueue.Enqueue(preemptionCandidate)
	if err != nil {
		return errors.Wrapf(err, "unable to add task to "+
			"preemption queue task ID:%s",
			t.Task().GetId().Value)
	}

	// Add task to taskSet
	p.taskSet.Add(preemptionCandidate.GetId().GetValue())
	return nil
}

// 1) Moves the state to PENDING state
// 2) Moves the task back to the pending queue
// 3) Add the task resources back to demand
// 4) Subtract the task resources from the resource pool allocation
// The task should be scheduled again at a later time.
func (p *Preemptor) processNonRunningTask(
	rmTask *task.RMTask,
	reason resmgr.PreemptionReason,
) error {
	t := rmTask.Task()
	resPool := rmTask.Respool()
	log.
		WithField("task_id", t.Id.Value).
		WithField("respool_id", resPool.ID()).
		WithField("state", rmTask.GetCurrentState()).
		Infof("Evicting non-running task from resource pool")

	// Transit task to PENDING
	if err := rmTask.TransitTo(peloton_task.TaskState_PENDING.String(),
		statemachine.WithReason(reason.String())); err != nil {
		log.
			WithField("task_id", t.Id.Value).
			WithField("respool_id", resPool.ID()).
			WithField("state", rmTask.GetCurrentState()).
			Debug("Unable to transit non-running task to PENDING for" +
				" preemption")
		// The task could have transited to another state
		return nil
	}

	// Enqueue task to the resource pool
	// A new gang is created for each task
	if err := resPool.EnqueueGang(&resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			t,
		},
	}); err != nil {
		return errors.Wrapf(err, "unable to enqueue gang to resource "+
			"pool")
	}

	// Add the task resources back to demand
	if err := resPool.AddToDemand(scalar.ConvertToResmgrResource(t.
		Resource)); err != nil {
		return errors.Wrapf(err, "unable to add task resources to "+
			"resource pool demand")
	}

	// Subtract the task resources from the resource pool allocation
	err := resPool.SubtractFromAllocation(scalar.GetTaskAllocation(t))
	if err != nil {
		return errors.Wrapf(err, "unable to subtract allocation from "+
			"resource pool")
	}

	log.WithField("task_id", t.Id.Value).
		WithField("respool_id", resPool.ID()).
		WithField("state", rmTask.GetCurrentState()).
		Debug("Evicted task from resource pool")

	return nil
}

// Resets the state of the resource pool
func (p *Preemptor) markProcessed(respoolID string) {
	p.respoolState[respoolID] = 0
}
