package preemption

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	peloton_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/common/statemachine"
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

var (
	once sync.Once
	p    *preemptor
)

// Preemptor is the interface for the task preemptor which preempts tasks from
// resource pools whose allocation is more than the entitlement for than a
// given number of cycles
type Preemptor interface {
	Start() error
	Stop() error
	DequeueTask(maxWaitTime time.Duration) (*resmgr.Task, error)
}

type preemptor struct {
	lock                         sync.Mutex
	enabled                      bool
	runningState                 int32
	resTree                      respool.Tree
	preemptionPeriod             time.Duration
	sustainedOverAllocationCount int
	stopChan                     chan struct{}
	respoolState                 map[string]int
	ranker                       ranker
	tracker                      task.Tracker
	preemptionQueue              queue.Queue
	scope                        tally.Scope
	m                            map[string]*Metrics
}

// InitPreemptor initializes the task preemptor
func InitPreemptor(
	parent tally.Scope,
	cfg *Config,
	tracker task.Tracker,
) {
	once.Do(func() {
		p = &preemptor{
			resTree:                      respool.GetTree(),
			runningState:                 common.RunningStateNotStarted,
			preemptionPeriod:             cfg.TaskPreemptionPeriod,
			sustainedOverAllocationCount: cfg.SustainedOverAllocationCount,
			enabled:  cfg.Enabled,
			stopChan: make(chan struct{}, 1),
			preemptionQueue: queue.NewQueue(
				"preemption-queue",
				reflect.TypeOf(resmgr.Task{}),
				maxPreemptionQueueSize,
			),
			respoolState: make(map[string]int),
			ranker:       newStatePriorityRuntimeRanker(tracker),
			tracker:      tracker,
			scope:        parent.SubScope("preemption"),
			m:            make(map[string]*Metrics),
		}
	})
}

// GetPreemptor returns the task scheduler instance
func GetPreemptor() Preemptor {
	if p == nil {
		log.Fatalf("Task preemptor is not initialized")
	}
	return p
}

// returns per resource pool tagged metrics
func (p *preemptor) metrics(pool respool.ResPool) *Metrics {
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
func (p *preemptor) Start() error {
	defer p.lock.Unlock()
	p.lock.Lock()

	if !p.enabled {
		log.Infof("Task preemptor is not enabled to run")
		return nil
	}

	if atomic.CompareAndSwapInt32(&p.runningState, common.RunningStateNotStarted, common.RunningStateRunning) {
		go func() {
			defer atomic.StoreInt32(&p.runningState, common.RunningStateNotStarted)

			ticker := time.NewTicker(p.preemptionPeriod)
			defer ticker.Stop()

			log.Info("Starting Task Preemptor")

			for {
				select {
				case <-p.stopChan:
					log.Info("Exiting Task Preemptor")
					return
				case <-ticker.C:
					err := p.preemptTasks()
					if err != nil {
						log.WithError(err).Warn("Task preemption unsuccessful")
					}
				}
			}
		}()
	}
	return nil
}

// Stop stops Task Preemptor process
func (p *preemptor) Stop() error {
	defer p.lock.Unlock()
	p.lock.Lock()

	if p.runningState == common.RunningStateNotStarted {
		log.Warn("Task preemptor is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task preemptor")
	close(p.stopChan)

	// Wait for task preemptor to be stopped
	for {
		runningState := atomic.LoadInt32(&p.runningState)
		if runningState == common.RunningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Preemptor Stopped")
	return nil
}

// DequeueTask dequeues a task from the preemption queue
func (p *preemptor) DequeueTask(maxWaitTime time.Duration) (*resmgr.Task, error) {
	item, err := p.preemptionQueue.Dequeue(maxWaitTime)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so we log the error
			log.WithError(err).
				Error("unable to dequeue task from preemption queue")
		}
		return nil, err
	}
	taskID := item.(*resmgr.Task)
	return taskID, nil
}

func (p *preemptor) preemptTasks() error {
	// collect resource allocation from all resource pools
	p.updateResourcePoolsState()

	var combinedErr error
	// go through the resource pools which need preemption
	for _, respoolID := range p.getEligibleResPools() {
		err := p.processResourcePool(respoolID)
		if err != nil {
			combinedErr = multierr.Append(combinedErr,
				errors.Wrapf(err, "unable to preempt tasks from resource pool :%s", respoolID))
		}
	}
	return combinedErr
}

// Loop through all the leaf nodes and set the count to the number consecutive of times
// the  allocation > entitlement; reset to zero otherwise
func (p *preemptor) updateResourcePoolsState() {
	nodes := p.resTree.GetAllNodes(true)
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		resourcesAboveEntitlement := n.GetTotalAllocatedResources().Subtract(n.GetEntitlement())
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

// Resets the state of the resource pool
func (p *preemptor) markProcessed(respoolID string) {
	p.respoolState[respoolID] = 0
}

// processResourcePool takes a resource pool ID and performs actions
// on the tasks based on their current state
func (p *preemptor) processResourcePool(respoolID string) error {
	resourcePool, err := p.resTree.Get(&peloton.ResourcePoolID{Value: respoolID})
	if err != nil {
		return errors.Wrap(err, "unable to get resource pool")
	}
	resourcesToFree := resourcePool.GetTotalAllocatedResources().Subtract(resourcePool.GetEntitlement())
	log.
		WithField("respool_id", respoolID).
		WithField("resource_to_free", resourcesToFree).
		Info("Resource to free from resource pool")

	tasks := p.ranker.GetTasksToEvict(respoolID, resourcesToFree)
	log.WithField("tasks", tasks).
		WithField("respool_id", respoolID).
		Debug("Tasks to evict from resource pool")

	var errs error
	for _, t := range tasks {
		switch t.GetCurrentState() {
		case peloton_task.TaskState_RUNNING:
			log.
				WithField("task_id", t.Task().Id.Value).
				WithField("respool_id", respoolID).
				Debug("Adding task to preemption queue")
			err := p.preemptionQueue.Enqueue(t.Task())
			if err != nil {
				// add error and metrics and move to the next task
				errs = multierr.Append(errs,
					errors.Wrapf(err, "unable to add RUNNING task to "+
						"preemption queue task ID:%s",
						t.Task().GetId().Value))
				p.metrics(resourcePool).TasksFailedPreemption.Inc(int64(1))
				continue
			}
			p.metrics(resourcePool).RunningTasksPreempted.Inc(int64(1))
		default:
			// For all non running tasks
			err := p.evictNonRunningTask(t)
			if err != nil {
				// add error and metrics and move to the next task
				errs = multierr.Append(errs,
					errors.Wrapf(err, "unable to evict task:%s with "+
						"state:%s",
						t.Task().GetId().Value, t.GetCurrentState().String()))
				p.metrics(resourcePool).TasksFailedPreemption.Inc(int64(1))
				continue
			}
			p.metrics(resourcePool).NonRunningTasksPreempted.Inc(int64(1))
		}
		// update resource freed metric
		// NB: This can also include resource for running tasks which will
		// technically will be freed once the task is preempted by job manager
		p.metrics(resourcePool).ResourcesFreed.Update(
			scalar.ConvertToResmgrResource(t.Task().Resource))
	}
	// we've processed the pool
	p.markProcessed(respoolID)
	return errs
}

func (p *preemptor) evictNonRunningTask(rmTask *task.RMTask) error {
	t := rmTask.Task()
	resPool := rmTask.Respool()
	log.
		WithField("task_id", t.Id.Value).
		WithField("respool_id", resPool.ID()).
		WithField("state", rmTask.GetCurrentState()).
		Infof("Evicting non-running task from resource pool")

	trackedTask := p.tracker.GetTask(t.Id)
	if trackedTask == nil {
		return errors.Errorf("task not found in tracker ID:%s", t.Id.Value)
	}

	// Transit task to PENDING
	if err := trackedTask.TransitTo(peloton_task.TaskState_PENDING.String(), statemachine.WithReason("non-running task evicted")); err != nil {
		// The task could have transited to another state
		log.
			WithField("task_id", t.Id.Value).
			WithField("respool_id", resPool.ID()).
			WithField("state", rmTask.GetCurrentState()).
			Debugf("Unable to transit non-running task to PENDING for" +
				" preemption")
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

// returns those resource pools which are eligible for preemption
func (p *preemptor) getEligibleResPools() (resPools []string) {
	for respoolID, count := range p.respoolState {
		if count >= p.sustainedOverAllocationCount {
			resPools = append(resPools, respoolID)
		}
	}
	log.WithField("pools", resPools).Info(
		"Eligible resource pools for preemption")
	return resPools
}
