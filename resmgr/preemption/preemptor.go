package preemption

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
)

// represents the max size of the preemption queue
const maxPreemptionQueueSize = 10000

// Preemptor is the interface for the task preemptor which preempts tasks from
// resource pools whose allocation is more than the entitlement for than a number
// of cycles
type Preemptor interface {
	Start() error
	Stop() error
}

type preemptor struct {
	lock                         sync.Mutex
	runningState                 int32
	resTree                      respool.Tree
	preemptionPeriod             time.Duration
	sustainedOverAllocationCount int
	stopChan                     chan struct{}
	respoolState                 map[string]int
	ranker                       ranker
	preemptionQueue              queue.Queue
}

// NewPreemptor returns a new task preemptor
func NewPreemptor(
	parent tally.Scope,
	preemptionPeriod time.Duration,
	sustainedOverAllocationCount int,
	tracker task.Tracker,
) Preemptor {
	return &preemptor{
		resTree:                      respool.GetTree(),
		runningState:                 common.RunningStateNotStarted,
		preemptionPeriod:             preemptionPeriod,
		sustainedOverAllocationCount: sustainedOverAllocationCount,
		stopChan:                     make(chan struct{}, 1),
		preemptionQueue: queue.NewQueue(
			"preemption-queue",
			reflect.TypeOf(&task.RMTask{}),
			maxPreemptionQueueSize,
		),
		respoolState: make(map[string]int),
		ranker:       newStatePriorityRuntimeRanker(tracker),
	}
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

// Start starts Task Preemptor process
func (p *preemptor) Start() error {
	defer p.lock.Unlock()
	p.lock.Lock()

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
						log.WithError(err).Warn("task preemption unsuccessful")
					}
				}
			}
		}()
	}
	return nil
}

func (p *preemptor) preemptTasks() error {
	// collect resource allocation from all resource pools
	p.updateResourcePoolsState()

	var combinedErr error
	// go through the resource pools which need preemption
	for _, respoolID := range p.getEligibleResPoolsForPreemption() {
		err := p.evictTasks(respoolID)
		if err != nil {
			log.WithField("respool_id", respoolID).
				WithError(err).
				Warn("unable to evict tasks from resource pool")
			combinedErr = multierr.Append(combinedErr, err)
		}
	}
	return combinedErr
}

// Loop through all the leaf nodes and set the count to the number consecutive of times
// the entitlement < allocation ; reset to zero otherwise
func (p *preemptor) updateResourcePoolsState() {
	nodes := p.resTree.GetAllNodes(true)
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		if n.GetEntitlement().LessThanOrEqual(n.GetAllocation()) {
			if count, ok := p.respoolState[n.ID()]; ok {
				p.respoolState[n.ID()] = count + 1
			} else {
				p.respoolState[n.ID()] = 0
			}
		} else {
			// reset counter to zero
			p.respoolState[n.ID()] = 0
		}
	}
}

// evictTasks takes a resource pool ID and attempts to remove tasks to
// bring the resource pool allocation equal to or below the entitlement
func (p *preemptor) evictTasks(respoolID string) error {
	// TODO implement this
	return nil
}

// returns those resource pools which are eligible for preemption
func (p *preemptor) getEligibleResPoolsForPreemption() (resPools []string) {
	for respoolID, count := range p.respoolState {
		if count >= p.sustainedOverAllocationCount {
			resPools = append(resPools, respoolID)
		}
	}
	return resPools
}
