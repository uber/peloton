package task

import (
	"container/list"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	log "github.com/Sirupsen/logrus"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
)

// Scheduler defines the interface of task scheduler which schedules
// tasks from the pending queues of resource pools to a ready queue
// using different scheduling policies.
type Scheduler interface {
	// Start starts the task scheduler goroutines
	Start() error
	// Stop stops the task scheduler goroutines
	Stop() error
	// GetReadyQueue returns the Ready queue in which all tasks which
	// are ready to be placed
	GetReadyQueue() queue.Queue
}

// scheduler implements the TaskScheduler interface
type scheduler struct {
	sync.Mutex
	runningState     int32
	resPoolTree      respool.Tree
	schedulingPeriod time.Duration
	stopChan         chan struct{}
	readyQueue       queue.Queue
	rmTaskTracker    Tracker
}

var sched *scheduler

// InitScheduler initializes a Task Scheduler
func InitScheduler(
	taskSchedulingPeriod time.Duration,
	rmTaskTracker Tracker,
) {

	if sched != nil {
		log.Warning("Task scheduler has already been initialized")
		return
	}

	sched = &scheduler{
		resPoolTree:      respool.GetTree(),
		runningState:     runningStateNotStarted,
		schedulingPeriod: taskSchedulingPeriod,
		stopChan:         make(chan struct{}, 1),
		// TODO: initialize ready queue elsewhere
		readyQueue: queue.NewQueue(
			"ready-queue",
			reflect.TypeOf(resmgr.Task{}),
			maxReadyQueueSize,
		),
		rmTaskTracker: rmTaskTracker,
	}
}

// GetScheduler returns the task scheduler instance
func GetScheduler() Scheduler {
	if sched == nil {
		log.Fatalf("Task scheduler is not initialized")
	}
	return sched
}

// Start starts the Task Scheduler in a goroutine
func (s *scheduler) Start() error {
	defer s.Unlock()
	s.Lock()

	if s.runningState == runningStateRunning {
		log.Warn("Task Scheduler is already running, no action will be performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&s.runningState, runningStateNotStarted)
		atomic.StoreInt32(&s.runningState, runningStateRunning)

		log.Info("Starting Task Scheduler")
		started <- 0

		for {
			// TODO: we need to remove timer and use chanel for signaling
			// For three cases
			// 1. When there is new Item in empty list
			// 2. When there is new Entitlement calculation
			// 3. When there is chamge in resources in resource pool
			timer := time.NewTimer(s.schedulingPeriod)
			select {
			case <-s.stopChan:
				log.Info("Exiting Task Scheduler")
				return
			case <-timer.C:
				s.scheduleTasks()
			}
			timer.Stop()
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// scheduleTasks moves gang tasks to ready queue in every scheduling cycle
func (s *scheduler) scheduleTasks() {
	// TODO: consider add DequeueTasks to respool.Tree interface
	// instead of returning all leaf nodes.
	nodes := s.resPoolTree.GetAllNodes(true)
	// TODO: we need to check the entitlement first
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		tlist, err := n.DequeueGangList(dequeueTaskLimit)
		if err != nil {
			log.WithField("respool", n.ID()).Debug("No Items found")
			continue
		}
		for tl := tlist.Front(); tl != nil; tl = tl.Next() {
			tle := tl.Value.(*list.List)
			for t := tle.Front(); t != nil; t = t.Next() {
				task := t.Value.(*resmgr.Task)
				s.readyQueue.Enqueue(task)
				if s.rmTaskTracker.GetTask(task.Id) != nil {
					err := s.rmTaskTracker.GetTask(task.Id).
						TransitTo(pt.TaskState_READY.String())
					if err != nil {
						log.WithError(err).Error("Error while " +
							"tranistioning to Ready state")
					}
				} else {
					log.WithError(err).Error("Error while " +
						"tranistioning to Ready state")
				}
			}
		}
	}
}

// Stop stops Task Scheduler process
func (s *scheduler) Stop() error {
	defer s.Unlock()
	s.Lock()

	if s.runningState == runningStateNotStarted {
		log.Warn("Task Scheduler is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Scheduler")
	s.stopChan <- struct{}{}

	// Wait for task scheduler to be stopped
	for {
		runningState := atomic.LoadInt32(&s.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Scheduler Stopped")
	return nil
}

// GetReadyQueue returns the Ready queue in which all tasks which are
// ready to be placed
func (s *scheduler) GetReadyQueue() queue.Queue {
	return s.readyQueue
}
