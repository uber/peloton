package task

import (
	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	log "github.com/Sirupsen/logrus"
	"peloton/private/resmgr"
	"sync"
	"sync/atomic"
	"time"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1
	dequeueTaskLimit       = 1000
)

// Scheduler defines the interface of task scheduler which schedules
// tasks from the pending queues of resource pools to a ready queue
// using different scheduling policies.
type Scheduler interface {
	Start() error
	Stop() error
}

// scheduler implements the TaskScheduler interface
type scheduler struct {
	sync.Mutex
	runningState     int32
	resPoolTree      *respool.Tree
	schedulingPeriod time.Duration
	stopChan         chan struct{}
	readyQueue       *queue.MultiLevelList
}

var sched *scheduler

// InitScheduler initializes a Task Scheduler
func InitScheduler(taskSchedulingPeriod time.Duration) {

	// TODO: replace with respool.GetTree
	resPoolTree := respool.GetServiceHandler().GetResourcePoolTree()

	sched = &scheduler{
		resPoolTree:      resPoolTree,
		runningState:     runningStateNotStarted,
		schedulingPeriod: taskSchedulingPeriod,
		stopChan:         make(chan struct{}, 1),
		// TODO: initialize ready queue elsewhere
		readyQueue: queue.NewMultiLevelList(),
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
				log.Debug("Running Task scheduler")
				s.scheduleTasks()
			}
			timer.Stop()
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// scheduleTasks moves the task to ready queue in every scheduling cycle
func (s *scheduler) scheduleTasks() {
	nodes := s.resPoolTree.GetAllLeafNodes()
	// TODO: we need to check the entitlement first
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(*respool.ResPool)
		t, err := n.DequeueTasks(dequeueTaskLimit)
		if err != nil {
			log.WithField("respool", n.ID).Debug("No Items found")
			continue
		}
		for e := t.Front(); e != nil; e = e.Next() {
			task := e.Value.(*resmgr.Task)
			s.readyQueue.Push(int(task.Priority), t)
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
func (s *scheduler) GetReadyQueue() *queue.MultiLevelList {
	return s.readyQueue
}
