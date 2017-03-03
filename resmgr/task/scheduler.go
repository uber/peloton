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

// NewTaskScheduler initiates Task Scheduler
func NewTaskScheduler(resPoolTree *respool.Tree, taskSchedulingPeriod time.Duration,
	readyQueue *queue.MultiLevelList) *Scheduler {
	taskScheduler := &Scheduler{
		resPoolTree:       resPoolTree,
		runningState:      runningStateNotStarted,
		schedulingPeriod:  taskSchedulingPeriod,
		stopTaskScheduler: make(chan struct{}, 1),
		readyQueue:        readyQueue,
	}
	return taskScheduler
}

// Scheduler implements Task Scheduler
type Scheduler struct {
	sync.Mutex
	runningState      int32
	resPoolTree       *respool.Tree
	schedulingPeriod  time.Duration
	stopTaskScheduler chan struct{}
	readyQueue        *queue.MultiLevelList
}

// Start starts Task Scheduler
func (p *Scheduler) Start() {
	defer p.Unlock()
	p.Lock()

	if p.runningState == runningStateRunning {
		log.Warn("Task Scheduler is already running, no action will be performed")
		return
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&p.runningState, runningStateNotStarted)
		atomic.StoreInt32(&p.runningState, runningStateRunning)

		log.Info("Starting Task Scheduler")
		started <- 0

		for {
			// TODO: we need to remove timer and use chanel for signaling
			// For three cases
			// 1. When there is new Item in empty list
			// 2. When there is new Entitlement calculation
			// 3. When there is chamge in resources in resource pool
			timer := time.NewTimer(p.schedulingPeriod)
			select {
			case <-p.stopTaskScheduler:
				log.Info("Exiting Task Scheduler")
				return
			case <-timer.C:
				log.Info("Running Task scheduler")
				p.scheduleTasks()
			}
			timer.Stop()
		}
	}()
	// Wait until go routine is started
	<-started
}

// movingTask moves the task to ready queue in every scheduling cycle
func (p *Scheduler) scheduleTasks() {
	nodes := p.resPoolTree.GetAllLeafNodes()
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
			p.readyQueue.Push(int(task.Priority), t)
		}
	}
}

// Stop stops Task Scheduler process
func (p *Scheduler) Stop() {
	defer p.Unlock()
	p.Lock()

	if p.runningState == runningStateNotStarted {
		log.Warn("Task Scheduler is already stopped, no action will be performed")
		return
	}

	log.Info("Stopping Task Scheduler")
	p.stopTaskScheduler <- struct{}{}

	// Wait for task scheduler to be stopped
	for {
		runningState := atomic.LoadInt32(&p.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Scheduler Stopped")
}
