package task

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
)

var (
	errEnqueueEmptySchedUnit   = errors.New("empty gang to to ready queue enqueue")
	errReadyQueueDequeueFailed = errors.New("dequeue gang from ready queue failed")
	errReadyQueueTaskMissing   = errors.New("task missed tracking in enqueue ready queue")
)

// Scheduler defines the interface of task scheduler which schedules
// tasks from the pending queues of resource pools to a ready queue
// using different scheduling policies.
type Scheduler interface {
	// Start starts the task scheduler goroutines
	Start() error
	// Stop stops the task scheduler goroutines
	Stop() error
	// Enqueues gang (task list) into resource pool ready queue
	EnqueueGang(gang *resmgrsvc.Gang) error
	// Dequeues gang (task list) from the resource pool ready queue
	DequeueGang(maxWaitTime time.Duration) (*resmgrsvc.Gang, error)
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
	metrics          *Metrics
}

var sched *scheduler

// InitScheduler initializes a Task Scheduler
func InitScheduler(
	parent tally.Scope,
	taskSchedulingPeriod time.Duration,
	rmTaskTracker Tracker,
) {

	if sched != nil {
		log.Warning("Task scheduler has already been initialized")
		return
	}
	var gang resmgrsvc.Gang

	sched = &scheduler{
		resPoolTree:      respool.GetTree(),
		runningState:     runningStateNotStarted,
		schedulingPeriod: taskSchedulingPeriod,
		stopChan:         make(chan struct{}, 1),
		// TODO: initialize ready queue elsewhere
		readyQueue: queue.NewQueue(
			"ready-queue",
			reflect.TypeOf(gang),
			maxReadyQueueSize,
		),
		rmTaskTracker: rmTaskTracker,
		metrics:       NewMetrics(parent.SubScope("task_scheduler")),
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
			// 3. When there is change in resources in resource pool
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
	// TODO: consider add DequeueGangs to respool.Tree interface
	// instead of returning all leaf nodes.
	nodes := s.resPoolTree.GetAllNodes(true)
	// TODO: we need to check the entitlement first
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		gangList, err := n.DequeueGangList(dequeueGangLimit)
		if err != nil {
			log.WithField("respool", n.ID()).Debug("No Items found")
			log.WithError(err).Error()
			continue
		}
		for _, gang := range gangList {
			s.EnqueueGang(gang)
			for _, task := range gang.GetTasks() {
				log.WithField("task ", task).Debug("Adding " +
					"task to ready queue")
				if s.rmTaskTracker.GetTask(task.Id) != nil {
					err := s.rmTaskTracker.GetTask(task.Id).
						TransitTo(pt.TaskState_READY.String())
					if err != nil {
						log.WithError(errors.WithStack(err)).Error("Error while " +
							"transitioning to Ready state")
					}
				} else {
					err := errReadyQueueTaskMissing
					log.WithError(err).Error("Error while " +
						"transitioning to Ready state")
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

// EnqueueGang inserts a gang, which is a task list of 1 or more (same priority)
// tasks, into the ready queue.
func (s *scheduler) EnqueueGang(gang *resmgrsvc.Gang) error {
	if (gang == nil) || (len(gang.Tasks) == 0) {
		return errEnqueueEmptySchedUnit
	}
	err := s.readyQueue.Enqueue(gang)
	if err != nil {
		log.WithError(err).Error("error in EnqueueGang")
	}
	s.metrics.ReadyQueueLen.Update(float64(s.readyQueue.Length()))
	return err
}

// DequeueGang dequeues a gang, which is a task list of 1 or more (same priority)
// tasks, from the ready queue.
func (s *scheduler) DequeueGang(maxWaitTime time.Duration) (*resmgrsvc.Gang, error) {
	item, err := s.readyQueue.Dequeue(maxWaitTime)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, errReadyQueueDequeueFailed
	}
	res := item.(*resmgrsvc.Gang)
	s.metrics.ReadyQueueLen.Update(float64(s.readyQueue.Length()))
	return res, nil
}
