package task

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/resmgr/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

var (
	errEnqueueEmptySchedUnit    = errors.New("empty gang to to ready queue enqueue")
	errReadyQueueDequeueFailed  = errors.New("dequeue gang from ready queue failed")
	errReadyQueueTaskMissing    = errors.New("task missed tracking in enqueue ready queue")
	errReadyQueueDequeueTimeout = errors.New("dequeue gang from ready queue timed out")
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
	DequeueGang(maxWaitTime time.Duration, taskType resmgr.TaskType) (*resmgrsvc.Gang, error)
}

// scheduler implements the TaskScheduler interface
type scheduler struct {
	lock             sync.Mutex
	condition        *sync.Cond
	runningState     int32
	resPoolTree      respool.Tree
	schedulingPeriod time.Duration
	stopChan         chan struct{}
	queue            *queue.MultiLevelList
	rmTaskTracker    Tracker
	metrics          *Metrics
	random           *rand.Rand
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
	sched = &scheduler{
		condition:        sync.NewCond(&sync.Mutex{}),
		resPoolTree:      respool.GetTree(),
		runningState:     runningStateNotStarted,
		schedulingPeriod: taskSchedulingPeriod,
		stopChan:         make(chan struct{}, 1),
		// TODO: initialize ready queue elsewhere
		queue:         queue.NewMultiLevelList("ready-queue", maxReadyQueueSize),
		rmTaskTracker: rmTaskTracker,
		metrics:       NewMetrics(parent.SubScope("task_scheduler")),
		random:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	log.Info("Task scheduler is initialized")
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
	defer s.lock.Unlock()
	s.lock.Lock()

	if s.runningState == runningStateRunning {
		log.Warn("Task Scheduler is already running, no action will be performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&s.runningState, runningStateNotStarted)
		atomic.StoreInt32(&s.runningState, runningStateRunning)
		ticker := time.NewTicker(s.schedulingPeriod)
		defer ticker.Stop()

		log.Info("Starting Task Scheduler")
		started <- 0

		for {
			// TODO: we need to remove ticker and use chanel for signaling
			// For three cases
			// 1. When there is new Item in empty list
			// 2. When there is new Entitlement calculation
			// 3. When there is change in resources in resource pool

			select {
			case <-s.stopChan:
				log.Info("Exiting Task Scheduler")
				return
			case <-ticker.C:
				s.scheduleTasks()
			}
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
			log.WithField("respool", n.ID()).WithError(err).Debug("No Items found")
			continue
		}
		for _, gang := range gangList {
			err = s.EnqueueGang(gang)
			if err != nil {
				continue
			}
			for _, task := range gang.GetTasks() {
				log.WithField("task ", task).Debug("Adding " +
					"task to ready queue")
				if s.rmTaskTracker.GetTask(task.Id) != nil {
					err := s.rmTaskTracker.GetTask(task.Id).
						TransitTo(pt.TaskState_READY.String())
					if err != nil {
						log.WithError(errors.WithStack(err)).Error("error while " +
							"transitioning to Ready state")
					}
				} else {
					log.Error(errReadyQueueTaskMissing)
				}
				// We have to remove demand as we admitted task to
				// ready queue.
				err = n.SubtractFromDemand(
					scalar.ConvertToResmgrResource(
						task.GetResource()))
				if err != nil {
					log.WithError(err).Errorf("Error while "+
						"subtracting demand for task %s ",
						task.Id.Value)
				}
			}
		}
	}
}

// Stop stops Task Scheduler process
func (s *scheduler) Stop() error {
	defer s.lock.Unlock()
	s.lock.Lock()

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
	level := gang.Tasks[0].Type
	err := s.queue.Push(int(level), gang)
	if err != nil {
		log.WithError(err).Error("error in EnqueueGang")
	}
	s.metrics.ReadyQueueLen.Update(float64(s.queue.Size()))
	if err == nil {
		s.condition.Broadcast()
	}
	return err
}

// DequeueGang dequeues a gang, which is a task list of 1 or more (same priority)
// tasks of type task type, from the ready queue. If task type is UNKNOWN then
// gangs with tasks of any task type will be returned.
func (s *scheduler) DequeueGang(maxWaitTime time.Duration, taskType resmgr.TaskType) (*resmgrsvc.Gang, error) {
	level := int(taskType)
	if taskType == resmgr.TaskType_UNKNOWN {
		levels := s.queue.Levels()
		if len(levels) > 0 {
			level = levels[s.random.Intn(len(levels))]
		}
	}
	s.condition.L.Lock()
	defer s.condition.L.Unlock()
	pastDeadline := uint32(0)
	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-timer.C:
			atomic.StoreUint32(&pastDeadline, 1)
			s.condition.Broadcast()
		}
	}()
	var item interface{}
	var err error
	for {
		item, err = s.queue.Pop(level)
		_, isEmpty := err.(queue.ErrorQueueEmpty)
		if atomic.LoadUint32(&pastDeadline) == 1 || err == nil || !isEmpty {
			break
		}
		s.condition.Wait()
	}
	if item == nil {
		if atomic.LoadUint32(&pastDeadline) == 1 {
			return nil, errReadyQueueDequeueTimeout
		}
		return nil, errReadyQueueDequeueFailed
	}
	res := item.(*resmgrsvc.Gang)
	s.metrics.ReadyQueueLen.Update(float64(s.queue.Size()))
	return res, nil
}
