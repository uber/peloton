package goalstate

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// Engine manages task state -> goalstate convergence. For every task it tracks,
// it will derive actions based on state/goalstate difference. Eventually it tries
// to converge state to goalstate. It will only run on jobmgr leader instance.
type Engine interface {
	// UpdateTaskGoalState update task goalstate.
	UpdateTaskGoalState(ctx context.Context, info *task.TaskInfo) error

	// OnEvent callback
	OnEvent(event *pb_eventstream.Event)

	// OnEvents is the implementation of the event stream handler callback
	OnEvents(events []*pb_eventstream.Event)

	// GetEventProgress returns the progress
	GetEventProgress() uint64

	// Start starts processing status update events
	Start()

	// Stop stops processing status update events
	Stop()
}

// NewEngine creates a new task goalstate Engine.
func NewEngine(
	cfg Config,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	taskOperator TaskOperator,
	parentScope tally.Scope) Engine {
	cfg.normalize()
	return &engine{
		cfg:          cfg,
		jobStore:     jobStore,
		taskStore:    taskStore,
		taskOperator: taskOperator,
		metrics:      NewMetrics(parentScope.SubScope("goalstate_engine")),
		tracker:      newTracker(),
		queue:        newTimerQueue(),
		queueChanged: make(chan struct{}, 1),
	}
}

type engine struct {
	sync.Mutex

	cfg Config

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	taskOperator TaskOperator

	tracker *tracker

	queue        *timeoutQueue
	queueChanged chan struct{}

	progress atomic.Uint64
	started  atomic.Bool
	stopChan chan struct{}

	metrics *Metrics
}

// UpdateTaskGoalState update task goalstate.
func (e *engine) UpdateTaskGoalState(ctx context.Context, info *task.TaskInfo) error {
	// TODO: It could be cleaner if this function persisted the goal state.
	return e.updateTask(ctx, info)
}

// OnEvent callback
func (e *engine) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback
func (e *engine) OnEvents(events []*pb_eventstream.Event) {
	for _, event := range events {
		mesosTaskID := event.GetMesosTaskStatus().GetTaskId().GetValue()
		taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			continue
		}

		if t := e.tracker.getTask(&peloton.TaskID{Value: taskID}); t != nil {
			e.Lock()
			t.updateState(event.GetPelotonTaskEvent().GetState())
			e.scheduleTask(t, time.Now())
			e.Unlock()
		}

		e.progress.Store(event.Offset)
	}
}

// GetEventProgress returns the progress
func (e *engine) GetEventProgress() uint64 {
	return e.progress.Load()
}

// Start starts processing status update events
func (e *engine) Start() {
	e.Lock()
	defer e.Unlock()

	log.Info("goalstate.Engine started")

	e.stopChan = make(chan struct{})

	go e.run()

	if !e.started.Swap(true) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := e.syncFromDB(ctx); err != nil {
				log.WithError(err).Warn("failed to sync with DB in goalstate engine")
			}
		}()
	}
}

// Stop stops processing status update events
func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	log.Info("goalstate.Engine stopped")
	if e.started.Swap(false) {
		close(e.stopChan)
	}
}

func (e *engine) updateTask(ctx context.Context, info *task.TaskInfo) error {
	t, err := e.tracker.addTask(info)
	if err != nil {
		return err
	}

	e.Lock()
	t.updateTask(info)
	e.scheduleTask(t, time.Now())
	e.Unlock()

	return nil
}

func (e *engine) scheduleTask(t *jmTask, timeout time.Time) {
	t.setTimeout(timeout)
	e.queue.update(t)
	select {
	case e.queueChanged <- struct{}{}:
	default:
	}
}

func (e *engine) run() {
	for {
		e.Lock()
		t := e.queue.nextTimeout()
		e.Unlock()

		var tc <-chan time.Time
		if !t.IsZero() {
			tc = time.After(t.Sub(time.Now()))
		}

		select {
		case <-tc:
			e.processNextTask()

		case <-e.queueChanged:

		case <-e.stopChan:
			return
		}
	}
}

func (e *engine) processNextTask() {
	e.Lock()
	defer e.Unlock()

	i := e.queue.popIfReady()
	if i == nil {
		return
	}

	t := i.(*jmTask)

	currentState := t.currentState()
	a := suggestAction(currentState, t.goalState())

	// TODO: Use async.Pool to control number of workers.
	go e.runTask(a, t)
}

// runTask by provided action and scheduling potential retry.
func (e *engine) runTask(a action, t *jmTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := t.applyAction(ctx, e.taskOperator, a)
	if err != nil {
		log.WithError(err).Error("failed to execute goalstate action")
	}
	cancel()

	// Update and reschedule the task, based on the result.
	e.Lock()
	defer e.Unlock()

	var timeout time.Time
	switch {
	case a == _noAction:
		// No need to reschedule.

	case a != t.lastAction:
		// First time we see this, trigger default timeout.
		if err != nil {
			timeout = time.Now().Add(e.cfg.FailureRetryDelay)
		} else {
			timeout = time.Now().Add(e.cfg.SuccessRetryDelay)
		}

	case a == t.lastAction:
		// Not the first time we see this, apply backoff.
		delay := time.Since(t.lastActionTime) * 2
		if delay > e.cfg.MaxRetryDelay {
			delay = e.cfg.MaxRetryDelay
		}
		timeout = time.Now().Add(delay)
	}

	t.updateLastAction(a, t.currentState())
	e.scheduleTask(t, timeout)
}

func (e *engine) syncFromDB(ctx context.Context) error {
	log.Info("syncing goalstate engine with DB goalstates")

	// TODO: Skip completed jobs?
	jobs, err := e.jobStore.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	for id := range jobs {
		tasks, err := e.taskStore.GetTasksForJob(ctx, &peloton.JobID{Value: id})
		if err != nil {
			return err
		}

		// TODO: Skip completed tasks.
		for _, task := range tasks {
			if err := e.updateTask(ctx, task); err != nil {
				return err
			}
		}
	}

	return nil
}
