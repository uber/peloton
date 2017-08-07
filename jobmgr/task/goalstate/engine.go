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
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	taskOperator TaskOperator,
	parentScope tally.Scope) Engine {
	return &engine{
		jobStore:     jobStore,
		taskStore:    taskStore,
		taskOperator: taskOperator,
		metrics:      NewMetrics(parentScope.SubScope("goalstate_engine")),
		tracker:      newTracker(),
	}
}

type engine struct {
	sync.Mutex

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	taskOperator TaskOperator

	tracker *tracker

	progress atomic.Uint64
	started  atomic.Bool

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
			state := State{
				State:         event.GetPelotonTaskEvent().GetState(),
				ConfigVersion: UnknownVersion,
			}
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			if err := t.processState(ctx, e.taskOperator, state); err != nil {
				log.
					WithError(err).
					WithField("mesos_task_id", mesosTaskID).
					Error("failed to process state")
			}
			cancel()
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

	if !e.started.Swap(true) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// TODO: Should this be non-blocking?
		if err := e.syncFromDB(ctx); err != nil {
			log.WithError(err).Warn("failed to sync with DB in goalstate engine")
		}
	}
}

// Stop stops processing status update events
func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	log.Info("goalstate.Engine stopped")
	e.started.Store(false)
}

func (e *engine) updateTask(ctx context.Context, info *task.TaskInfo) error {
	t, err := e.tracker.addTask(info)
	if err != nil {
		return err
	}

	t.updateGoalState(info)

	return t.processState(ctx, e.taskOperator, State{
		State:         info.GetRuntime().GetState(),
		ConfigVersion: info.GetRuntime().GetConfigVersion(),
	})
}

func (e *engine) syncFromDB(ctx context.Context) error {
	log.Info("syncing goalstate engine with DB goalstates")

	jobs, err := e.jobStore.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	for id := range jobs {
		tasks, err := e.taskStore.GetTasksForJob(ctx, &peloton.JobID{Value: id})
		if err != nil {
			return err
		}

		for _, task := range tasks {
			if err := e.updateTask(ctx, task); err != nil {
				return err
			}
		}
	}

	return nil
}
