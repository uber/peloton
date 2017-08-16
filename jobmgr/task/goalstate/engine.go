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
	}
}

type engine struct {
	sync.Mutex

	cfg Config

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	taskOperator TaskOperator

	tracker *tracker

	progress atomic.Uint64
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
		jobID, instanceID, err := util.ParseJobAndInstanceID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			continue
		}

		if j := e.tracker.getJob(&peloton.JobID{Value: jobID}); j != nil {
			j.updateTaskState(uint32(instanceID), event.GetPelotonTaskEvent().GetState())
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

	if e.stopChan != nil {
		return
	}

	e.stopChan = make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := e.syncFromDB(ctx); err != nil {
			log.WithError(err).Warn("failed to sync with DB in goalstate engine")
		}
	}()

	log.Info("goalstate.Engine started")
}

// Stop stops processing status update events
func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	if e.stopChan == nil {
		return
	}

	close(e.stopChan)
	e.stopChan = nil

	log.Info("goalstate.Engine stopping")
}

func (e *engine) updateTask(ctx context.Context, info *task.TaskInfo) error {
	e.Lock()
	defer e.Unlock()

	j := e.tracker.addOrGetJob(info.GetJobId(), e)
	if e.stopChan != nil {
		j.start(e.stopChan)
	}

	j.updateTask(info.GetInstanceId(), info.GetRuntime())

	return nil
}

func (e *engine) syncFromDB(ctx context.Context) error {
	log.Info("syncing goalstate engine with DB goalstates")

	// TODO: Skip completed jobs.
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
