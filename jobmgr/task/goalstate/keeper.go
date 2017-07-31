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

// Keeper manages task state -> goalstate convergence. For every task it tracks,
// it will derive actions based on state/goalstate difference. Eventually it tries
// to converge state to goalstate. It will only run on jobmgr leader instance.
type Keeper interface {
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

// NewKeeper creates a new TaskGoalstateKeeper.
func NewKeeper(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	taskOperator TaskOperator,
	parentScope tally.Scope) Keeper {
	return &keeper{
		jobStore:     jobStore,
		taskStore:    taskStore,
		taskOperator: taskOperator,
		metrics:      NewMetrics(parentScope.SubScope("goalstate_keeper")),
		tracker:      NewTracker(),
	}
}

type keeper struct {
	sync.Mutex

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	taskOperator TaskOperator

	tracker Tracker

	progress atomic.Uint64
	started  atomic.Bool

	metrics *Metrics
}

// UpdateTaskGoalState update task goalstate.
func (k *keeper) UpdateTaskGoalState(ctx context.Context, info *task.TaskInfo) error {
	// TODO: It could be cleaner if this function persisted the goal state.
	return k.updateTask(ctx, info)
}

// OnEvent callback
func (k *keeper) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback
func (k *keeper) OnEvents(events []*pb_eventstream.Event) {
	for _, event := range events {
		mesosTaskID := event.GetMesosTaskStatus().GetTaskId().GetValue()
		taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			continue
		}

		if t := k.tracker.GetTask(&peloton.TaskID{Value: taskID}); t != nil {
			state := State{
				State:         event.GetPelotonTaskEvent().GetState(),
				ConfigVersion: UnknownVersion,
			}
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			if err := t.ProcessState(ctx, k.taskOperator, state); err != nil {
				log.
					WithError(err).
					WithField("mesos_task_id", mesosTaskID).
					Error("failed to process state")
			}
			cancel()
		}

		k.progress.Store(event.Offset)
	}
}

// GetEventProgress returns the progress
func (k *keeper) GetEventProgress() uint64 {
	return k.progress.Load()
}

// Start starts processing status update events
func (k *keeper) Start() {
	k.Lock()
	defer k.Unlock()

	log.Info("TaskGoalstateKeeper started")
	k.started.Store(true)
}

// Stop stops processing status update events
func (k *keeper) Stop() {
	k.Lock()
	defer k.Unlock()

	log.Info("TaskGoalstateKeeper stopped")
	k.started.Store(false)
}

func (k *keeper) updateTask(ctx context.Context, info *task.TaskInfo) error {
	t, err := k.tracker.AddTask(info)
	if err != nil {
		return err
	}

	t.UpdateGoalState(info)

	return t.ProcessState(ctx, k.taskOperator, State{
		State:         info.GetRuntime().GetState(),
		ConfigVersion: info.GetRuntime().GetConfigVersion(),
	})
}
