package goalstate

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"

	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/storage"
)

// NewKeeper creates a new TaskGoalstateKeeper.
func NewKeeper(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	parentScope tally.Scope) *Keeper {
	return &Keeper{
		jobStore:    jobStore,
		taskStore:   taskStore,
		metrics:     NewMetrics(parentScope.SubScope("goalstate_keeper")),
		taskTracker: NewTracker(),
	}
}

// Keeper manages task state -> goalstate convergence. For every task it tracks,
// it will derive actions based on state/goalstate difference. Eventually it tries
// to converge state to goalstate. It will only run on jobmgr leader instance.
type Keeper struct {
	sync.Mutex

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	taskTracker Tracker

	progress atomic.Uint64
	started  atomic.Bool

	metrics *Metrics
}

// UpdateTaskGoalState update task goalstate.
func (k *Keeper) UpdateTaskGoalState(taskInfo *task.TaskInfo) error {
	return nil
}

// OnEvent callback
func (k *Keeper) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback
func (k *Keeper) OnEvents(events []*pb_eventstream.Event) {
	log.Error("Not implemented")
}

// GetEventProgress returns the progress
func (k *Keeper) GetEventProgress() uint64 {
	return k.progress.Load()
}

// Start starts processing status update events
func (k *Keeper) Start() {
	k.Lock()
	defer k.Unlock()

	log.Info("TaskGoalstateKeeper started")
	k.started.Store(true)
	k.metrics.IsLeader.Update(1.0)
}

// Stop stops processing status update events
func (k *Keeper) Stop() {
	k.Lock()
	defer k.Unlock()

	log.Info("TaskGoalstateKeeper stopped")
	k.started.Store(false)
	k.metrics.IsLeader.Update(0.0)
}
