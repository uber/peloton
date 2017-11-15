package tracked

import (
	"errors"
	"math"
	"sync"
	"time"

	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

const (
	// UnknownVersion is used by the goalstate engine, when either the current
	// or desired config version is unknown.
	UnknownVersion uint64 = math.MaxUint64
)

// Task tracked by the system, serving as a best effort view of what's stored
// in the database.
type Task interface {
	// ID of the task.
	ID() uint32

	// Job the task belongs to.
	Job() Job

	// RuntimeInfo of the task. This is a best effort cached valued of what's
	// currently stored in the DB.
	Runtime() *pb_task.RuntimeInfo

	// CurrentState of the task.
	CurrentState() State

	// GoalState of the task.
	GoalState() GoalState

	// LastAction performed by the task, as well as when it was performed.
	LastAction() (TaskAction, time.Time)
}

// State of a task. This encapsulate the actual state.
type State struct {
	State         pb_task.TaskState
	ConfigVersion uint64
}

// GoalState of a task. This encapsulate the goal state.
type GoalState struct {
	State         pb_task.TaskGoalState
	ConfigVersion uint64
}

// TaskAction that can be given to the Task.RunAction method.
type TaskAction string

// Actions available to be performed on the task.
const (
	NoAction                   TaskAction = "no_action"
	ReloadRuntime              TaskAction = "reload_runtime"
	UntrackAction              TaskAction = "untrack"
	PreemptAction              TaskAction = "preempt_action"
	InitializeAction           TaskAction = "initialize_task"
	StartAction                TaskAction = "start_task"
	StopAction                 TaskAction = "stop_task"
	UseGoalConfigVersionAction TaskAction = "use_goal_config_version"
)

func newTask(job *job, id uint32) *task {
	task := &task{
		queueItemMixin: newQueueItemMixing(),
		job:            job,
		id:             id,
	}

	return task
}

// task is the wrapper around task info for state machine
type task struct {
	sync.RWMutex
	queueItemMixin

	job *job
	id  uint32

	runtime *pb_task.RuntimeInfo

	// goalState along with the time the goal state was updated.
	stateTime     time.Time
	goalStateTime time.Time

	// lastState set, the resulting action and when that action was last tried.
	lastAction     TaskAction
	lastActionTime time.Time
}

func (t *task) ID() uint32 {
	return t.id
}

func (t *task) Job() Job {
	return t.job
}

func (t *task) Runtime() *pb_task.RuntimeInfo {
	return t.runtime
}

func (t *task) CurrentState() State {
	t.RLock()
	defer t.RUnlock()

	return State{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
	}
}

func (t *task) GoalState() GoalState {
	t.RLock()
	defer t.RUnlock()

	return GoalState{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
	}
}

func (t *task) LastAction() (TaskAction, time.Time) {
	t.RLock()
	defer t.RUnlock()

	return t.lastAction, t.lastActionTime
}

// getRuntime returns a shallow copy of the runtime, or an error if's not set.
func (t *task) getRuntime() (*pb_task.RuntimeInfo, error) {
	t.RLock()
	defer t.RUnlock()

	if t.runtime == nil {
		return nil, errors.New("missing task runtime")
	}

	// Shallow copy of the runtime.
	runtime := *t.runtime
	// Also do a copy of the revision, to ensure storage doesn't change it.
	if runtime.Revision != nil {
		revision := *runtime.Revision
		runtime.Revision = &revision
	}
	return &runtime, nil
}

func (t *task) updateRuntime(runtime *pb_task.RuntimeInfo) {
	t.Lock()
	defer t.Unlock()

	// Ignore older revisions of the task runtime.
	if runtime != nil && runtime.GetRevision().GetVersion() < t.runtime.GetRevision().GetVersion() {
		return
	}

	t.runtime = runtime

	now := time.Now()
	t.goalStateTime = now
	t.stateTime = now
}
