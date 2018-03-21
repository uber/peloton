package cached

import (
	"math"
	"reflect"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	log "github.com/sirupsen/logrus"
)

const (
	// UnknownVersion is used by the goalstate engine, when either the current
	// or desired config version is unknown.
	UnknownVersion = math.MaxUint64
)

// Task in the cache.
type Task interface {
	// Identifier of the task.
	ID() uint32

	// Job identifier the task belongs to.
	JobID() *peloton.JobID

	// UpdateRuntime sets the task run time
	UpdateRuntime(runtime *pbtask.RuntimeInfo)

	// GetRunTime returns the task run time
	GetRunTime() *pbtask.RuntimeInfo

	// GetLastRuntimeUpdateTime returns the last time the task runtime was updated.
	GetLastRuntimeUpdateTime() time.Time

	// CurrentState of the task.
	CurrentState() TaskStateVector

	// GoalState of the task.
	GoalState() TaskStateVector

	// TBD remove LastAction from the cache
	// SetLastAction performed by the task, as well as when it was performed.
	SetLastAction(a string, d time.Time)

	// GetLastAction performed by the task, as well as when it was performed.
	GetLastAction() (string, time.Time)

	// TODO remove all functions related to kill attempts
	// GetKillAttempts returns number of kill attempts up till now
	GetKillAttempts() int

	// IncrementKillAttempts increments the number of kill attempts
	IncrementKillAttempts()

	// ClearKillAttempts  sets kill attempts to 0
	ClearKillAttempts()
}

// TaskStateVector defines the state of a task.
// This encapsulates both the actual state and the goal state.
type TaskStateVector struct {
	State         pbtask.TaskState
	ConfigVersion uint64
}

// newTask creates a new cache task object
func newTask(jobID *peloton.JobID, id uint32) *task {
	task := &task{
		jobID: jobID,
		id:    id,
	}

	return task
}

// task structure holds the information about a given task in the cache.
type task struct {
	sync.RWMutex // Mutex to acquire before accessing any task information in cache

	jobID *peloton.JobID // Parent job identifier
	id    uint32         // instance identifier

	runtime *pbtask.RuntimeInfo // task runtime information

	lastAction     string    // lastAction run on this task by the goal state
	lastActionTime time.Time // the time at which the last action was run on this task by goal state

	lastRuntimeUpdateTime time.Time // last time at which the task runtime information was updated

	// TBD killingAttempts need to be removed from cache
	// killingAttempts tracks how many times we had try to kill this task
	killingAttempts int
}

func (t *task) ID() uint32 {
	return t.id
}

func (t *task) JobID() *peloton.JobID {
	return t.jobID
}

func (t *task) UpdateRuntime(runtime *pbtask.RuntimeInfo) {
	t.Lock()
	defer t.Unlock()

	if reflect.DeepEqual(t.runtime, runtime) {
		return
	}

	// The runtime version needs to be monotonically increasing.
	// If the revision in the update is smaller than the current version,
	// then ignore the update. If the revisions are the same, then reset the
	// cache and let goal state or job runtime updater reload the runtime from DB.

	if t.runtime.GetRevision().GetVersion() > runtime.GetRevision().GetVersion() {
		log.WithField("current_revision", t.runtime.GetRevision().GetVersion()).
			WithField("new_revision", runtime.GetRevision().GetVersion()).
			WithField("new_state", runtime.GetState().String()).
			WithField("old_state", t.runtime.GetState().String()).
			WithField("new_goal_state", runtime.GetGoalState().String()).
			WithField("old_goal_state", t.runtime.GetGoalState().String()).
			Info("got old revision")
		return
	}

	if t.runtime != nil && t.runtime.GetRevision().GetVersion() == runtime.GetRevision().GetVersion() {
		log.WithField("current_revision", t.runtime.GetRevision().GetVersion()).
			WithField("new_revision", runtime.GetRevision().GetVersion()).
			WithField("new_state", runtime.GetState().String()).
			WithField("old_state", t.runtime.GetState().String()).
			WithField("new_goal_state", runtime.GetGoalState().String()).
			WithField("old_goal_state", t.runtime.GetGoalState().String()).
			Debug("got same revision")
		t.runtime = nil
	}

	t.runtime = runtime

	t.lastRuntimeUpdateTime = time.Now()
}

func (t *task) GetRunTime() *pbtask.RuntimeInfo {
	t.RLock()
	defer t.RUnlock()
	return t.runtime
}

func (t *task) GetLastRuntimeUpdateTime() time.Time {
	t.RLock()
	defer t.RUnlock()
	return t.lastRuntimeUpdateTime
}

func (t *task) CurrentState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
	}
}

func (t *task) GoalState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
	}
}

func (t *task) SetLastAction(a string, d time.Time) {
	t.Lock()
	defer t.Unlock()

	t.lastAction = a
	t.lastActionTime = d
}

func (t *task) GetLastAction() (string, time.Time) {
	t.RLock()
	defer t.RUnlock()

	return t.lastAction, t.lastActionTime
}

func (t *task) GetKillAttempts() int {
	t.RLock()
	defer t.RUnlock()

	return t.killingAttempts
}

func (t *task) IncrementKillAttempts() {
	t.Lock()
	defer t.Unlock()

	t.killingAttempts++
}

func (t *task) ClearKillAttempts() {
	t.Lock()
	defer t.Unlock()

	t.killingAttempts = 0
}
