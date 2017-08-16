package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/util"
)

const (
	// UnknownVersion is used by the goalstate engine, when either the current
	// or desired config version is unknown.
	UnknownVersion = -1

	// TODO: Should be configurable.
	_retryDelay = 30 * time.Second
)

type action int

const (
	_noAction             action = iota
	_stopAction           action = iota
	_useGoalVersionAction action = iota
)

// State of a job. This can encapsulate either the actual state or the goal
// state.
type State struct {
	State         task.TaskState
	ConfigVersion int64
}

func newTrackedTask(job *trackedJob, id uint32) *trackedTask {
	task := &trackedTask{
		job:        job,
		id:         id,
		queueIndex: -1,
	}

	return task
}

// trackedTask is the wrapper around task info for state machine
type trackedTask struct {
	job *trackedJob
	id  uint32

	runtime *task.RuntimeInfo

	// queueDeadline and index is used by the timeout queue to schedule workload.
	queueDeadline time.Time
	queueIndex    int

	// goalState along with the time the goal state was updated.
	stateTime     time.Time
	goalStateTime time.Time

	// lastState set, the resulting action and when that action was last tried.
	lastState      State
	lastAction     action
	lastActionTime time.Time
}

func (t *trackedTask) deadline() time.Time {
	return t.queueDeadline
}

func (t *trackedTask) setDeadline(deadline time.Time) {
	t.queueDeadline = deadline
}

func (t *trackedTask) index() int {
	return t.queueIndex
}

func (t *trackedTask) setIndex(i int) {
	t.queueIndex = i
}

func (t *trackedTask) updateRuntime(runtime *task.RuntimeInfo) {
	// TODO: Reject update if older than current revision.
	t.runtime = runtime

	now := time.Now()
	t.goalStateTime = now
	t.stateTime = now
}

func (t *trackedTask) updateState(currentState task.TaskState) {
	// TODO: Should we set version to unknown?
	t.runtime.State = currentState
	t.stateTime = time.Now()
}

func (t *trackedTask) currentState() State {
	return State{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
	}
}

func (t *trackedTask) goalState() State {
	return State{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
	}
}

func (t *trackedTask) updateLastAction(a action, s State) {
	t.lastAction = a
	t.lastState = s
	t.lastActionTime = time.Now()
}

func (t *trackedTask) applyAction(ctx context.Context, taskOperator TaskOperator, a action) error {
	switch a {
	case _noAction:
		return nil

	case _stopAction:
		return taskOperator.StopTask(ctx, t.runtime)

	default:
		return fmt.Errorf("unhandled action `%v` in goalstate engine", a)
	}
}

func suggestAction(state, goalState State) action {
	// First test if the task is at the goal version. If not, we'll have to
	// trigger a stop and wait until the task is in a terminal state.
	if state.ConfigVersion != goalState.ConfigVersion {
		switch {
		case state.ConfigVersion == UnknownVersion,
			goalState.ConfigVersion == UnknownVersion:
			// Ignore versions if version is unknown.

		case util.IsPelotonStateTerminal(state.State):
			return _useGoalVersionAction

		default:
			return _stopAction
		}
	}

	// At this point the job has the correct version. Now test for goal state.
	if state.State == goalState.State {
		return _noAction
	}

	// TODO: Make this rules based.
	switch goalState.State {
	case task.TaskState_KILLED:
		switch state.State {
		case task.TaskState_LAUNCHING, task.TaskState_RUNNING:
			return _stopAction
		}
	}

	return _noAction
}
