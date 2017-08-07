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

func newJMTask(taskInfo *task.TaskInfo) (*jmTask, error) {
	task := &jmTask{
		queueIndex: -1,
	}

	task.updateTask(taskInfo)

	return task, nil
}

// jmTask is the wrapper around task info for state machine
type jmTask struct {
	task *task.TaskInfo

	// queueTimeout and index is used by the timeout queue to schedule workload.
	queueTimeout time.Time
	queueIndex   int

	// goalState along with the time the goal state was updated.
	stateTime     time.Time
	goalStateTime time.Time

	// lastState set, the resulting action and when that action was last tried.
	lastState      State
	lastAction     action
	lastActionTime time.Time
}

func (j *jmTask) timeout() time.Time {
	return j.queueTimeout
}

func (j *jmTask) setTimeout(t time.Time) {
	j.queueTimeout = t
}

func (j *jmTask) index() int {
	return j.queueIndex
}

func (j *jmTask) setIndex(i int) {
	j.queueIndex = i
}

func (j *jmTask) updateTask(taskInfo *task.TaskInfo) {
	// TODO: Reject update if older than current version.
	j.task = taskInfo

	t := time.Now()
	j.goalStateTime = t
	j.stateTime = t
}

func (j *jmTask) updateState(currentState task.TaskState) {
	// TODO: Should we set version to unknown?
	j.task.Runtime.State = currentState
	j.stateTime = time.Now()
}

func (j *jmTask) currentState() State {
	return State{
		State:         j.task.GetRuntime().GetState(),
		ConfigVersion: j.task.GetRuntime().GetConfigVersion(),
	}
}

func (j *jmTask) goalState() State {
	return State{
		State:         j.task.GetRuntime().GetGoalState(),
		ConfigVersion: j.task.GetRuntime().GetDesiredConfigVersion(),
	}
}

func (j *jmTask) updateLastAction(a action, s State) {
	j.lastAction = a
	j.lastState = s
	j.lastActionTime = time.Now()
}

func (j *jmTask) applyAction(ctx context.Context, taskOperator TaskOperator, a action) error {
	switch a {
	case _noAction:
		return nil

	case _stopAction:
		return taskOperator.StopTask(ctx, j.task)

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
