package goalstate

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

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

// JMTask provides an interface to converge actual state to goal state.
type JMTask interface {
	UpdateGoalState(taskInfo *task.TaskInfo)

	ProcessState(ctx context.Context, taskOperator TaskOperator, state State) error
}

// CreateJMTask creates the JM task from task info.
func CreateJMTask(taskInfo *task.TaskInfo) (JMTask, error) {
	task := &jmTask{}

	task.UpdateGoalState(taskInfo)

	return task, nil
}

// jmTask is the wrapper around task info for state machine
// TODO: It's currently not thread safe to process a JMTask. We should consider
// adding a lock and spawning go-routines for the subsequent actions to not
// block event processing.
type jmTask struct {
	task *task.TaskInfo

	// goalState along with the time the goal state was updated.
	goalState     State
	goalStateTime time.Time

	// lastState set, the resulting action and when that action was last tried.
	lastState      State
	lastAction     action
	lastActionTime time.Time
}

func (j *jmTask) UpdateGoalState(taskInfo *task.TaskInfo) {
	// TODO: Reject update if older than current version.
	j.task = taskInfo

	j.goalState.State = taskInfo.GetRuntime().GetGoalState()
	j.goalState.ConfigVersion = taskInfo.GetRuntime().GetDesiredConfigVersion()
	j.goalStateTime = time.Now()
}

// ProcessStatusUpdate takes latest status update then derives actions to converge
// task state to goalstate.
func (j *jmTask) ProcessState(ctx context.Context, taskOperator TaskOperator, state State) error {
	a := suggestAction(state, j.goalState)

	since := time.Now().Sub(j.lastActionTime)
	if a == j.lastAction && since < _retryDelay {
		log.Debugf("skipping state action, last action was tasken only %s ago", since)
		return nil
	}

	j.lastState = state
	j.lastAction = a
	j.lastActionTime = time.Now()

	return j.applyAction(ctx, taskOperator, a)
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
