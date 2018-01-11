package goalstate

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/util"
)

const (
	_defaultTaskActionTimeout = 5 * time.Second
)

var (
	// _isoVersionsTaskRules maps current states to action, given a goal state:
	// goal-state -> current-state -> action.
	// It assumes task's runtime and goal are at the same version
	_isoVersionsTaskRules = map[task.TaskState]map[task.TaskState]tracked.TaskAction{
		task.TaskState_UNKNOWN: {
			// This reloads the task runtime from DB if the task runtime in cache is nil
			task.TaskState_UNKNOWN: tracked.ReloadTaskRuntime,
		},
		task.TaskState_RUNNING: {
			task.TaskState_INITIALIZED: tracked.StartAction,
			task.TaskState_LAUNCHED:    tracked.StartAction,
			task.TaskState_SUCCEEDED:   tracked.StartAction,
			task.TaskState_FAILED:      tracked.StartAction,
		},
		task.TaskState_SUCCEEDED: {
			task.TaskState_INITIALIZED: tracked.StartAction,
		},
		task.TaskState_KILLED: {
			task.TaskState_INITIALIZED: tracked.StopAction,
			task.TaskState_PENDING:     tracked.StopAction,
			task.TaskState_LAUNCHING:   tracked.StopAction,
			task.TaskState_LAUNCHED:    tracked.StopAction,
			task.TaskState_RUNNING:     tracked.StopAction,
			task.TaskState_KILLED:      tracked.KilledAction,
			task.TaskState_SUCCEEDED:   tracked.KilledAction,
			task.TaskState_FAILED:      tracked.KilledAction,
		},
		task.TaskState_FAILED: {
			// FAILED is not a valid task goal state.
			// The actions here are used to recover stuck tasks
			// whose goal state was incorrectly set to FAILED.
			task.TaskState_INITIALIZED: tracked.FailAction,
		},
		task.TaskState_PREEMPTING: {
			task.TaskState_INITIALIZED: tracked.StopAction,
			task.TaskState_PENDING:     tracked.StopAction,
			task.TaskState_LAUNCHING:   tracked.StopAction,
			task.TaskState_LAUNCHED:    tracked.StopAction,
			task.TaskState_RUNNING:     tracked.StopAction,
			task.TaskState_LOST:        tracked.PreemptAction,
			task.TaskState_KILLED:      tracked.PreemptAction,
		},
	}
)

func (e *engine) processTask(t tracked.Task) {
	action := e.suggestTaskAction(t)
	lastAction, lastActionTime := t.LastAction()

	// Now run the action, to reflect the decision taken above.
	reschedule, success := e.runTaskAction(action, t)

	// Update and reschedule the task, based on the result.
	delay := _indefDelay
	if reschedule {
		switch {
		case success && action == tracked.StartAction:
			// No need to reschedule.

		case action != lastAction:
			// First time we see this, trigger default timeout.
			if success {
				delay = e.cfg.SuccessRetryDelay
			} else {
				delay = e.cfg.FailureRetryDelay
			}

		case action == lastAction:
			// Not the first time we see this, apply backoff.
			delay = time.Since(lastActionTime)
			if success {
				delay += e.cfg.SuccessRetryDelay
			} else {
				delay += e.cfg.FailureRetryDelay
			}
		}
	}

	var deadline time.Time
	if delay != _indefDelay {
		// Cap delay to max.
		if delay > e.cfg.MaxRetryDelay {
			delay = e.cfg.MaxRetryDelay
		}
		deadline = time.Now().Add(delay)
	}

	e.trackedManager.ScheduleTask(t, deadline)
}

func (e *engine) runTaskAction(action tracked.TaskAction,
	t tracked.Task) (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), _defaultTaskActionTimeout)
	reschedule, err := t.RunAction(ctx, action)
	cancel()

	if err != nil {
		log.
			WithField("job_id", t.Job().ID().GetValue()).
			WithField("instance_id", t.ID()).
			WithField("action", action).
			WithError(err).
			Error("failed to execute goalstate action")
	}

	return reschedule, err == nil
}

func (e *engine) suggestTaskAction(t tracked.Task) tracked.TaskAction {
	currentState := t.CurrentState()
	goalState := t.GoalState()

	// First test if the task is at the goal version. If not, we'll have to
	// trigger a stop and wait until the task is in a terminal state.
	if currentState.ConfigVersion != goalState.ConfigVersion {
		switch {
		case currentState.ConfigVersion == tracked.UnknownVersion,
			goalState.ConfigVersion == tracked.UnknownVersion:
			// Ignore versions if version is unknown.

		case util.IsPelotonStateTerminal(currentState.State):
			return tracked.UseGoalVersionAction

		default:
			return tracked.StopAction
		}
	}

	// At this point the job has the correct version.
	// Find action to reach goal state from current state.
	if tr, ok := _isoVersionsTaskRules[goalState.State]; ok {
		if a, ok := tr[currentState.State]; ok {
			return a
		}
	}

	return tracked.NoAction
}
