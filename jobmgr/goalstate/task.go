package goalstate

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/util"
)

func (e *engine) processTask(t tracked.Task) {
	j := t.Job()

	// Take job lock only while we evaluate state. That ensure we have a
	// consistent view across the entire job, while we decide the action.
	j.Lock()
	action := e.suggestTaskAction(t)
	lastAction, lastActionTime := t.LastAction()
	j.Unlock()

	// Now runt he action, to reflect the decision taken above.
	success := e.runTaskAction(action, t)

	// Update and reschedule the task, based on the result.
	delay := _indefDelay
	switch {
	case action == tracked.NoAction:
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

func (e *engine) runTaskAction(action tracked.TaskAction, t tracked.Task) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := t.RunAction(ctx, action)
	cancel()

	if err != nil {
		log.
			WithField("job", t.Job().ID().GetValue()).
			WithField("task", t.ID()).
			WithField("action", action).
			WithError(err).
			Error("failed to execute goalstate action")
	}

	return err == nil
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

	// At this point the job has the correct version. Now test for goal state.
	if currentState.State == goalState.State {
		return tracked.NoAction
	}

	// TODO: Make this rules based.
	switch goalState.State {
	case task.TaskState_KILLED:
		switch currentState.State {
		case task.TaskState_LAUNCHING, task.TaskState_RUNNING:
			return tracked.StopAction
		}
	}

	return tracked.NoAction
}
