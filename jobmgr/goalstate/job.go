package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/jobmgr/tracked"

	log "github.com/sirupsen/logrus"
)

const (
	_defaultJobActionTimeout = 5 * time.Second
)

var (
	// _isoVersionsJobRules maps current states to action, given a goal state:
	// goal-state -> current-state -> action.
	// It assumes job's runtime and goal are at the same version
	_isoVersionsJobRules = map[job.JobState]map[job.JobState]tracked.JobAction{
		job.JobState_RUNNING: {
			job.JobState_INITIALIZED: tracked.JobCreateTasks,
		},
		job.JobState_SUCCEEDED: {
			job.JobState_INITIALIZED: tracked.JobCreateTasks,
		},
	}
)

func (e *engine) processJob(j tracked.Job) {
	var reschedule bool
	var success bool
	action, err := e.suggestJobAction(j)
	if err != nil {
		reschedule = true
		success = false
	} else {
		reschedule, success = e.runJobAction(j, action)
	}

	// Update and reschedule the job, based on the result.
	delay := _indefDelay
	if reschedule {
		if success {
			delay = e.cfg.SuccessRetryDelay
		} else {
			delay = e.cfg.FailureRetryDelay
		}
	}

	var deadline time.Time
	deadline = time.Now().Add(delay)
	e.trackedManager.ScheduleJob(j, deadline)
}

func (e *engine) runJobAction(j tracked.Job, action tracked.JobAction) (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), _defaultJobActionTimeout)
	defer cancel()

	reschedule, err := j.RunAction(ctx, action)
	if err != nil {
		log.
			WithField("job_id", j.ID().GetValue()).
			WithField("action", action).
			WithError(err).
			Error("failed to execute job goalstate action")
	}
	j.ClearJobRuntime()
	return reschedule, err == nil
}

func (e *engine) suggestJobAction(j tracked.Job) (tracked.JobAction, error) {
	// first get the job runtime
	ctx, cancel := context.WithTimeout(context.Background(), _defaultJobActionTimeout)
	defer cancel()
	runtime, err := j.GetJobRuntime(ctx)
	if err != nil {
		return tracked.JobNoAction, err
	}
	currentState := runtime.GetState()
	goalState := runtime.GetGoalState()

	if tr, ok := _isoVersionsJobRules[goalState]; ok {
		if a, ok := tr[currentState]; ok {
			return a, nil
		}
	}
	return tracked.JobNoAction, nil
}
