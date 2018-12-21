package goalstate

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	log "github.com/sirupsen/logrus"
)

// JobAction is a string for job actions.
type JobAction string

const (
	// NoJobAction implies do not take any action
	NoJobAction JobAction = "noop"
	// CreateTasksAction creates/recovers tasks in a job
	CreateTasksAction JobAction = "create_tasks"
	// KillAction kills all tasks in the job
	KillAction JobAction = "job_kill"
	// UntrackAction deletes the job and all its tasks
	UntrackAction JobAction = "untrack"
	// JobStateInvalidAction is executed for an unexpected/invalid job goal state,
	// state combination and it prints a sentry error
	JobStateInvalidAction JobAction = "state_invalid"
	// RuntimeUpdateAction updates the job runtime
	RuntimeUpdateAction JobAction = "runtime_update"
	// EvaluateSLAAction evaluates job SLA
	EvaluateSLAAction JobAction = "evaluate_sla"
	// ClearAction clears the job runtime
	ClearAction JobAction = "job_clear"
	// EnqeueAction enqueues the job again into the goal state engine
	EnqeueAction JobAction = "enqueue"
	// RecoverAction attempts to recover a partially created job
	RecoverAction JobAction = "recover"
	// DeleteFromActiveJobsAction deletes a jobID from active jobs list if
	// the job is a terminal BATCH job
	DeleteFromActiveJobsAction JobAction = "delete_from_active_jobs"
)

// _jobActionsMaps maps the JobAction string to the Action function.
var (
	_jobActionsMaps = map[JobAction]goalstate.ActionExecute{
		NoJobAction:           nil,
		CreateTasksAction:     JobCreateTasks,
		KillAction:            JobKill,
		UntrackAction:         JobUntrack,
		JobStateInvalidAction: JobStateInvalid,
		RecoverAction:         JobRecover,
	}
)

var (
	// _isoVersionsJobRules maps current states to action, given a goal state:
	// goal-state -> current-state -> action.
	_isoVersionsJobRules = map[job.JobState]map[job.JobState]JobAction{
		job.JobState_RUNNING: {
			job.JobState_INITIALIZED:   CreateTasksAction,
			job.JobState_SUCCEEDED:     JobStateInvalidAction,
			job.JobState_FAILED:        JobStateInvalidAction,
			job.JobState_KILLING:       JobStateInvalidAction,
			job.JobState_UNINITIALIZED: RecoverAction,
		},
		job.JobState_SUCCEEDED: {
			job.JobState_INITIALIZED:   CreateTasksAction,
			job.JobState_SUCCEEDED:     UntrackAction,
			job.JobState_FAILED:        UntrackAction,
			job.JobState_KILLED:        UntrackAction,
			job.JobState_KILLING:       JobStateInvalidAction,
			job.JobState_UNINITIALIZED: RecoverAction,
		},
		job.JobState_KILLED: {
			job.JobState_SUCCEEDED:     UntrackAction,
			job.JobState_FAILED:        UntrackAction,
			job.JobState_KILLED:        UntrackAction,
			job.JobState_UNINITIALIZED: UntrackAction,
			// TODO: revisit the rules after new job kill
			// code is checked in
			job.JobState_INITIALIZED: KillAction,
			job.JobState_PENDING:     KillAction,
			job.JobState_RUNNING:     KillAction,
		},
		job.JobState_FAILED: {
			job.JobState_INITIALIZED:   JobStateInvalidAction,
			job.JobState_PENDING:       JobStateInvalidAction,
			job.JobState_RUNNING:       JobStateInvalidAction,
			job.JobState_SUCCEEDED:     JobStateInvalidAction,
			job.JobState_FAILED:        JobStateInvalidAction,
			job.JobState_KILLED:        JobStateInvalidAction,
			job.JobState_KILLING:       JobStateInvalidAction,
			job.JobState_UNINITIALIZED: JobStateInvalidAction,
		},
	}
)

// NewJobEntity implements the goal state Entity interface for jobs.
func NewJobEntity(id *peloton.JobID, driver *driver) goalstate.Entity {
	return &jobEntity{
		id:     id,
		driver: driver,
	}
}

type jobEntity struct {
	id     *peloton.JobID // peloton job identifier
	driver *driver        // the goal state driver
}

func (j *jobEntity) GetID() string {
	// return job identifier
	return j.id.GetValue()
}

func (j *jobEntity) GetState() interface{} {
	cachedJob := j.driver.jobFactory.AddJob(j.id)
	return cachedJob.CurrentState()
}

func (j *jobEntity) GetGoalState() interface{} {
	cachedJob := j.driver.jobFactory.AddJob(j.id)
	return cachedJob.GoalState()
}

func (j *jobEntity) GetActionList(
	state interface{},
	goalState interface{}) (
	context.Context,
	context.CancelFunc,
	[]goalstate.Action) {
	var actions []goalstate.Action

	jobState := state.(cached.JobStateVector)
	jobGoalState := goalState.(cached.JobStateVector)

	if jobState.State == job.JobState_UNKNOWN || jobGoalState.State == job.JobState_UNKNOWN {
		// State or goal state could not be loaded from DB, so enqueue the job
		// back into the goal state engine so that the states can be fetched again.
		actions = append(actions, goalstate.Action{
			Name:    string(EnqeueAction),
			Execute: JobEnqueue,
		})
		return context.Background(), nil, actions
	}

	actionStr := j.suggestJobAction(jobState, jobGoalState)
	action := _jobActionsMaps[actionStr]

	log.WithField("job_id", j.id.GetValue()).
		WithField("current_state", jobState.State.String()).
		WithField("goal_state", jobGoalState.State.String()).
		WithField("current_state_version", jobState.StateVersion).
		WithField("goal_state_version", jobGoalState.StateVersion).
		WithField("job_action", actionStr).
		Info("running job action")

	if action != nil {
		// nil action is returned for noop
		actions = append(actions, goalstate.Action{
			Name:    string(actionStr),
			Execute: action,
		})
	}

	if actionStr != UntrackAction && actionStr != RecoverAction {
		// These should always be run
		actions = append(actions, goalstate.Action{
			Name:    string(RuntimeUpdateAction),
			Execute: JobRuntimeUpdater,
		})

		actions = append(actions, goalstate.Action{
			Name:    string(EvaluateSLAAction),
			Execute: JobEvaluateMaxRunningInstancesSLA,
		})

		actions = append(actions, goalstate.Action{
			Name:    string(DeleteFromActiveJobsAction),
			Execute: DeleteJobFromActiveJobs,
		})

	}

	return context.Background(), nil, actions
}

// suggestJobAction provides the job action for a given state and goal state
func (j *jobEntity) suggestJobAction(state cached.JobStateVector, goalstate cached.JobStateVector) JobAction {
	// TODO: after all job kill is controlled by job state version and desired state version,
	// consider move rules out of _isoVersionsJobRules and check
	// sate version and desired state version here
	if tr, ok := _isoVersionsJobRules[goalstate.State]; ok {
		if a, ok := tr[state.State]; ok {
			return a
		}
	}

	return NoJobAction
}
