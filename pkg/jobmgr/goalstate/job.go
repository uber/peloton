// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/cached"

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
	// KillAndUntrackAction kills all tasks and untrack the job when possible
	KillAndUntrackAction JobAction = "kill_untrack"
	// JobStateInvalidAction is executed for an unexpected/invalid job goal state,
	// state combination and it prints a sentry error
	JobStateInvalidAction JobAction = "state_invalid"
	// RuntimeUpdateAction updates the job runtime
	RuntimeUpdateAction JobAction = "runtime_update"
	// EvaluateSLAAction evaluates job SLA
	EvaluateSLAAction JobAction = "evaluate_sla"
	// RecoverAction attempts to recover a partially created job
	RecoverAction JobAction = "recover"
	// DeleteFromActiveJobsAction deletes a jobID from active jobs list if
	// the job is a terminal BATCH job
	DeleteFromActiveJobsAction JobAction = "delete_from_active_jobs"
	// StartTasksAction starts all tasks of a job
	StartTasksAction JobAction = "job_start"
	// DeleteJobAction deletes a job from cache and DB
	DeleteJobAction JobAction = "delete"
	// ReloadRuntimeAction reloads the job runtime into the cache
	ReloadRuntimeAction JobAction = "reload"
	// KillAndDeleteJobAction kills a job and deletes it if possible
	KillAndDeleteJobAction JobAction = "kill_and_delete"
)

// _jobActionsMaps maps the JobAction string to the Action function.
var (
	_jobActionsMaps = map[JobAction]goalstate.ActionExecute{
		NoJobAction:            nil,
		CreateTasksAction:      JobCreateTasks,
		KillAction:             JobKill,
		UntrackAction:          JobUntrack,
		KillAndUntrackAction:   JobKillAndUntrack,
		JobStateInvalidAction:  JobStateInvalid,
		RecoverAction:          JobRecover,
		StartTasksAction:       JobStart,
		DeleteJobAction:        JobDelete,
		ReloadRuntimeAction:    JobReloadRuntime,
		KillAndDeleteJobAction: JobKillAndDelete,
	}
)

var (
	// _isoVersionsJobRules maps current states to action, given a goal state:
	// goal-state -> current-state -> action.
	_isoVersionsJobRules = map[job.JobState]map[job.JobState]JobAction{
		job.JobState_RUNNING: {
			job.JobState_INITIALIZED:   CreateTasksAction,
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
			job.JobState_SUCCEEDED:     KillAndUntrackAction,
			job.JobState_FAILED:        KillAndUntrackAction,
			job.JobState_KILLED:        KillAndUntrackAction,
			job.JobState_UNINITIALIZED: KillAndUntrackAction,
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
		job.JobState_DELETED: {
			job.JobState_UNINITIALIZED: DeleteJobAction,
			job.JobState_INITIALIZED:   KillAction,
			job.JobState_PENDING:       KillAction,
			job.JobState_RUNNING:       KillAction,
			job.JobState_KILLING:       KillAction,
			job.JobState_FAILED:        KillAndDeleteJobAction,
			job.JobState_KILLED:        KillAndDeleteJobAction,
			job.JobState_SUCCEEDED:     KillAndDeleteJobAction,
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
	goalState interface{},
) (context.Context, context.CancelFunc, []goalstate.Action) {
	var actions []goalstate.Action

	jobState := state.(cached.JobStateVector)
	jobGoalState := goalState.(cached.JobStateVector)

	if jobState.State == job.JobState_UNKNOWN ||
		jobGoalState.State == job.JobState_UNKNOWN {
		// State or goal state could not be loaded from DB, so
		// reload the job so that the states can be fetched again.
		actions = append(actions, goalstate.Action{
			Name:    string(ReloadRuntimeAction),
			Execute: JobReloadRuntime,
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

	actions = append(actions, goalstate.Action{
		Name:    string(DeleteFromActiveJobsAction),
		Execute: DeleteJobFromActiveJobs,
	})

	if actionStr == DeleteJobAction ||
		actionStr == KillAndDeleteJobAction {
		return context.Background(), nil, actions
	}

	// Run this action always.
	actions = append(actions,
		goalstate.Action{
			Name:    "EnqueueJobUpdate",
			Execute: EnqueueJobUpdate,
		})

	if actionStr != UntrackAction &&
		actionStr != KillAndUntrackAction &&
		actionStr != RecoverAction {
		// These should always be run
		actions = append(actions, goalstate.Action{
			Name:    string(RuntimeUpdateAction),
			Execute: JobRuntimeUpdater,
		})

		actions = append(actions, goalstate.Action{
			Name:    string(EvaluateSLAAction),
			Execute: JobEvaluateMaxRunningInstancesSLA,
		})
	}

	return context.Background(), nil, actions
}

// suggestJobAction provides the job action for a given state and goal state
func (j *jobEntity) suggestJobAction(state cached.JobStateVector, goalstate cached.JobStateVector) JobAction {
	if state.StateVersion < goalstate.StateVersion {
		// This condition is true currently only for stateless jobs.
		switch goalstate.State {
		case job.JobState_RUNNING:
			if state.State == job.JobState_INITIALIZED {
				return CreateTasksAction
			}
			return StartTasksAction
		case job.JobState_DELETED, job.JobState_KILLED:
			// Don't do anything. The rules are specified in _isoVersionsJobRules
		default:
			log.WithFields(log.Fields{
				"job_id":             j.GetID(),
				"state_version":      state.StateVersion,
				"goal_state_version": goalstate.StateVersion,
				"goal_state":         goalstate.State.String(),
			}).Warn("unexpected divergence of state version from goal state version")
		}
	}

	// TODO: after all job kill is controlled by job state version and desired state version,
	// consider move rules out of _isoVersionsJobRules and check
	// state version and desired state version here
	if tr, ok := _isoVersionsJobRules[goalstate.State]; ok {
		if a, ok := tr[state.State]; ok {
			return a
		}
	}

	return NoJobAction
}
