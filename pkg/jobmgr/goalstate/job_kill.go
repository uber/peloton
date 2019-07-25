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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// JobKill will stop all tasks in the job.
func JobKill(ctx context.Context, entity goalstate.Entity) error {
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		return nil
	}

	jobState, nonTerminalTaskKilled, err :=
		killJob(ctx, cachedJob, goalStateDriver)
	if err != nil {
		return err
	}

	// Only enqueue the job into goal state:
	// 1. any of the non terminated tasks need to be killed.
	// 2. job state is already KILLED
	if nonTerminalTaskKilled || util.IsPelotonJobStateTerminal(jobState) {
		EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
	}

	log.WithField("job_id", id).
		Info("initiated kill of all tasks in the job")
	return nil
}

// createRuntimeDiffForKill creates the runtime diffs to kill the tasks in job.
// it returns:
// runtimeDiffNonTerminatedTasks which is used to kill non-terminated tasks,
// runtimeDiffTerminatedTasks which is used to kill tasks already terminal
// state (to prevent restart),
// runtimeDiffAll which is a union of runtimeDiffNonTerminatedTasks and
// runtimeDiffTerminatedTasks
func createRuntimeDiffForKill(
	ctx context.Context,
	cachedJob cached.Job,
) (
	runtimeDiffNonTerminatedTasks map[uint32]jobmgrcommon.RuntimeDiff,
	runtimeDiffTerminatedTasks map[uint32]jobmgrcommon.RuntimeDiff,
	runtimeDiffAll map[uint32]jobmgrcommon.RuntimeDiff,
	err error,
) {
	runtimeDiffNonTerminatedTasks = make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffTerminatedTasks = make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffAll = make(map[uint32]jobmgrcommon.RuntimeDiff)

	tasks := cachedJob.GetAllTasks()
	for instanceID, cachedTask := range tasks {
		runtime, err := cachedTask.GetRuntime(ctx)

		// runtime not created yet, ignore the task
		if yarpcerrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			log.WithError(err).
				WithField("job_id", cachedJob.ID().Value).
				WithField("instance_id", instanceID).
				Info("failed to fetch task runtime to kill a job")
			return nil, nil, nil, err
		}

		// A task in terminal state can be running later due to failure
		// retry (batch job) or task restart (stateless job), so it is
		// necessary to kill a task even if it is in terminal state as
		// long as the goal state is not KILLED.
		if runtime.GetGoalState() == task.TaskState_KILLED {
			continue
		}

		runtimeDiff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.GoalStateField: task.TaskState_KILLED,
			jobmgrcommon.MessageField:   "Task stop API request",
			jobmgrcommon.ReasonField:    "",
			jobmgrcommon.TerminationStatusField: &task.TerminationStatus{
				Reason: task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
			},
			jobmgrcommon.DesiredHostField: "",
		}
		runtimeDiffAll[instanceID] = runtimeDiff
		if util.IsPelotonStateTerminal(runtime.GetState()) {
			runtimeDiffTerminatedTasks[instanceID] = runtimeDiff
		} else {
			runtimeDiffNonTerminatedTasks[instanceID] = runtimeDiff
		}
	}
	return runtimeDiffNonTerminatedTasks, runtimeDiffTerminatedTasks, runtimeDiffAll, nil
}

// calculateJobState calculates if the job to be killed is
// in KILLING state or KILLED state
func calculateJobState(
	ctx context.Context,
	cachedJob cached.Job,
	jobRuntime *job.RuntimeInfo,
	runtimeDiffNonTerminatedTasks map[uint32]jobmgrcommon.RuntimeDiff) job.JobState {
	// if no non-terminal instance is killed, check if the job
	// should directly enter KILLED state
	if len(runtimeDiffNonTerminatedTasks) == 0 {
		if cachedJob.GetJobType() == job.JobType_BATCH &&
			jobRuntime.GetState() != job.JobState_INITIALIZED {
			return job.JobState_KILLING
		}

		for _, cachedTask := range cachedJob.GetAllTasks() {
			runtime, err := cachedTask.GetRuntime(ctx)
			if err != nil || !util.IsPelotonStateTerminal(runtime.GetState()) {
				return job.JobState_KILLING
			}
		}
		return job.JobState_KILLED
	}

	return job.JobState_KILLING
}

// killJob kills all tasks in the job, and returns
// 1. the new job state after the kill
// 2. whether any non-terminated task is killed
func killJob(
	ctx context.Context,
	cachedJob cached.Job,
	goalStateDriver Driver,
) (newState job.JobState, taskKilled bool, err error) {
	// Get job runtime and update job state to killing
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to get job runtime during job kill")
		return
	}

	runtimeDiffNonTerminatedTasks, allTasksMarked, err := stopTasks(ctx, cachedJob, goalStateDriver)
	if err != nil {
		err = errors.Wrap(err, "failed to update task runtimes to kill a job")
		return
	}

	// if the goal state of some tasks could not be patched, do not update
	// the state of the job. We should update the job state to KILLING only
	// when the goalstate of all tasks have been set to KILLED.
	if !allTasksMarked {
		return jobRuntime.GetState(), false, nil
	}

	jobState := calculateJobState(
		ctx,
		cachedJob,
		jobRuntime,
		runtimeDiffNonTerminatedTasks,
	)

	runtimeUpdate := &job.RuntimeInfo{
		State:        jobState,
		StateVersion: jobRuntime.DesiredStateVersion,
	}

	if util.IsPelotonJobStateTerminal(jobState) {
		runtimeUpdate.CompletionTime = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// update job state as well as state version,
	// once state version == desired state version,
	// goal state engine knows that the all the tasks
	// are being sent to task goal state engine to kill and
	// no further action is needed.
	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: runtimeUpdate,
	}, nil,
		nil,
		cached.UpdateCacheAndDB)
	if err != nil {
		err = errors.Wrap(err, "failed to update job runtime during job kill")
		return
	}

	return jobState, len(runtimeDiffNonTerminatedTasks) > 0, err
}

func stopTasks(
	ctx context.Context,
	cachedJob cached.Job,
	goalStateDriver Driver,
) (
	runtimeDiffNonTerminatedTasks map[uint32]jobmgrcommon.RuntimeDiff,
	allTasksMarked bool,
	err error,
) {
	// Update task runtimes in DB and cache to kill task
	runtimeDiffNonTerminatedTasks, _, runtimeDiffAll, err :=
		createRuntimeDiffForKill(ctx, cachedJob)
	if err != nil {
		return nil, false, err
	}

	_, instancesToBeRetried, err := cachedJob.PatchTasks(ctx, runtimeDiffAll, false)

	// Schedule non terminated tasks in goal state engine.
	// This should happen even if PatchTasks fail, so if part of
	// the tasks are updated successfully, those tasks can be
	// terminated. Otherwise, those tasks would not be enqueued
	// into goal state engine in JobKill retry.
	for instanceID := range runtimeDiffNonTerminatedTasks {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instanceID, time.Now())
	}

	// If patching of few non terminal tasks failed, enqueue
	// the job so that the action is retried
	if len(instancesToBeRetried) != 0 {
		EnqueueJobWithDefaultDelay(cachedJob.ID(), goalStateDriver, cachedJob)
		return runtimeDiffNonTerminatedTasks, false, err
	}

	return runtimeDiffNonTerminatedTasks, true, err
}
