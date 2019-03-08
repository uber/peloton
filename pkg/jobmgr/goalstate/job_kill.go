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

	log "github.com/sirupsen/logrus"
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

	// Get job runtime and update job state to killing
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime during job kill")
		return err
	}

	// Update task runtimes in DB and cache to kill task
	runtimeDiffNonTerminatedTasks, _, runtimeDiffAll, err :=
		createRuntimeDiffForKill(ctx, cachedJob)
	if err != nil {
		return err
	}

	config, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job config")
		return err
	}

	err = cachedJob.PatchTasks(ctx, runtimeDiffAll)

	// Schedule non terminated tasks in goal state engine.
	// This should happen even if PatchTasks fail, so if part of
	// the tasks are updated successfully, those tasks can be
	// terminated. Otherwise, those tasks would not be enqueued
	// into goal state engine in JobKill retry.
	for instanceID := range runtimeDiffNonTerminatedTasks {
		goalStateDriver.EnqueueTask(jobID, instanceID, time.Now())
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update task runtimes to kill a job")
		return err
	}

	// Only enqueue the job into goal state if any of the
	// non terminated tasks need to be killed.
	if len(runtimeDiffNonTerminatedTasks) > 0 {
		EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
	}

	jobState := calculateJobState(
		ctx,
		cachedJob,
		config,
		jobRuntime,
		runtimeDiffNonTerminatedTasks,
	)
	// update job sate as well as state version,
	// once state version == desired state version,
	// goal state engine knows that the all the tasks
	// are being sent to task goal state engine to kill and
	// no further action is needed.
	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: &job.RuntimeInfo{
			State:        jobState,
			StateVersion: jobRuntime.DesiredStateVersion,
		},
	}, nil,
		cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update job runtime during job kill")
		return err
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
	config jobmgrcommon.JobConfig,
	jobRuntime *job.RuntimeInfo,
	runtimeDiffNonTerminatedTasks map[uint32]jobmgrcommon.RuntimeDiff) job.JobState {
	// If not all instances have been created,
	// and all instances to be killed are already in terminal state,
	// then directly update the job state to KILLED.
	// Partially create batch job can be killed only on INITIALIZED state.
	// Partially created stateless job can be killed on
	// INITIALIZED & PENDING states.
	if len(runtimeDiffNonTerminatedTasks) == 0 &&
		cachedJob.IsPartiallyCreated(config) {

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
