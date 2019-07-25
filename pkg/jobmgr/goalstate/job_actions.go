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
	updateutil "github.com/uber/peloton/pkg/jobmgr/util/update"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

var _errTasksNotInCache = yarpcerrors.InternalErrorf("some tasks not in cache")

// JobUntrack deletes the job and tasks from the goal state engine and the cache.
func JobUntrack(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)

	if cachedJob == nil {
		return nil
	}

	jobConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		if !yarpcerrors.IsNotFound(err) {
			// if config is not found, untrack the job from cache
			return err
		}
	} else if jobConfig.GetType() == job.JobType_SERVICE {
		// service jobs are always active and never untracked.
		// Call runtime updater, because job runtime can change
		// when an update is running on the job.
		return JobRuntimeUpdater(ctx, entity)
	}

	// First clean from goal state
	taskMap := cachedJob.GetAllTasks()
	for instID := range taskMap {
		goalStateDriver.DeleteTask(jobEnt.id, instID)
	}
	goalStateDriver.DeleteJob(jobEnt.id)

	// Next clean up from the cache
	goalStateDriver.jobFactory.ClearJob(jobEnt.id)
	return nil
}

// JobStateInvalid dumps a sentry error to indicate that the
// job goal state, state combination is not valid
func JobStateInvalid(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)

	if cachedJob == nil {
		return nil
	}

	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"current_state": jobRuntime.State.String(),
		"goal_state":    jobRuntime.GoalState.String(),
		"job_id":        jobEnt.GetID(),
	}).Error("unexpected job state")
	goalStateDriver.mtx.jobMetrics.JobInvalidState.Inc(1)
	return nil
}

// JobRecover tries to recover a partially created job.
// If job is not recoverable, it would untrack the job
func JobRecover(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.AddJob(jobEnt.id)

	config, err := cachedJob.GetConfig(ctx)

	// config is not created, job cannot be recovered.
	if yarpcerrors.IsNotFound(err) {
		log.WithFields(log.Fields{
			"job_id": jobEnt.GetID(),
		}).Info("job is not recoverable due to missing config")

		runtime, err := cachedJob.GetRuntime(ctx)
		// runtime may already be removed, ignore not found
		// error here
		if err != nil && !yarpcerrors.IsNotFound(err) {
			return err
		}

		// delete from job_index
		if err := goalStateDriver.jobIndexOps.Delete(
			ctx,
			cachedJob.ID(),
		); err != nil {
			return err
		}

		// remove the update
		if len(runtime.GetUpdateID().GetValue()) != 0 {
			if err := goalStateDriver.updateStore.DeleteUpdate(
				ctx,
				runtime.GetUpdateID(),
				cachedJob.ID(),
				runtime.GetConfigurationVersion(),
			); err != nil {
				return err
			}
		}

		if err := goalStateDriver.jobStore.DeleteJob(
			ctx,
			cachedJob.ID().GetValue(),
		); err != nil {
			return err
		}

		// delete from active job in the end, after this step,
		// we would not have any reference to the job.
		if err := goalStateDriver.activeJobsOps.Delete(
			ctx,
			cachedJob.ID(),
		); err != nil {
			return err
		}

		return JobUntrack(ctx, entity)
	}

	if err != nil {
		return err
	}

	// config exists, it means the job is created
	log.WithFields(log.Fields{
		"job_id": jobEnt.GetID(),
	}).Info("job config is found and job is recoverable")

	jobState := job.JobState_INITIALIZED
	if config.GetType() == job.JobType_SERVICE {
		// stateless job uses workflow to create the job,
		// so directly move to PENDING state
		jobState = job.JobState_PENDING
	}
	if err := cachedJob.Update(
		ctx,
		&job.JobInfo{Runtime: &job.RuntimeInfo{State: jobState}},
		nil,
		nil,
		cached.UpdateCacheAndDB); err != nil {
		return err
	}
	goalStateDriver.EnqueueJob(jobEnt.id, time.Now())
	return nil
}

// DeleteJobFromActiveJobs deletes a terminal batch job from active jobs
// table
func DeleteJobFromActiveJobs(
	ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	cfg, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return err
	}

	// delete a terminal batch job from the active jobs table
	if cfg.GetType() == job.JobType_BATCH &&
		util.IsPelotonJobStateTerminal(runtime.GetState()) {
		if err := goalStateDriver.activeJobsOps.Delete(
			ctx, jobEnt.id); err != nil {
			return err
		}
	}
	return nil
}

// JobStart starts all tasks of the job
func JobStart(
	ctx context.Context,
	entity goalstate.Entity,
) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	for i := range cachedJob.GetAllTasks() {
		runtimeDiff[i] = jobmgrcommon.RuntimeDiff{
			jobmgrcommon.GoalStateField: task.TaskState_RUNNING,
		}
	}

	// We do not need to handle 'instancesToBeRetried' here since all tasks
	// in runtimeDiff are being requeued to the goalstate. The action will be
	// retried by the goalstate when the tasks are evaluated next time.
	_, _, err := cachedJob.PatchTasks(ctx, runtimeDiff, false)
	if err != nil {
		return err
	}

	for i := range runtimeDiff {
		goalStateDriver.EnqueueTask(cachedJob.ID(), i, time.Now())
	}

	var jobRuntime *job.RuntimeInfo
	count := 0
	for {
		jobRuntime, err = cachedJob.GetRuntime(ctx)
		if err != nil {
			return err
		}

		jobRuntime.State = job.JobState_PENDING
		jobRuntime.StateVersion = jobRuntime.GetDesiredStateVersion()
		_, err = cachedJob.CompareAndSetRuntime(ctx, jobRuntime)
		if err == jobmgrcommon.UnexpectedVersionError {
			// concurrency error; retry MaxConcurrencyErrorRetry times
			count = count + 1
			if count < jobmgrcommon.MaxConcurrencyErrorRetry {
				continue
			}
		}

		if err != nil {
			return errors.Wrap(err, "fail to update job runtime")
		}

		goalStateDriver.EnqueueJob(cachedJob.ID(), time.Now())
		return nil
	}
}

// JobDelete deletes a job from cache and DB
func JobDelete(
	ctx context.Context,
	entity goalstate.Entity,
) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	err := cachedJob.Delete(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete job from store")
	}

	// Delete job from goalstate and cache
	taskMap := cachedJob.GetAllTasks()
	for instID := range taskMap {
		goalStateDriver.DeleteTask(cachedJob.ID(), instID)
	}
	goalStateDriver.DeleteJob(cachedJob.ID())
	goalStateDriver.jobFactory.ClearJob(cachedJob.ID())

	return nil
}

// JobReloadRuntime reloads the job runtime into the cache
func JobReloadRuntime(
	ctx context.Context,
	entity goalstate.Entity,
) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.AddJob(jobEnt.id)

	jobRuntime, err := goalStateDriver.jobRuntimeOps.Get(
		ctx,
		&peloton.JobID{Value: jobEnt.GetID()},
	)
	if yarpcerrors.IsNotFound(err) {
		// runtime is not created, see if config is created and the job is
		// recoverable.
		return JobRecover(ctx, entity)
	}

	err = cachedJob.Update(
		ctx,
		&job.JobInfo{
			Runtime: jobRuntime,
		}, nil,
		nil,
		cached.UpdateCacheOnly,
	)
	if err != nil {
		return err
	}

	goalStateDriver.EnqueueJob(jobEnt.id, time.Now())

	return nil
}

// EnqueueJobUpdate enqueues an ongoing update, if any,
// for a stateless job. It is noop for batch jobs.
func EnqueueJobUpdate(
	ctx context.Context,
	entity goalstate.Entity,
) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	jobConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return err
	}

	if jobConfig.GetType() != job.JobType_SERVICE {
		return nil
	}

	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	if !updateutil.HasUpdate(jobRuntime) {
		return nil
	}

	updateInfo, err := goalStateDriver.updateStore.GetUpdateProgress(ctx, jobRuntime.GetUpdateID())
	if err != nil {
		return err
	}

	if !cached.IsUpdateStateTerminal(updateInfo.GetState()) {
		goalStateDriver.EnqueueUpdate(jobEnt.id, jobRuntime.GetUpdateID(), time.Now())
	}
	return nil
}

// JobKillAndDelete terminates each task in job, and makes
// sure tasks would not get restarted. It deletes the job
// if all tasks are in terminated states.
func JobKillAndDelete(
	ctx context.Context,
	entity goalstate.Entity,
) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	jobState, _, err :=
		killJob(ctx, cachedJob, goalStateDriver)
	if err != nil {
		return err
	}

	// job is in terminal state now, can safely delete the job
	if util.IsPelotonJobStateTerminal(jobState) {
		log.WithField("job_id", cachedJob.ID()).
			Info("all tasks are killed, deleting the job")
		return JobDelete(ctx, entity)
	}

	log.WithFields(log.Fields{
		"job_id": cachedJob.ID(),
		"state":  jobState.String(),
	}).Info("some tasks are not killed, waiting to kill before deleting")

	return nil
}

// JobKillAndUntrack kills all of the pods in the job, makes sure they would
// not start again and untrack the job if possible
func JobKillAndUntrack(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}

	runtimeDiffNonTerminatedTasks, _, err :=
		stopTasks(ctx, cachedJob, goalStateDriver)
	if err != nil {
		return err
	}

	if len(runtimeDiffNonTerminatedTasks) == 0 {
		log.WithField("job_id", cachedJob.ID()).
			Info("all tasks are killed, untrack the job")
		return JobUntrack(ctx, entity)
	}

	log.WithFields(log.Fields{
		"job_id": cachedJob.ID(),
	}).Info("some tasks are not killed, waiting to kill before untracking")

	return nil
}
