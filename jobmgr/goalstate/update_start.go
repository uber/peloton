package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"

	log "github.com/sirupsen/logrus"
)

// fetchUpdateAndJobFromCache is a helper function to fetch the
// update and the job from the cache for a given update entity.
func fetchUpdateAndJobFromCache(
	ctx context.Context,
	updateID *peloton.UpdateID,
	goalStateDriver *driver) (
	cachedUpdate cached.Update, cachedJob cached.Job, err error) {
	// first fetch the update
	cachedUpdate = goalStateDriver.updateFactory.GetUpdate(updateID)
	if cachedUpdate == nil {
		return
	}

	// now lets fetch the job
	jobID := cachedUpdate.JobID()
	cachedJob = goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		// if job has been untracked, cancel the update and then enqueue into
		// goal state to untrack it.
		log.WithFields(
			log.Fields{
				"job_id":    jobID.GetValue(),
				"update_id": updateID.GetValue(),
			}).Info("job has been deleted, so canceling the update as well")
		err = cachedUpdate.Cancel(ctx)
		if err == nil {
			goalStateDriver.EnqueueUpdate(jobID, updateID, time.Now())
		}
	}
	return
}

// handleUnchangedInstancesInUpdate updates the runtime state of the
// instances left unchanged with the given update; essentially, the
// configuration and desired configuration version of all unchanged
// tasks is updated to the newest version.
func handleUnchangedInstancesInUpdate(
	ctx context.Context,
	cachedUpdate cached.Update,
	cachedJob cached.Job,
	jobConfig *job.JobConfig) error {

	runtimes := make(map[uint32]jobmgrcommon.RuntimeDiff)
	instanceCount := jobConfig.GetInstanceCount()
	instancesTotal := cachedUpdate.GetGoalState().Instances

	for i := uint32(0); i < instanceCount; i++ {
		// first find the instances which have not been updated
		found := false
		for _, j := range instancesTotal {
			if i == j {
				// instance has been updated
				found = true
				break
			}
		}

		if found == false {
			// instance is left unchanged with this update
			runtimeDiff := jobmgrcommon.RuntimeDiff{
				jobmgrcommon.ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
				jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
			}
			runtimes[i] = runtimeDiff
		}
	}

	if len(runtimes) > 0 {
		// Just update the runtime of the tasks with the
		// new version and move on.
		if err := cachedJob.PatchTasks(ctx, runtimes); err != nil {
			return err
		}
	}

	return nil
}

// UpdateStart initializes the update. It will move the configuration version
// of the tasks which are not touched by this update to the new version.
// Then it will move the update state to ROLLING_FORWARD and enqueue to
// goal state engine to start the rolling update process.
func UpdateStart(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	log.WithField("update_id", updateEnt.id.GetValue()).
		Info("update starting")

	// fetch the update and job from the cache
	cachedUpdate, cachedJob, err := fetchUpdateAndJobFromCache(
		ctx, updateEnt.id, goalStateDriver)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}
	if cachedUpdate == nil || cachedJob == nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return nil
	}

	jobID := cachedUpdate.JobID()
	// fetch the job configuration first
	jobConfig, systemLabels, err := goalStateDriver.jobStore.GetJobConfigWithVersion(
		ctx,
		jobID,
		cachedUpdate.GetGoalState().JobVersion)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// lets write the new task configs first
	if err := goalStateDriver.taskStore.CreateTaskConfigs(
		ctx,
		jobID,
		jobConfig,
		systemLabels); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// then, update the configuration and desired configuration version of
	// all instances which do not need to be updated
	if err = handleUnchangedInstancesInUpdate(
		ctx, cachedUpdate, cachedJob, jobConfig); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// update the state of the job update
	if err = cachedUpdate.WriteProgress(
		ctx,
		pbupdate.State_ROLLING_FORWARD,
		[]uint32{},
		[]uint32{},
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	goalStateDriver.EnqueueUpdate(jobID, updateEnt.id, time.Now())

	goalStateDriver.mtx.updateMetrics.UpdateStart.Inc(1)
	return nil
}
