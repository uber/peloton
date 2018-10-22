package goalstate

import (
	"context"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"
	updateutil "code.uber.internal/infra/peloton/jobmgr/util/update"

	log "github.com/sirupsen/logrus"
)

// TaskTerminatedRetry retries on task that is terminated
func TaskTerminatedRetry(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	// TODO: use jobFactory.AddJob after GetJob and AddJob get cleaned up
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	cachedTask, err := cachedJob.AddTask(ctx, taskEnt.instanceID)
	if err != nil {
		return err
	}

	taskRuntime, err := cachedTask.GetRunTime(ctx)
	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		taskRuntime.GetConfigVersion())
	if err != nil {
		return err
	}

	if shouldTaskRetry(
		taskEnt.jobID,
		taskEnt.instanceID,
		jobRuntime,
		taskRuntime,
		goalStateDriver,
	) {
		return rescheduleTask(
			ctx,
			cachedJob,
			taskEnt.instanceID,
			taskRuntime,
			taskConfig,
			goalStateDriver,
			true)
	}

	return nil
}

// shouldTaskRetry returns whether a terminated task should retry given its
// MaxInstanceAttempts config
func shouldTaskRetry(
	jobID *peloton.JobID,
	instanceID uint32,
	jobRuntime *pbjob.RuntimeInfo,
	taskRuntime *pbtask.RuntimeInfo,
	goalStateDriver *driver,
) bool {
	// Check Whether task retry is in an update
	updateID := jobRuntime.GetUpdateID()
	if updateID != nil {
		cachedUpdate := goalStateDriver.updateFactory.GetUpdate(updateID)

		// directly retry when there is no update going on, or no failure
		// has occurred
		if cachedUpdate == nil ||
			!cachedUpdate.IsTaskInUpdateProgress(instanceID) {
			return true
		}
		if taskutil.IsSystemFailure(taskRuntime) {
			goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
		}

		// If the current failure retry count has reached the maxAttempts, we give up retry
		if updateutil.HasFailedUpdate(
			taskRuntime,
			cachedUpdate.GetUpdateConfig().GetMaxInstanceAttempts()) {
			log.
				WithField("jobID", jobID.GetValue()).
				WithField("instanceID", instanceID).
				WithField("failureCount", taskRuntime.GetFailureCount()).
				Debug("failureCount larger than max attempts, give up retry")
			return false
		}
	}
	return true
}
