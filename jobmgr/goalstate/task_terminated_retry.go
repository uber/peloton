package goalstate

import (
	"context"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/goalstate"

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
			goalStateDriver)
	}

	return nil
}

// shouldTaskRetry returns whether a terminated task should retry given its
// MaxInstanceRetries config
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
			!cachedUpdate.IsTaskInUpdateProgress(instanceID) ||
			taskRuntime.GetFailureCount() == 0 {
			return true
		}

		// the task is in the progress of update, will do a retry with max retry attempts
		maxAttempts := cachedUpdate.GetUpdateConfig().GetMaxInstanceRetries()
		if isSystemFailure(taskRuntime) {
			if maxAttempts < MaxSystemFailureAttempts {
				maxAttempts = MaxSystemFailureAttempts
			}
			goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
		}

		// failureRetryCount is failure count - 1, because JobMgr would only
		// retry for failure after a failure happens. Therefore, failure count
		// is always one larger then failureRetryCount. For example, when failure
		// count is 1, it means JobMgr has never done a failure retry.
		failureRetryCount := taskRuntime.GetFailureCount() - 1
		// If the current failure retry count has reached the maxAttempts, we give up retry
		if failureRetryCount >= maxAttempts {
			log.
				WithField("jobID", jobID.GetValue()).
				WithField("instanceID", instanceID).
				WithField("failureCount", taskRuntime.GetFailureCount()).
				WithField("maxAttemps", maxAttempts).
				Debug("failureCount larger than maxAttemps, give up retry")
			return false
		}
	}
	return true
}
