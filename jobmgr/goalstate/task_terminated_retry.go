package goalstate

import (
	"code.uber.internal/infra/peloton/common/goalstate"
	"context"
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

	taskConfig, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		taskRuntime.GetConfigVersion())
	if err != nil {
		return err
	}

	// Check Whether task retry is in an update
	updateID := jobRuntime.GetUpdateID()
	if updateID != nil {
		cachedUpdate := goalStateDriver.updateFactory.GetUpdate(updateID)
		// If the task is in the progress of update, we will do a retry with max retry attemps
		if cachedUpdate != nil &&
			cachedUpdate.IsTaskInUpdateProgress(taskEnt.instanceID) {

			maxAttempts := cachedUpdate.GetUpdateConfig().GetMaxInstanceRetries()
			if isSystemFailure(taskRuntime) {
				if maxAttempts < MaxSystemFailureAttempts {
					maxAttempts = MaxSystemFailureAttempts
				}
				goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
			}
			// If the current faliure count is larger then max retry attemps, we give up retry
			if taskRuntime.GetFailureCount() >= maxAttempts {
				log.
					WithField("jobID", taskEnt.jobID).
					WithField("instanceID", taskEnt.instanceID).
					WithField("failureCount", taskRuntime.GetFailureCount()).
					WithField("maxAttemps", maxAttempts).
					Debug("failureCount larger than maxAttemps, give up retry")
				return nil
			}
		}
	}

	return rescheduleTask(
		ctx,
		cachedJob,
		taskEnt.instanceID,
		taskRuntime,
		taskConfig,
		goalStateDriver)
}
