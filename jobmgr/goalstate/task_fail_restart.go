package goalstate

import (
	"context"
	"math"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"

	log "github.com/sirupsen/logrus"
)

// rescheduleTask patch the new job runtime and enqueue the task into goalstate engine
// When JobMgr restarts, the task would be throttled again. Therefore, a task can be throttled
// for more than the duration returned by getBackoff.
func rescheduleTask(
	ctx context.Context,
	cachedJob cached.Job,
	instanceID uint32,
	taskRuntime *task.RuntimeInfo,
	taskConfig *task.TaskConfig,
	goalStateDriver *driver,
	throttleOnFailure bool) error {

	jobID := cachedJob.ID()
	healthState := taskutil.GetInitialHealthState(taskConfig)
	// reschedule the task
	goalStateDriver.mtx.taskMetrics.RetryFailedTasksTotal.Inc(1)
	runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
		jobID,
		instanceID,
		taskRuntime,
		healthState)
	runtimeDiff[jobmgrcommon.MessageField] = "Rescheduled after task terminated"
	log.WithField("job_id", jobID).
		WithField("instance_id", instanceID).
		Debug("restarting terminated task")
	err := cachedJob.PatchTasks(ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{instanceID: runtimeDiff})
	if err != nil {
		return err
	}
	if throttleOnFailure {
		goalStateDriver.EnqueueTask(
			jobID,
			instanceID,
			time.Now().Add(getBackoff(taskRuntime,
				goalStateDriver.cfg.InitialTaskBackoff,
				goalStateDriver.cfg.MaxTaskBackoff)))
	} else {
		goalStateDriver.EnqueueTask(jobID, instanceID, time.Now())
	}

	EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
	return nil
}

func getBackoff(taskRuntime *task.RuntimeInfo,
	initialTaskBackOff time.Duration,
	maxTaskBackOff time.Duration) time.Duration {
	if taskRuntime.GetFailureCount() == 0 {
		return time.Duration(0)
	}

	// backOff = _initialTaskBackOff * 2 ^ (failureCount - 1)
	backOff := time.Duration(float64(initialTaskBackOff.Nanoseconds()) *
		math.Pow(2, float64(taskRuntime.GetFailureCount()-1)))

	if backOff > maxTaskBackOff {
		return maxTaskBackOff
	}
	return backOff
}

// TaskFailRetry retries on task failure
func TaskFailRetry(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
		}).Error("task is nil in cache with valid job")
		return nil
	}

	runtime, err := cachedTask.GetRunTime(ctx)
	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		runtime.GetConfigVersion())
	if err != nil {
		return err
	}

	maxAttempts := taskConfig.GetRestartPolicy().GetMaxFailures()

	if taskutil.IsSystemFailure(runtime) {
		if maxAttempts < jobmgrcommon.MaxSystemFailureAttempts {
			maxAttempts = jobmgrcommon.MaxSystemFailureAttempts
		}
		goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
	}

	if runtime.GetFailureCount() >= maxAttempts {
		// do not retry the task
		return nil
	}

	return rescheduleTask(
		ctx,
		cachedJob,
		taskEnt.instanceID,
		runtime,
		taskConfig,
		goalStateDriver,
		false)
}
