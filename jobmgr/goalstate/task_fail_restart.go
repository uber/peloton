package goalstate

import (
	"context"
	"strings"
	"time"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"

	log "github.com/sirupsen/logrus"
)

// Maximum retries on mesos system failures
const (
	MaxSystemFailureAttempts = 4
)

// isSystemFailure returns true is failure is due to a system failure like
// container launch failure or container terminated with signal broken pipe.
// System failures should be tried MaxSystemFailureAttempts irrespective of
// the maxium retries in the job configuration.
func isSystemFailure(runtime *task.RuntimeInfo) bool {
	if runtime.GetReason() == mesosv1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String() {
		return true
	}

	if runtime.GetReason() == mesosv1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String() {
		if strings.Contains(runtime.GetMessage(), "Container terminated with signal Broken pipe") {
			return true
		}
	}
	return false
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

	taskConfig, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		runtime.GetConfigVersion())
	if err != nil {
		return err
	}

	maxAttempts := taskConfig.GetRestartPolicy().GetMaxFailures()

	if isSystemFailure(runtime) {
		if maxAttempts < MaxSystemFailureAttempts {
			maxAttempts = MaxSystemFailureAttempts
		}
		goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
	}

	if runtime.GetFailureCount() >= maxAttempts {
		// do not retry the task
		return nil
	}

	// reschedule the task
	goalStateDriver.mtx.taskMetrics.RetryFailedTasksTotal.Inc(1)
	runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
		taskEnt.jobID, taskEnt.instanceID, runtime.GetMesosTaskId())
	runtimeDiff[cached.FailureCountField] = runtime.GetFailureCount() + 1
	runtimeDiff[cached.MessageField] = "Rescheduled after task failure"
	log.WithField("job_id", taskEnt.jobID).
		WithField("instance_id", taskEnt.instanceID).
		Debug("restarting failed task")
	err = cachedJob.PatchTasks(ctx,
		map[uint32]cached.RuntimeDiff{taskEnt.instanceID: runtimeDiff})
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
