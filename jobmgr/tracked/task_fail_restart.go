package tracked

import (
	"context"
	"strings"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

// Maximum retries on mesos system failures
const (
	MaxSystemFailureAttempts = 4
)

func (t *task) isSystemFailure(runtime *pb_task.RuntimeInfo) bool {
	if runtime.GetReason() == mesos_v1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String() {
		return true
	}

	if runtime.GetReason() == mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String() {
		if strings.Contains(runtime.GetMessage(), "Container terminated with signal Broken pipe") {
			return true
		}
	}
	return false
}

func (t *task) failureRetry(ctx context.Context) (bool, error) {
	runtime := t.GetRunTime()

	taskConfig, err := t.job.m.taskStore.GetTaskConfig(ctx, t.job.ID(), t.ID(), int64(runtime.GetConfigVersion()))
	if err != nil {
		return true, err
	}

	maxAttempts := taskConfig.GetRestartPolicy().GetMaxFailures()

	if t.isSystemFailure(runtime) {
		if maxAttempts < MaxSystemFailureAttempts {
			maxAttempts = MaxSystemFailureAttempts
		}
		t.job.m.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
	}

	if runtime.GetFailureCount() >= maxAttempts {
		return false, nil
	}

	t.job.m.mtx.taskMetrics.RetryFailedTasksTotal.Inc(1)
	util.RegenerateMesosTaskID(t.job.ID(), t.ID(), runtime)
	runtime.FailureCount++
	runtime.Message = "Rescheduled after task failure"
	log.WithField("job_id", t.job.ID()).
		WithField("instance_id", t.ID()).
		Debug("restarting failed task")
	return true, t.job.m.UpdateTaskRuntime(ctx, t.job.ID(), t.ID(), runtime, UpdateAndSchedule)
}
