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
	"math"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"

	log "github.com/sirupsen/logrus"
)

const (
	_rescheduleMessage = "Rescheduled after task terminated"
	_throttleMessage   = "Task throttled due to failure"
)

// rescheduleTask patch the new job runtime and enqueue the task into goalstate engine
// When JobMgr restarts, the task would be throttled again. Therefore, a task can be throttled
// for more than the duration returned by getBackoff.
func rescheduleTask(
	ctx context.Context,
	cachedJob cached.Job,
	cachedTask cached.Task,
	taskRuntime *task.RuntimeInfo,
	taskConfig *task.TaskConfig,
	goalStateDriver *driver,
	throttleOnFailure bool) error {

	jobID := cachedJob.ID()
	healthState := taskutil.GetInitialHealthState(taskConfig)
	// reschedule the task
	if taskRuntime.GetState() == task.TaskState_LOST {
		goalStateDriver.mtx.taskMetrics.RetryLostTasksTotal.Inc(1)
	} else {
		goalStateDriver.mtx.taskMetrics.RetryFailedTasksTotal.Inc(1)
	}

	var runtimeDiff jobmgrcommon.RuntimeDiff
	scheduleDelay := getScheduleDelay(
		cachedTask,
		taskRuntime,
		goalStateDriver.cfg.InitialTaskBackoff,
		goalStateDriver.cfg.MaxTaskBackoff,
		throttleOnFailure,
	)

	if scheduleDelay <= time.Duration(0) {
		// scheduleDelay is negative, which means the task
		// should have been scheduled. Reinit the task right away.
		runtimeDiff = taskutil.RegenerateMesosTaskIDDiff(
			jobID,
			cachedTask.ID(),
			taskRuntime,
			healthState)
		runtimeDiff[jobmgrcommon.MessageField] = _rescheduleMessage
		log.WithField("job_id", jobID).
			WithField("instance_id", cachedTask.ID()).
			Debug("restarting terminated task")
	} else if taskRuntime.GetMessage() != _throttleMessage {
		// only update the message when the throttled task enters
		// this func for the first time
		runtimeDiff = jobmgrcommon.RuntimeDiff{
			jobmgrcommon.MessageField: _throttleMessage,
		}
	}

	if len(runtimeDiff) != 0 {
		err := cachedJob.PatchTasks(ctx,
			map[uint32]jobmgrcommon.RuntimeDiff{cachedTask.ID(): runtimeDiff})
		if err != nil {
			return err
		}
	}

	goalStateDriver.EnqueueTask(jobID, cachedTask.ID(), time.Now().Add(scheduleDelay))
	EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)

	return nil
}

// getScheduleDelay returns how much delay
// the task should be scheduled after.
// zero or negative value means no delay,
// and the task should be rescheduled immediately
func getScheduleDelay(
	cachedTask cached.Task,
	taskRuntime *task.RuntimeInfo,
	initialTaskBackOff time.Duration,
	maxTaskBackOff time.Duration,
	throttleOnFailure bool,
) time.Duration {
	if !throttleOnFailure {
		return time.Duration(0)
	}

	backOff := getBackoff(taskRuntime, initialTaskBackOff, maxTaskBackOff)
	ddl := cachedTask.GetLastRuntimeUpdateTime().Add(backOff)

	return ddl.Sub(time.Now())
}

func getBackoff(
	taskRuntime *task.RuntimeInfo,
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

	runtime, err := cachedTask.GetRuntime(ctx)
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
		cachedTask,
		runtime,
		taskConfig,
		goalStateDriver,
		false)
}
