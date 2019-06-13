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
	"github.com/uber-go/tally"
)

// JobMetrics contains all counters to track job metrics in goal state engine.
type JobMetrics struct {
	JobCreate           tally.Counter
	JobCreateFailed     tally.Counter
	JobRecoveryDuration tally.Gauge

	JobSucceeded    tally.Counter
	JobKilled       tally.Counter
	JobFailed       tally.Counter
	JobDeleted      tally.Counter
	JobInvalidState tally.Counter

	JobRuntimeUpdated               tally.Counter
	JobRuntimeUpdateFailed          tally.Counter
	JobMaxRunningInstancesExceeding tally.Counter

	JobRecalculateFromCache tally.Counter
}

// TaskMetrics contains all counters to track task metrics in goal state.
type TaskMetrics struct {
	TaskCreate             tally.Counter
	TaskCreateFail         tally.Counter
	TaskRecovered          tally.Counter
	ExecutorShutdown       tally.Counter
	TaskLaunchTimeout      tally.Counter
	TaskInvalidState       tally.Counter
	TaskStartTimeout       tally.Counter
	RetryFailedLaunchTotal tally.Counter
	RetryFailedTasksTotal  tally.Counter
	RetryLostTasksTotal    tally.Counter
}

// UpdateMetrics contains all counters to track
// update metrics in the goal state.
type UpdateMetrics struct {
	UpdateReload            tally.Counter
	UpdateComplete          tally.Counter
	UpdateCompleteFail      tally.Counter
	UpdateUntrack           tally.Counter
	UpdateStart             tally.Counter
	UpdateStartFail         tally.Counter
	UpdateRun               tally.Counter
	UpdateRunFail           tally.Counter
	UpdateWriteProgress     tally.Counter
	UpdateWriteProgressFail tally.Counter
}

// Metrics is the struct containing all the counters that track job and task
// metrics in goal state.
type Metrics struct {
	jobMetrics    *JobMetrics
	taskMetrics   *TaskMetrics
	updateMetrics *UpdateMetrics
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobScope := scope.SubScope("job")
	taskScope := scope.SubScope("task")
	updateScope := scope.SubScope("update")

	jobMetrics := &JobMetrics{
		JobCreate:                       jobScope.Counter("create"),
		JobCreateFailed:                 jobScope.Counter("create_failed"),
		JobRecoveryDuration:             jobScope.Gauge("recovery_duration"),
		JobSucceeded:                    jobScope.Counter("job_succeeded"),
		JobKilled:                       jobScope.Counter("job_killed"),
		JobFailed:                       jobScope.Counter("job_failed"),
		JobDeleted:                      jobScope.Counter("job_deleted"),
		JobInvalidState:                 jobScope.Counter("invalid_state"),
		JobRuntimeUpdated:               jobScope.Counter("runtime_update_success"),
		JobRuntimeUpdateFailed:          jobScope.Counter("runtime_update_fail"),
		JobMaxRunningInstancesExceeding: jobScope.Counter("max_running_instances_exceeded"),
		JobRecalculateFromCache: jobScope.Counter(
			"job_recalculate_from_cache"),
	}

	taskMetrics := &TaskMetrics{
		TaskCreate:             taskScope.Counter("create"),
		TaskCreateFail:         taskScope.Counter("create_fail"),
		TaskRecovered:          taskScope.Counter("recovered"),
		ExecutorShutdown:       taskScope.Counter("executor_shutdown"),
		TaskLaunchTimeout:      taskScope.Counter("launch_timeout"),
		TaskStartTimeout:       taskScope.Counter("start_timeout"),
		TaskInvalidState:       taskScope.Counter("invalid_state"),
		RetryFailedLaunchTotal: taskScope.Counter("retry_system_failure_total"),
		RetryFailedTasksTotal:  taskScope.Counter("retry_failed_total"),
		RetryLostTasksTotal:    taskScope.Counter("retry_lost_total"),
	}

	updateMetrics := &UpdateMetrics{
		UpdateReload:            updateScope.Counter("reload"),
		UpdateComplete:          updateScope.Counter("complete"),
		UpdateCompleteFail:      updateScope.Counter("complete_fail"),
		UpdateUntrack:           updateScope.Counter("untrack"),
		UpdateStart:             updateScope.Counter("start"),
		UpdateStartFail:         updateScope.Counter("start_fail"),
		UpdateRun:               updateScope.Counter("run"),
		UpdateRunFail:           updateScope.Counter("run_fail"),
		UpdateWriteProgress:     updateScope.Counter("write_progress"),
		UpdateWriteProgressFail: updateScope.Counter("write_progress_fail"),
	}

	return &Metrics{
		jobMetrics:    jobMetrics,
		taskMetrics:   taskMetrics,
		updateMetrics: updateMetrics,
	}
}
