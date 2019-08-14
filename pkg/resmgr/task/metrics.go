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

package task

import (
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	ReadyQueueLen tally.Gauge

	TasksCountInTracker tally.Gauge

	TaskStatesGauge map[task.TaskState]tally.Gauge

	ResourcesHeldByTaskState map[task.TaskState]scalar.GaugeMaps
	LeakedResources          scalar.GaugeMaps

	ReconciliationSuccess tally.Counter
	ReconciliationFail    tally.Counter

	OrphanTasks tally.Gauge
}

// NewMetrics returns a new instance of task.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	readyScope := scope.SubScope("ready")
	trackerScope := scope.SubScope("tracker")
	taskStateScope := scope.SubScope("tasks_state")

	reconcilerScope := scope.SubScope("reconciler")
	leakScope := reconcilerScope.SubScope("leaks")
	successScope := reconcilerScope.Tagged(map[string]string{"result": "success"})
	failScope := reconcilerScope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		ReadyQueueLen:       readyScope.Gauge("ready_queue_length"),
		TasksCountInTracker: trackerScope.Gauge("task_len_tracker"),
		TaskStatesGauge: map[task.TaskState]tally.Gauge{
			task.TaskState_PENDING: taskStateScope.Gauge(
				"task_state_pending"),
			task.TaskState_READY: taskStateScope.Gauge(
				"task_state_ready"),
			task.TaskState_PLACING: taskStateScope.Gauge(
				"task_state_placing"),
			task.TaskState_PLACED: taskStateScope.Gauge(
				"task_state_placed"),
			task.TaskState_LAUNCHING: taskStateScope.Gauge(
				"task_state_launching"),
			task.TaskState_LAUNCHED: taskStateScope.Gauge(
				"task_state_launched"),
			task.TaskState_RUNNING: taskStateScope.Gauge(
				"task_state_running"),
			task.TaskState_SUCCEEDED: taskStateScope.Gauge(
				"task_state_succeeded"),
			task.TaskState_FAILED: taskStateScope.Gauge(
				"task_state_failed"),
			task.TaskState_KILLED: taskStateScope.Gauge(
				"task_state_killed"),
			task.TaskState_LOST: taskStateScope.Gauge(
				"task_state_lost"),
			task.TaskState_PREEMPTING: taskStateScope.Gauge(
				"task_state_preempting"),
		},
		ResourcesHeldByTaskState: map[task.TaskState]scalar.GaugeMaps{
			task.TaskState_READY: scalar.NewGaugeMaps(
				scope.SubScope("task_state_ready"),
			),
			task.TaskState_PLACING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_placing"),
			),
			task.TaskState_PLACED: scalar.NewGaugeMaps(
				scope.SubScope("task_state_placed"),
			),
			task.TaskState_LAUNCHING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_launching"),
			),
			task.TaskState_LAUNCHED: scalar.NewGaugeMaps(
				scope.SubScope("task_state_launched"),
			),
			task.TaskState_RUNNING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_running"),
			),
			task.TaskState_STARTING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_starting"),
			),
			task.TaskState_PREEMPTING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_preempting"),
			),
			task.TaskState_KILLING: scalar.NewGaugeMaps(
				scope.SubScope("task_state_killing"),
			),
		},
		LeakedResources:       scalar.NewGaugeMaps(leakScope),
		ReconciliationSuccess: successScope.Counter("run"),
		ReconciliationFail:    failScope.Counter("run"),
		OrphanTasks:           scope.Gauge("orphan_tasks"),
	}
}
