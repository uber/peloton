package task

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	ReadyQueueLen tally.Gauge

	TasksCountInTracker tally.Gauge

	TaskStatesGauge map[task.TaskState]tally.Gauge

	LeakedResources scalar.GaugeMaps

	ReconciliationSuccess tally.Counter
	ReconciliationFail    tally.Counter
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
				"task_state_pending"),
			task.TaskState_LOST: taskStateScope.Gauge(
				"task_state_lost"),
			task.TaskState_PREEMPTING: taskStateScope.Gauge(
				"task_state_preempting"),
		},
		LeakedResources:       scalar.NewGaugeMaps(leakScope),
		ReconciliationSuccess: successScope.Counter("run"),
		ReconciliationFail:    failScope.Counter("run"),
	}
}
