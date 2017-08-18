package task

import (
	"code.uber.internal/infra/peloton/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	ReadyQueueLen tally.Gauge

	TaskLeninTracker tally.Gauge

	pendingTasks   tally.Gauge
	readyTasks     tally.Gauge
	placingTasks   tally.Gauge
	placedTasks    tally.Gauge
	launchingTasks tally.Gauge
	runningTasks   tally.Gauge
	succeededTasks tally.Gauge
	failedTasks    tally.Gauge
	lostTasks      tally.Gauge
	killedTasks    tally.Gauge

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
	successScope := reconcilerScope.Tagged(map[string]string{"type": "success"})
	failScope := reconcilerScope.Tagged(map[string]string{"type": "fail"})
	return &Metrics{
		ReadyQueueLen:    readyScope.Gauge("ready_queue_length"),
		TaskLeninTracker: trackerScope.Gauge("task_len_tracker"),
		pendingTasks:     taskStateScope.Gauge("task_state_pending"),
		readyTasks:       taskStateScope.Gauge("task_state_ready"),
		placingTasks:     taskStateScope.Gauge("task_state_placing"),
		placedTasks:      taskStateScope.Gauge("task_state_placed"),
		launchingTasks:   taskStateScope.Gauge("task_state_launching"),
		runningTasks:     taskStateScope.Gauge("task_state_running"),
		succeededTasks:   taskStateScope.Gauge("task_state_succeeded"),
		failedTasks:      taskStateScope.Gauge("task_state_failed"),
		lostTasks:        taskStateScope.Gauge("task_state_lost"),
		killedTasks:      taskStateScope.Gauge("task_state_killed"),

		LeakedResources:       scalar.NewGaugeMaps(leakScope),
		ReconciliationSuccess: successScope.Counter("run"),
		ReconciliationFail:    failScope.Counter("run"),
	}
}
