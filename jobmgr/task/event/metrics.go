package event

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the task updater.
type Metrics struct {
	RetryFailedTasksTotal  tally.Counter
	RetryFailedLaunchTotal tally.Counter
	RetryLostTasksTotal    tally.Counter

	SkipOrphanTasksTotal tally.Counter

	TasksFailedTotal    tally.Counter
	TasksLostTotal      tally.Counter
	TasksKilledTotal    tally.Counter
	TasksSucceededTotal tally.Counter
	TasksRunningTotal   tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		RetryFailedTasksTotal:  scope.Counter("retry_failed_total"),
		RetryFailedLaunchTotal: scope.Counter("retry_system_failure_total"),
		RetryLostTasksTotal:    scope.Counter("retry_lost_total"),

		SkipOrphanTasksTotal: scope.Counter("skip_orphan_task_total"),

		TasksFailedTotal:    scope.Counter("tasks_failed_total"),
		TasksLostTotal:      scope.Counter("tasks_lost_total"),
		TasksSucceededTotal: scope.Counter("tasks_succeeded_total"),
		TasksKilledTotal:    scope.Counter("tasks_killed_total"),
		TasksRunningTotal:   scope.Counter("tasks_running_total"),
	}
}
