package event

import (
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/mesos/v1"
)

// Metrics is the struct containing all the counters that track
// internal state of the task updater.
type Metrics struct {
	RetryFailedTasksTotal  tally.Counter
	RetryFailedLaunchTotal tally.Counter

	SkipOrphanTasksTotal tally.Counter

	TasksFailedTotal    tally.Counter
	TasksLostTotal      tally.Counter
	TasksKilledTotal    tally.Counter
	TasksSucceededTotal tally.Counter
	TasksRunningTotal   tally.Counter
	TasksLaunchedTotal  tally.Counter
	TasksStartingTotal  tally.Counter

	TasksHealthyTotal   tally.Counter
	TasksUnHealthyTotal tally.Counter

	TasksReconciledTotal tally.Counter
	TasksFailedReason    map[int32]tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		RetryFailedTasksTotal:  scope.Counter("retry_failed_total"),
		RetryFailedLaunchTotal: scope.Counter("retry_system_failure_total"),

		SkipOrphanTasksTotal: scope.Counter("skip_orphan_task_total"),

		TasksFailedTotal:    scope.Counter("tasks_failed_total"),
		TasksLostTotal:      scope.Counter("tasks_lost_total"),
		TasksSucceededTotal: scope.Counter("tasks_succeeded_total"),
		TasksKilledTotal:    scope.Counter("tasks_killed_total"),
		TasksRunningTotal:   scope.Counter("tasks_running_total"),
		TasksLaunchedTotal:  scope.Counter("tasks_launched_total"),
		TasksStartingTotal:  scope.Counter("tasks_starting_total"),

		TasksHealthyTotal:   scope.Counter("tasks_healthy_total"),
		TasksUnHealthyTotal: scope.Counter("tasks_unhealthy_total"),

		TasksReconciledTotal: scope.Counter("tasks_reconciled_total"),
		TasksFailedReason:    newTasksFailedReasonScope(scope),
	}
}

// newTasksFailedReasonScope creates a map of task failed reason to counter
func newTasksFailedReasonScope(scope tally.Scope) map[int32]tally.Counter {
	var taggedScopes = make(map[int32]tally.Counter)
	for reasonID, reasonName := range mesos_v1.TaskStatus_Reason_name {
		taggedScopes[reasonID] = scope.Tagged(map[string]string{"reason": reasonName}).Counter("task_failed")
	}
	return taggedScopes
}
