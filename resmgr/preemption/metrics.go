package preemption

import (
	"code.uber.internal/infra/peloton/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in preemption
type Metrics struct {
	RevocableRunningTasksToPreempt    tally.Counter
	RevocableNonRunningTasksToPreempt tally.Counter

	NonRevocableRunningTasksToPreempt    tally.Counter
	NonRevocableNonRunningTasksToPreempt tally.Counter

	PreemptionQueueSize tally.Gauge
	TasksToEvict        tally.Gauge

	TasksFailedPreemption tally.Counter

	NonSlackTotalResourcesToFree          scalar.CounterMaps
	NonSlackNonRunningTasksResourcesFreed scalar.CounterMaps
	NonSlackRunningTasksResourcesToFreed  scalar.CounterMaps

	SlackTotalResourcesToFree          scalar.CounterMaps
	SlackNonRunningTasksResourcesFreed scalar.CounterMaps
	SlackRunningTasksResourcesToFreed  scalar.CounterMaps

	OverAllocationCount tally.Gauge
}

// NewMetrics returns a new instance of preemption.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		RevocableRunningTasksToPreempt:    scope.Counter("revocable_running_tasks"),
		RevocableNonRunningTasksToPreempt: scope.Counter("revocable_non_running_tasks"),

		NonRevocableRunningTasksToPreempt:    scope.Counter("non_revocable_running_tasks"),
		NonRevocableNonRunningTasksToPreempt: scope.Counter("non_revocable_non_running_tasks"),

		TasksFailedPreemption: scope.Counter("num_tasks_failed"),

		PreemptionQueueSize: scope.Gauge("preemption_queue_size"),
		TasksToEvict:        scope.Gauge("tasks_to_evict"),

		NonSlackTotalResourcesToFree:          scalar.NewCounterMaps(scope.SubScope("non_slack_total_resources_to_free")),
		NonSlackNonRunningTasksResourcesFreed: scalar.NewCounterMaps(scope.SubScope("non_slack_non_running_tasks_resources_freed")),
		NonSlackRunningTasksResourcesToFreed:  scalar.NewCounterMaps(scope.SubScope("non_slack_running_tasks_resources_freed")),

		SlackTotalResourcesToFree:          scalar.NewCounterMaps(scope.SubScope("slack_total_resources_to_free")),
		SlackNonRunningTasksResourcesFreed: scalar.NewCounterMaps(scope.SubScope("slack_non_running_tasks_resources_freed")),
		SlackRunningTasksResourcesToFreed:  scalar.NewCounterMaps(scope.SubScope("slack_running_tasks_resources_freed")),

		OverAllocationCount: scope.Gauge("over_allocation_count"),
	}
}
