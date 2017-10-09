package preemption

import (
	"code.uber.internal/infra/peloton/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in preemption
type Metrics struct {
	RunningTasksPreempted    tally.Counter
	NonRunningTasksPreempted tally.Counter
	TasksFailedPreemption    tally.Counter

	ResourcesFreed scalar.GaugeMaps

	OverAllocationCount tally.Gauge
}

// NewMetrics returns a new instance of preemption.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		RunningTasksPreempted:    scope.Counter("num_running_tasks"),
		NonRunningTasksPreempted: scope.Counter("num_non_running_tasks"),
		TasksFailedPreemption:    scope.Counter("num_tasks_failed"),

		ResourcesFreed: scalar.NewGaugeMaps(scope.SubScope("resources")),

		OverAllocationCount: scope.Gauge("over_allocation_count"),
	}
}
