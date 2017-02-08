package hostmgr

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	LaunchTasks        tally.Counter
	LaunchTasksFail    tally.Counter
	LaunchTasksInvalid tally.Counter
}

// NewMetrics returns a new instance of hostmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		LaunchTasks:        scope.Counter("launch_tasks"),
		LaunchTasksFail:    scope.Counter("launch_tasks_fail"),
		LaunchTasksInvalid: scope.Counter("launch_tasks_invalid"),
	}
}
