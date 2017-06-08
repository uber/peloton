package task

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	ReadyQueueLen tally.Gauge
}

// NewMetrics returns a new instance of task.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	readyScope := scope.SubScope("ready")
	return &Metrics{
		ReadyQueueLen: readyScope.Gauge("ready_queue_length"),
	}
}
