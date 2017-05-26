package event

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the task updater.
type Metrics struct {
	RetryFailedTasksTotal tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	subScope := scope.SubScope("status_update")

	return &Metrics{
		RetryFailedTasksTotal: subScope.Counter("retry_failed_total"),
	}
}
