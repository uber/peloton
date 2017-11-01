package goalstate

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of goalstate keeper.
type Metrics struct {
	IsLeader         tally.Gauge
	TaskActions      tally.Counter
	TaskActionErrors tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		IsLeader:         scope.Gauge("is_leader"),
		TaskActions:      scope.Counter("task_actions"),
		TaskActionErrors: scope.Counter("task_action_errors"),
	}
}
