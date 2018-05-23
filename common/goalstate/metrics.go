package goalstate

import (
	"github.com/uber-go/tally"
)

// Metrics contains counters to track goal state engine metrics
type Metrics struct {
	// the metrics scope for goal state engine
	scope tally.Scope
	// counter to track items not found in goal state engine after dequeue
	missingItems tally.Counter
	// counter to track total items in the goal state engine
	totalItems tally.Gauge
}

// NewMetrics returns a new Metrics struct.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		scope:        scope,
		missingItems: scope.Counter("missing_items"),
		totalItems:   scope.Gauge("total_items"),
	}
}
