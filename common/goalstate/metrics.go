package goalstate

import (
	"github.com/uber-go/tally"
)

// Metrics contains counters to track goal state engine metrics
type Metrics struct {
	// counter to track items not found in goal state engine after dequeue
	missingItems tally.Counter
}

// NewMetrics returns a new Metrics struct.
func NewMetrics(scope tally.Scope) *Metrics {
	goalStateScope := scope.SubScope("goalstate")
	return &Metrics{
		missingItems: goalStateScope.Counter("missing_items"),
	}
}
