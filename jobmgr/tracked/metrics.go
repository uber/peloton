package tracked

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of tracked manager.
type metrics struct {
	scope         tally.Scope
	queueLength   tally.Gauge
	queuePopDelay tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func newMetrics(scope tally.Scope) *metrics {
	queueScope := scope.SubScope("queue")
	return &metrics{
		scope:         scope,
		queueLength:   queueScope.Gauge("length"),
		queuePopDelay: queueScope.Timer("pop_delay"),
	}
}
