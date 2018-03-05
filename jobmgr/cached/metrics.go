package cached

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state of the cache.
type Metrics struct {
	scope tally.Scope
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		scope: scope,
	}
}
