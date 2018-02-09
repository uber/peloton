package engine

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of archiver engine.
type Metrics struct {
	ArchiverSuccess tally.Counter
	ArchiverFail    tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		ArchiverSuccess: scope.Counter("archiver_success"),
		ArchiverFail:    scope.Counter("archiver_fail"),
	}
}
