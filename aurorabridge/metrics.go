package aurorabridge

import "github.com/uber-go/tally"

// Metrics is the struct containing all metrics relevant for aurora api parrity
type Metrics struct {
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return nil
}
