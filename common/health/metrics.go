package health

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in health package.
type Metrics struct {
	Init      tally.Counter
	Heartbeat tally.Gauge
	Leader    tally.Gauge
}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		Init:      scope.Counter("init"),
		Heartbeat: scope.Gauge("heartbeat"),
		Leader:    scope.Gauge("leader"),
	}
}
