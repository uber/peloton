package host

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in host.
type Metrics struct {
	HostDrainSuccess tally.Counter
	HostDrainFail    tally.Counter
}

// NewMetrics returns a new instance of host.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	hostSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	hostFailScope := scope.Tagged(map[string]string{"type": "fail"})
	return &Metrics{
		HostDrainSuccess: hostSuccessScope.Counter("host_drain"),
		HostDrainFail:    hostFailScope.Counter("host_drain"),
	}
}
