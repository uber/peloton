package hostsvc

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in host.svc
type Metrics struct {
	StartMaintenanceAPI     tally.Counter
	StartMaintenanceAPIFail tally.Counter

	CompleteMaintenanceAPI     tally.Counter
	CompleteMaintenanceAPIFail tally.Counter

	QueryHostsAPI     tally.Counter
	QueryHostsAPIFail tally.Counter
}

// NewMetrics returns a new instance of host.svc.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	return &Metrics{
		StartMaintenanceAPI:     successScope.Counter("start_maintenance"),
		StartMaintenanceAPIFail: failScope.Counter("start_maintenance"),

		CompleteMaintenanceAPI:     successScope.Counter("complete_maintenance"),
		CompleteMaintenanceAPIFail: failScope.Counter("complete_maintenance"),

		QueryHostsAPI:     successScope.Counter("query_hosts"),
		QueryHostsAPIFail: failScope.Counter("query_hosts"),
	}
}
