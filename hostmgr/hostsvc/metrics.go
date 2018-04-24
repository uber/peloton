package hostsvc

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in host.svc
type Metrics struct {
	StartMaintenanceAPI     tally.Counter
	StartMaintenanceSuccess tally.Counter
	StartMaintenanceFail    tally.Counter

	CompleteMaintenanceAPI     tally.Counter
	CompleteMaintenanceSuccess tally.Counter
	CompleteMaintenanceFail    tally.Counter

	QueryHostsAPI     tally.Counter
	QueryHostsSuccess tally.Counter
	QueryHostsFail    tally.Counter
}

// NewMetrics returns a new instance of host.svc.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	apiScope := scope.SubScope("api")
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	return &Metrics{
		StartMaintenanceAPI:     apiScope.Counter("start_maintenance"),
		StartMaintenanceSuccess: successScope.Counter("start_maintenance"),
		StartMaintenanceFail:    failScope.Counter("start_maintenance"),

		CompleteMaintenanceAPI:     apiScope.Counter("complete_maintenance"),
		CompleteMaintenanceSuccess: successScope.Counter("complete_maintenance"),
		CompleteMaintenanceFail:    failScope.Counter("complete_maintenance"),

		QueryHostsAPI:     apiScope.Counter("query_hosts"),
		QueryHostsSuccess: successScope.Counter("query_hosts"),
		QueryHostsFail:    failScope.Counter("query_hosts"),
	}
}
