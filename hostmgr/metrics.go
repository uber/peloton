package hostmgr

import (
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	LaunchTasks              tally.Counter
	LaunchTasksFail          tally.Counter
	LaunchTasksInvalid       tally.Counter
	LaunchTasksInvalidOffers tally.Counter

	AcquireHostOffers        tally.Counter
	AcquireHostOffersInvalid tally.Counter

	KillTasks     tally.Counter
	KillTasksFail tally.Counter

	ReleaseHostOffers tally.Counter

	Elected         tally.Gauge
	MesosConnected  tally.Gauge
	HandlersRunning tally.Gauge

	StartMesosLoopFail tally.Counter

	ClusterCapacity     tally.Counter
	ClusterCapacityFail tally.Counter

	OfferOperations              tally.Counter
	OfferOperationsFail          tally.Counter
	OfferOperationsInvalid       tally.Counter
	OfferOperationsInvalidOffers tally.Counter

	scope tally.Scope
}

// NewMetrics returns a new instance of hostmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	serverScope := scope.SubScope("server")
	return &Metrics{
		LaunchTasks:              scope.Counter("launch_tasks"),
		LaunchTasksFail:          scope.Counter("launch_tasks_fail"),
		LaunchTasksInvalid:       scope.Counter("launch_tasks_invalid"),
		LaunchTasksInvalidOffers: scope.Counter("launch_tasks_invalid_offers"),

		OfferOperations:              scope.Counter("offer_operations"),
		OfferOperationsFail:          scope.Counter("offer_operations_fail"),
		OfferOperationsInvalid:       scope.Counter("offer_operations_invalid"),
		OfferOperationsInvalidOffers: scope.Counter("offer_operations_invalid_offers"),

		AcquireHostOffers:        scope.Counter("acquire_host_offers"),
		AcquireHostOffersInvalid: scope.Counter("acquire_host_offers_invalid"),

		KillTasks:     scope.Counter("kill_tasks"),
		KillTasksFail: scope.Counter("kill_tasks_fail"),

		ReleaseHostOffers: scope.Counter("release_host_offers"),

		Elected:         serverScope.Gauge("elected"),
		MesosConnected:  serverScope.Gauge("mesos_connected"),
		HandlersRunning: serverScope.Gauge("handlers_running"),

		StartMesosLoopFail: scope.Counter("start_mesos_loop_fail"),

		ClusterCapacity:     scope.Counter("cluster_capacity"),
		ClusterCapacityFail: scope.Counter("cluster_capacity_fail"),

		scope: scope,
	}
}

func (m *Metrics) refreshClusterCapacityGauges(response *hostsvc.ClusterCapacityResponse) {
	for _, resource := range response.GetResources() {
		if len(resource.GetKind()) == 0 {
			continue
		}

		gauge := m.scope.Gauge("cluster_capacity_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}
}
