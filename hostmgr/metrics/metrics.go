package metrics

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
	AcquireHostOffersCount   tally.Counter

	GetHosts        tally.Counter
	GetHostsInvalid tally.Counter
	GetHostsCount   tally.Counter

	KillTasks     tally.Counter
	KillTasksFail tally.Counter

	ShutdownExecutors        tally.Counter
	ShutdownExecutorsInvalid tally.Counter
	ShutdownExecutorsFail    tally.Counter

	ReleaseHostOffers tally.Counter
	ReleaseHostsCount tally.Counter

	Elected         tally.Gauge
	MesosConnected  tally.Gauge
	HandlersRunning tally.Gauge

	ClusterCapacity     tally.Counter
	ClusterCapacityFail tally.Counter

	OfferOperations              tally.Counter
	OfferOperationsFail          tally.Counter
	OfferOperationsInvalid       tally.Counter
	OfferOperationsInvalidOffers tally.Counter

	RecoverySuccess tally.Counter
	RecoveryFail    tally.Counter

	GetDrainingHosts     tally.Counter
	GetDrainingHostsFail tally.Counter

	MarkHostsDrained     tally.Counter
	MarkHostsDrainedFail tally.Counter

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
		AcquireHostOffersCount:   scope.Counter("acquire_host_offers_count"),

		GetHosts:        scope.Counter("get_hosts"),
		GetHostsInvalid: scope.Counter("get_hosts_invalid"),
		GetHostsCount:   scope.Counter("get_hosts_count"),

		KillTasks:     scope.Counter("kill_tasks"),
		KillTasksFail: scope.Counter("kill_tasks_fail"),

		ShutdownExecutors:        scope.Counter("shutdown_executors"),
		ShutdownExecutorsInvalid: scope.Counter("shutdown_executors_invalid"),
		ShutdownExecutorsFail:    scope.Counter("shutdown_executors_fail"),

		ReleaseHostOffers: scope.Counter("release_host_offers"),
		ReleaseHostsCount: scope.Counter("release_hosts_count"),

		Elected:         serverScope.Gauge("elected"),
		MesosConnected:  serverScope.Gauge("mesos_connected"),
		HandlersRunning: serverScope.Gauge("handlers_running"),

		ClusterCapacity:     scope.Counter("cluster_capacity"),
		ClusterCapacityFail: scope.Counter("cluster_capacity_fail"),

		RecoverySuccess: scope.Counter("recovery_success"),
		RecoveryFail:    scope.Counter("recovery_fail"),

		GetDrainingHosts:     scope.Counter("get_hosts_to_drain"),
		GetDrainingHostsFail: scope.Counter("get_hosts_to_drain_fail"),

		MarkHostsDrained:     scope.Counter("mark_hosts_draining"),
		MarkHostsDrainedFail: scope.Counter("mark_hosts_draining_fail"),

		scope: scope,
	}
}

// RefreshClusterCapacityGauges refreshes all the cluster capacity gauges
func (m *Metrics) RefreshClusterCapacityGauges(response *hostsvc.ClusterCapacityResponse) {
	for _, resource := range response.GetResources() {
		if len(resource.GetKind()) == 0 {
			continue
		}

		gauge := m.scope.Gauge("cluster_capacity_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}

	for _, resource := range response.GetPhysicalSlackResources() {
		if resource.GetCapacity() == 0 {
			continue
		}

		gauge := m.scope.Gauge("cluster_capacity_revocable_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}
}
