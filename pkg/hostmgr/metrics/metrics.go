// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/uber-go/tally"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common/util"
)

// Metrics is a placeholder for all metrics in hostmgr.
type Metrics struct {
	LaunchTasks              tally.Counter
	LaunchTasksFail          tally.Counter
	LaunchTasksInvalid       tally.Counter
	LaunchTasksInvalidOffers tally.Counter

	AcquireHostOffers        tally.Counter
	AcquireHostOffersFail    tally.Counter
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

	ReleaseHostOffers     tally.Counter
	ReleaseHostOffersFail tally.Counter
	ReleaseHostsCount     tally.Counter

	GetMesosMasterHostPort     tally.Counter
	GetMesosMasterHostPortFail tally.Counter

	Elected         tally.Gauge
	MesosConnected  tally.Gauge
	HandlersRunning tally.Gauge

	ClusterCapacity     tally.Counter
	ClusterCapacityFail tally.Counter

	RecoverySuccess tally.Counter
	RecoveryFail    tally.Counter

	GetDrainingHosts     tally.Counter
	GetDrainingHostsFail tally.Counter

	MarkHostDrained     tally.Counter
	MarkHostDrainedFail tally.Counter

	WatchEventCancel   tally.Counter
	WatchEventOverflow tally.Counter

	WatchCancelNotFound tally.Counter

	// Time takes to acquire lock in watch processor
	WatchProcessorLockDuration tally.Timer

	GetCqosAdvisorMetric     tally.Counter
	GetCqosAdvisorMetricFail tally.Counter

	scope tally.Scope
}

// NewMetrics returns a new instance of hostmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	serverScope := scope.SubScope("server")
	watchEventScope := scope.SubScope("watch")
	return &Metrics{
		LaunchTasks:              scope.Counter("launch_tasks"),
		LaunchTasksFail:          scope.Counter("launch_tasks_fail"),
		LaunchTasksInvalid:       scope.Counter("launch_tasks_invalid"),
		LaunchTasksInvalidOffers: scope.Counter("launch_tasks_invalid_offers"),

		AcquireHostOffers:        scope.Counter("acquire_host_offers"),
		AcquireHostOffersFail:    scope.Counter("acquire_host_offers_fail"),
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

		ReleaseHostOffers:     scope.Counter("release_host_offers"),
		ReleaseHostOffersFail: scope.Counter("release_host_offers_fail"),
		ReleaseHostsCount:     scope.Counter("release_hosts_count"),

		GetMesosMasterHostPort:     scope.Counter("get_mesos_master_host_port"),
		GetMesosMasterHostPortFail: scope.Counter("get_mesos_master_host_port_fail"),

		Elected:         serverScope.Gauge("elected"),
		MesosConnected:  serverScope.Gauge("mesos_connected"),
		HandlersRunning: serverScope.Gauge("handlers_running"),

		ClusterCapacity:     scope.Counter("cluster_capacity"),
		ClusterCapacityFail: scope.Counter("cluster_capacity_fail"),

		RecoverySuccess: scope.Counter("recovery_success"),
		RecoveryFail:    scope.Counter("recovery_fail"),

		GetDrainingHosts:     scope.Counter("get_draining_hosts"),
		GetDrainingHostsFail: scope.Counter("get_draining_hosts_fail"),

		MarkHostDrained:     scope.Counter("mark_host_drained"),
		MarkHostDrainedFail: scope.Counter("mark_host_drained_fail"),

		WatchEventCancel:           watchEventScope.Counter("watch_event_cancel"),
		WatchEventOverflow:         watchEventScope.Counter("watch_event_overflow"),
		WatchCancelNotFound:        watchEventScope.Counter("watch_cancel_not_found"),
		WatchProcessorLockDuration: watchEventScope.Timer("watch_processor_lock_duration"),

		GetCqosAdvisorMetric:     scope.Counter("get_cqos_advisor_metric"),
		GetCqosAdvisorMetricFail: scope.Counter("get_cqos_advisor_metric_fail"),

		scope: scope,
	}
}

// RefreshClusterCapacityGauges refreshes all the cluster capacity gauges
func (m *Metrics) RefreshClusterCapacityGauges(response *hostsvc.ClusterCapacityResponse) {
	// update metrics for total cluster capacity
	for _, resource := range response.GetPhysicalResources() {
		if len(resource.GetKind()) == 0 || resource.GetCapacity() < util.ResourceEpsilon {
			continue
		}

		gauge := m.scope.Gauge("cluster_capacity_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}

	for _, resource := range response.GetPhysicalSlackResources() {
		if len(resource.GetKind()) == 0 || resource.GetCapacity() < util.ResourceEpsilon {
			continue
		}

		gauge := m.scope.Gauge("cluster_capacity_revocable_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}

	// update metrics for resources allocated tasks launched by Peloton
	for _, resource := range response.GetResources() {
		if len(resource.GetKind()) == 0 || resource.GetCapacity() < util.ResourceEpsilon {
			continue
		}

		gauge := m.scope.Gauge("mesos_task_allocation_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}

	for _, resource := range response.GetAllocatedSlackResources() {
		if len(resource.GetKind()) == 0 || resource.GetCapacity() < util.ResourceEpsilon {
			continue
		}

		gauge := m.scope.Gauge("mesos_task_allocation_revocable_" + resource.GetKind())
		gauge.Update(resource.GetCapacity())
	}
}
