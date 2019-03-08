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

package respool

import (
	"github.com/uber/peloton/pkg/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in respool.
type Metrics struct {
	APICreateResourcePool          tally.Counter
	CreateResourcePoolSuccess      tally.Counter
	CreateResourcePoolFail         tally.Counter
	CreateResourcePoolRollbackFail tally.Counter

	APIGetResourcePool     tally.Counter
	GetResourcePoolSuccess tally.Counter
	GetResourcePoolFail    tally.Counter

	APILookupResourcePoolID     tally.Counter
	LookupResourcePoolIDSuccess tally.Counter
	LookupResourcePoolIDFail    tally.Counter

	APIUpdateResourcePool          tally.Counter
	UpdateResourcePoolSuccess      tally.Counter
	UpdateResourcePoolFail         tally.Counter
	UpdateResourcePoolRollbackFail tally.Counter

	APIDeleteResourcePool     tally.Counter
	DeleteResourcePoolSuccess tally.Counter
	DeleteResourcePoolFail    tally.Counter

	APIQueryResourcePools     tally.Counter
	QueryResourcePoolsSuccess tally.Counter
	QueryResourcePoolsFail    tally.Counter

	PendingQueueSize    tally.Gauge
	RevocableQueueSize  tally.Gauge
	ControllerQueueSize tally.Gauge
	NPQueueSize         tally.Gauge

	TotalAllocation          scalar.GaugeMaps
	NonPreemptibleAllocation scalar.GaugeMaps
	NonSlackAllocation       scalar.GaugeMaps
	SlackAllocation          scalar.GaugeMaps
	ControllerAllocation     scalar.GaugeMaps

	TotalEntitlement    scalar.GaugeMaps
	NonSlackEntitlement scalar.GaugeMaps
	SlackEntitlement    scalar.GaugeMaps

	NonSlackAvailable scalar.GaugeMaps
	SlackAvailable    scalar.GaugeMaps

	Demand      scalar.GaugeMaps
	SlackDemand scalar.GaugeMaps

	ResourcePoolReservation scalar.GaugeMaps
	ResourcePoolLimit       scalar.GaugeMaps
	ResourcePoolShare       scalar.GaugeMaps

	ControllerLimit scalar.GaugeMaps
	SlackLimit      scalar.GaugeMaps
}

// NewMetrics returns a new instance of respool.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	apiScope := scope.SubScope("api")

	queueScope := scope.SubScope("queue")

	allocationScope := scope.SubScope("allocation")
	entitlementScope := scope.SubScope("entitlement")
	availableScope := scope.SubScope("available")
	slackAvailableScope := scope.SubScope("slack_available")
	demandScope := scope.SubScope("demand")
	slackDemandScope := scope.SubScope("slack_demand")

	reservationScope := scope.SubScope("reservation")
	limitScope := scope.SubScope("limit")
	shareScope := scope.SubScope("share")
	return &Metrics{
		APICreateResourcePool:          apiScope.Counter("create_resource_pool"),
		CreateResourcePoolSuccess:      successScope.Counter("create_resource_pool"),
		CreateResourcePoolFail:         failScope.Counter("create_resource_pool"),
		CreateResourcePoolRollbackFail: failScope.Counter("create_resource_pool_rollback"),

		APIGetResourcePool:     apiScope.Counter("get_resource_pool"),
		GetResourcePoolSuccess: successScope.Counter("get_resource_pool"),
		GetResourcePoolFail:    failScope.Counter("get_resource_pool"),

		APILookupResourcePoolID:     apiScope.Counter("lookup_resource_pool_id"),
		LookupResourcePoolIDSuccess: successScope.Counter("lookup_resource_pool_id"),
		LookupResourcePoolIDFail:    failScope.Counter("lookup_resource_pool_id"),

		APIUpdateResourcePool:          apiScope.Counter("update_resource_pool"),
		UpdateResourcePoolSuccess:      successScope.Counter("update_resource_pool"),
		UpdateResourcePoolFail:         failScope.Counter("update_resource_pool"),
		UpdateResourcePoolRollbackFail: failScope.Counter("update_resource_pool_rollback"),

		APIDeleteResourcePool:     apiScope.Counter("delete_resource_pool"),
		DeleteResourcePoolSuccess: successScope.Counter("delete_resource_pool"),
		DeleteResourcePoolFail:    failScope.Counter("delete_resource_pool"),

		APIQueryResourcePools:     apiScope.Counter("query_resource_pools"),
		QueryResourcePoolsSuccess: successScope.Counter("query_resource_pools"),
		QueryResourcePoolsFail:    failScope.Counter("query_resource_pools"),

		PendingQueueSize:    queueScope.Gauge("pending_queue_size"),
		RevocableQueueSize:  queueScope.Gauge("revocable_queue_size"),
		ControllerQueueSize: queueScope.Gauge("controller_queue_size"),
		NPQueueSize:         queueScope.Gauge("np_queue_size"),

		TotalAllocation: scalar.NewGaugeMaps(allocationScope),
		NonPreemptibleAllocation: scalar.NewGaugeMaps(allocationScope.
			SubScope("non_preemptible")),
		ControllerAllocation: scalar.NewGaugeMaps(allocationScope.
			SubScope("controller_allocation")),
		NonSlackAllocation: scalar.NewGaugeMaps(allocationScope.
			SubScope("non_slack_allocation")),
		SlackAllocation: scalar.NewGaugeMaps(allocationScope.
			SubScope("slack_allocation")),

		TotalEntitlement: scalar.NewGaugeMaps(entitlementScope),
		SlackEntitlement: scalar.NewGaugeMaps(entitlementScope.
			SubScope("slack_entitlement")),
		NonSlackEntitlement: scalar.NewGaugeMaps(entitlementScope.
			SubScope("non_slack_entitlement")),

		NonSlackAvailable: scalar.NewGaugeMaps(availableScope),
		SlackAvailable:    scalar.NewGaugeMaps(slackAvailableScope),

		Demand:      scalar.NewGaugeMaps(demandScope),
		SlackDemand: scalar.NewGaugeMaps(slackDemandScope),

		ResourcePoolReservation: scalar.NewGaugeMaps(reservationScope),
		ResourcePoolLimit:       scalar.NewGaugeMaps(limitScope),
		ResourcePoolShare:       scalar.NewGaugeMaps(shareScope),

		ControllerLimit: scalar.NewGaugeMaps(limitScope.SubScope(
			"controller_limit")),
		SlackLimit: scalar.NewGaugeMaps(limitScope.SubScope(
			"slack_limit")),
	}
}
