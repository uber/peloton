package respool

import (
	"code.uber.internal/infra/peloton/common/scalar"

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

	PendingQueueSize tally.Gauge

	TotalAllocation          scalar.GaugeMaps
	NonPreemptibleAllocation scalar.GaugeMaps
	Entitlement              scalar.GaugeMaps
	Available                scalar.GaugeMaps
	Demand                   scalar.GaugeMaps

	ResourcePoolReservation scalar.GaugeMaps
	ResourcePoolLimit       scalar.GaugeMaps
	ResourcePoolShare       scalar.GaugeMaps
}

// NewMetrics returns a new instance of respool.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	apiScope := scope.SubScope("api")

	pendingScope := scope.SubScope("pending")

	allocationScope := scope.SubScope("totalAlloc")
	entitlementScope := scope.SubScope("entitlement")
	availableScope := scope.SubScope("available")
	demandScope := scope.SubScope("demand")

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

		PendingQueueSize: pendingScope.Gauge("pending_queue_size"),

		TotalAllocation: scalar.NewGaugeMaps(allocationScope.SubScope(
			"total")),
		NonPreemptibleAllocation: scalar.NewGaugeMaps(allocationScope.
			SubScope("non_preemptible")),
		Entitlement: scalar.NewGaugeMaps(entitlementScope),
		Available:   scalar.NewGaugeMaps(availableScope),
		Demand:      scalar.NewGaugeMaps(demandScope),

		ResourcePoolReservation: scalar.NewGaugeMaps(reservationScope),
		ResourcePoolLimit:       scalar.NewGaugeMaps(limitScope),
		ResourcePoolShare:       scalar.NewGaugeMaps(shareScope),
	}
}
