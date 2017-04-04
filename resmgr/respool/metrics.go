package respool

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in respool.
type Metrics struct {
	APICreateResourcePool          tally.Counter
	CreateResourcePoolSuccess      tally.Counter
	CreateResourcePoolFail         tally.Counter
	CreateResourcePoolRollbackFail tally.Counter

	APIGetResourcePool     tally.Counter
	GetResourcePoolSuccess tally.Counter
	GetResourcePoolFail    tally.Counter

	APIUpdateResourcePool          tally.Counter
	UpdateResourcePoolSuccess      tally.Counter
	UpdateResourcePoolFail         tally.Counter
	UpdateResourcePoolRollbackFail tally.Counter

	APIDeleteResourcePool     tally.Counter
	DeleteResourcePoolSuccess tally.Counter
	DeleteResourcePoolFail    tally.Counter
}

// NewMetrics returns a new instance of respool.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	apiScope := scope.SubScope("api")

	return &Metrics{
		APICreateResourcePool:          apiScope.Counter("create_resource_pool"),
		CreateResourcePoolSuccess:      successScope.Counter("create_resource_pool"),
		CreateResourcePoolFail:         failScope.Counter("create_resource_pool"),
		CreateResourcePoolRollbackFail: failScope.Counter("create_resource_pool_rollback"),

		APIGetResourcePool:     apiScope.Counter("get_resource_pool"),
		GetResourcePoolSuccess: successScope.Counter("get_resource_pool"),
		GetResourcePoolFail:    failScope.Counter("get_resource_pool"),

		APIUpdateResourcePool:          apiScope.Counter("update_resource_pool"),
		UpdateResourcePoolSuccess:      successScope.Counter("update_resource_pool"),
		UpdateResourcePoolFail:         failScope.Counter("update_resource_pool"),
		UpdateResourcePoolRollbackFail: failScope.Counter("update_resource_pool_rollback"),

		APIDeleteResourcePool:     apiScope.Counter("delete_resource_pool"),
		DeleteResourcePoolSuccess: successScope.Counter("delete_resource_pool"),
		DeleteResourcePoolFail:    failScope.Counter("delete_resource_pool"),
	}
}
