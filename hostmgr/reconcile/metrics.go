package reconcile

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in hostmgr
// reconciliation package.
type Metrics struct {
	ReconcileImplicitly     tally.Counter
	ReconcileImplicitlyFail tally.Counter
}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})
	return &Metrics{
		ReconcileImplicitly:     successScope.Counter("reconcile_implicitly"),
		ReconcileImplicitlyFail: failScope.Counter("reconcile_implicitly"),
	}
}
