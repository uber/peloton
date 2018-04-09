package reconcile

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in hostmgr
// reconciliation package.
type Metrics struct {
	ReconcileImplicitly      tally.Counter
	ReconcileImplicitlyFail  tally.Counter
	ReconcileExplicitly      tally.Counter
	ReconcileExplicitlyAbort tally.Counter
	ReconcileExplicitlyFail  tally.Counter
	ReconcileGetTasksFail    tally.Counter

	ExplicitTasksPerRun tally.Gauge
}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	return &Metrics{
		ReconcileImplicitly:      successScope.Counter("implicitly_total"),
		ReconcileImplicitlyFail:  failScope.Counter("implicitly_total"),
		ReconcileExplicitly:      successScope.Counter("explicitly_total"),
		ReconcileExplicitlyAbort: failScope.Counter("explicitly_abort_total"),
		ReconcileExplicitlyFail:  failScope.Counter("explicitly_total"),
		ReconcileGetTasksFail:    failScope.Counter("explicitly_gettasks_total"),

		ExplicitTasksPerRun: scope.Gauge("explicit_tasks_per_run"),
	}
}
