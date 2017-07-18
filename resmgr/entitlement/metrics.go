package entitlement

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	EntitlementCalculationMissed tally.Counter
}

// NewMetrics returns a new instance of task.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	calculatorScope := scope.SubScope("Entitlement")
	return &Metrics{
		EntitlementCalculationMissed: calculatorScope.Counter(
			"calculation_missed"),
	}
}
