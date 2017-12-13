package job

import (
	"github.com/uber-go/tally"
)

// RuntimeUpdaterMetrics contains the counters for RuntimeUpdater
type RuntimeUpdaterMetrics struct {
	JobSucceeded tally.Counter
	JobKilled    tally.Counter
	JobFailed    tally.Counter

	JobRuntimeUpdated      tally.Counter
	JobRuntimeUpdateFailed tally.Counter

	IsLeader tally.Gauge
}

// NewRuntimeUpdaterMetrics creates a RuntimeUpdaterMetrics
func NewRuntimeUpdaterMetrics(scope tally.Scope) *RuntimeUpdaterMetrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})

	return &RuntimeUpdaterMetrics{
		JobSucceeded: scope.Counter("job_succeeded"),
		JobKilled:    scope.Counter("job_killed"),
		JobFailed:    scope.Counter("job_failed"),
		IsLeader:     scope.Gauge("is_leader"),

		JobRuntimeUpdated:      successScope.Counter("runtime_updated"),
		JobRuntimeUpdateFailed: failScope.Counter("runtime_updated"),
	}
}
