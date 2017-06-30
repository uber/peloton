package job

import (
	"github.com/uber-go/tally"
)

// RecoveryMetrics is the struct contains the counters or job recovery
type RecoveryMetrics struct {
	JobRecovered      tally.Counter
	JobRecoverFailed  tally.Counter
	TaskRecovered     tally.Counter
	TaskRecoverFailed tally.Counter
	TaskRequeued      tally.Counter
	TaskRequeueFailed tally.Counter
}

// NewRecoveryMetrics creates the RecoveryMetrics
func NewRecoveryMetrics(scope tally.Scope) *RecoveryMetrics {
	successScope := scope.Tagged(map[string]string{"type": "success"})
	failScope := scope.Tagged(map[string]string{"type": "fail"})

	return &RecoveryMetrics{
		JobRecovered:      successScope.Counter("job_recovered"),
		JobRecoverFailed:  failScope.Counter("job_recovered"),
		TaskRecovered:     successScope.Counter("task_recovered"),
		TaskRecoverFailed: failScope.Counter("task_recovered"),
		TaskRequeued:      successScope.Counter("task_requeued"),
		TaskRequeueFailed: failScope.Counter("task_requeued"),
	}
}

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
