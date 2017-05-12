package job

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the job service
type Metrics struct {
	JobAPICreate  tally.Counter
	JobCreate     tally.Counter
	JobCreateFail tally.Counter
	JobAPIGet     tally.Counter
	JobGet        tally.Counter
	JobGetFail    tally.Counter
	JobAPIDelete  tally.Counter
	JobDelete     tally.Counter
	JobDeleteFail tally.Counter
	JobAPIQuery   tally.Counter
	JobQuery      tally.Counter
	JobQueryFail  tally.Counter
	JobUpdate     tally.Counter
	JobUpdateFail tally.Counter

	JobAPIGetByRespoolID  tally.Counter
	JobGetByRespoolID     tally.Counter
	JobGetByRespoolIDFail tally.Counter

	// TODO: find a better way of organizing metrics per package
	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	jobFailScope := scope.Tagged(map[string]string{"type": "fail"})
	jobAPIScope := scope.SubScope("api")

	// TODO: find a better way of organizing metrics per package so we
	// don't have to nest task scope under job scope here.
	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})

	return &Metrics{
		JobAPICreate:   jobAPIScope.Counter("create"),
		JobCreate:      jobSuccessScope.Counter("create"),
		JobCreateFail:  jobFailScope.Counter("create"),
		JobAPIGet:      jobAPIScope.Counter("get"),
		JobGet:         jobSuccessScope.Counter("get"),
		JobGetFail:     jobFailScope.Counter("get"),
		JobAPIDelete:   jobAPIScope.Counter("delete"),
		JobDelete:      jobSuccessScope.Counter("delete"),
		JobDeleteFail:  jobFailScope.Counter("delete"),
		JobAPIQuery:    jobAPIScope.Counter("query"),
		JobQuery:       jobSuccessScope.Counter("query"),
		JobQueryFail:   jobFailScope.Counter("query"),
		JobUpdate:      jobSuccessScope.Counter("update"),
		JobUpdateFail:  jobFailScope.Counter("update"),
		TaskCreate:     taskSuccessScope.Counter("create"),
		TaskCreateFail: taskFailScope.Counter("create"),

		JobAPIGetByRespoolID:  jobAPIScope.Counter("get_by_respool_id"),
		JobGetByRespoolID:     jobSuccessScope.Counter("get_by_respool_id"),
		JobGetByRespoolIDFail: jobFailScope.Counter("get_by_respool_id"),
	}
}

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
