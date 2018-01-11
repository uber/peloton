package tracked

import (
	"github.com/uber-go/tally"
)

// QueueMetrics contains all counters to track queue metrics
type QueueMetrics struct {
	queueLength   tally.Gauge
	queuePopDelay tally.Timer
}

// JobMetrics contains all counters to track job metrics
type JobMetrics struct {
	JobCreate           tally.Counter
	JobCreateFailed     tally.Counter
	JobRecoveryDuration tally.Gauge

	JobSucceeded tally.Counter
	JobKilled    tally.Counter
	JobFailed    tally.Counter

	JobRuntimeUpdated      tally.Counter
	JobRuntimeUpdateFailed tally.Counter
}

// TaskMetrics contains all counters to track task metrics
type TaskMetrics struct {
	TaskCreate       tally.Counter
	TaskCreateFail   tally.Counter
	TaskRecovered    tally.Counter
	ExecutorShutdown tally.Counter
}

// Metrics is the struct containing all the counters that track internal state
// of tracked manager.
type Metrics struct {
	scope       tally.Scope
	jobMetrics  *JobMetrics
	taskMetrics *TaskMetrics
	IsLeader    tally.Gauge
}

// NewQueueMetrics returns a new QueueMetrics struct.
func NewQueueMetrics(scope tally.Scope) *QueueMetrics {
	queueScope := scope.SubScope("queue")
	return &QueueMetrics{
		queueLength:   queueScope.Gauge("length"),
		queuePopDelay: queueScope.Timer("pop_delay"),
	}
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobScope := scope.SubScope("job")
	taskScope := scope.SubScope("task")
	managerScope := scope.SubScope("manager")

	jobMetrics := &JobMetrics{
		JobCreate:              jobScope.Counter("recovered"),
		JobCreateFailed:        jobScope.Counter("recover_failed"),
		JobRecoveryDuration:    jobScope.Gauge("recovery_duration"),
		JobSucceeded:           jobScope.Counter("job_succeeded"),
		JobKilled:              jobScope.Counter("job_killed"),
		JobFailed:              jobScope.Counter("job_failed"),
		JobRuntimeUpdated:      jobScope.Counter("runtime_update_success"),
		JobRuntimeUpdateFailed: jobScope.Counter("runtime_update_fail"),
	}

	taskMetrics := &TaskMetrics{
		TaskCreate:       taskScope.Counter("create"),
		TaskCreateFail:   taskScope.Counter("create_fail"),
		TaskRecovered:    taskScope.Counter("recovered"),
		ExecutorShutdown: taskScope.Counter("executor_shutdown"),
	}

	return &Metrics{
		scope:       scope,
		jobMetrics:  jobMetrics,
		taskMetrics: taskMetrics,
		IsLeader:    managerScope.Gauge("is_leader"),
	}
}
