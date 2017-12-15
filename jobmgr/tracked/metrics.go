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
}

// TaskMetrics contains all counters to track task metrics
type TaskMetrics struct {
	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter
	TaskRecovered  tally.Counter
}

// Metrics is the struct containing all the counters that track internal state
// of tracked manager.
type Metrics struct {
	scope       tally.Scope
	jobMetrics  *JobMetrics
	taskMetrics *TaskMetrics
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

	jobMetrics := &JobMetrics{
		JobCreate:           jobScope.Counter("recovered"),
		JobCreateFailed:     jobScope.Counter("recover_failed"),
		JobRecoveryDuration: jobScope.Gauge("recovery_duration"),
	}

	taskMetrics := &TaskMetrics{
		TaskCreate:     taskScope.Counter("create"),
		TaskCreateFail: taskScope.Counter("create_fail"),
		TaskRecovered:  taskScope.Counter("recovered"),
	}

	return &Metrics{
		scope:       scope,
		jobMetrics:  jobMetrics,
		taskMetrics: taskMetrics,
	}
}
