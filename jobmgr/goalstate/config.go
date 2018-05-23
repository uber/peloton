package goalstate

import "time"

const (
	_defaultMaxRetryDelay            = 60 * time.Minute
	_defaultFailureRetryDelay        = 2 * time.Second
	_defaultLaunchTimeRetryDuration  = 20 * time.Minute
	_defaultJobRuntimeUpdateInterval = 1 * time.Second
	_defaultJobWorkerThreads         = 50
	_defaultTaskWorkerThreads        = 1000
)

// Config for the goalstate engine.
type Config struct {
	// MaxRetryDelay is the absolute maximum duration between any retry, capping
	// any backoff to this abount.
	MaxRetryDelay time.Duration `yaml:"max_retry_delay"`
	// FailureRetryDelay is the delay for retry, if an operation failed. Backoff
	// will be applied for up to MaxRetryDelay.
	FailureRetryDelay time.Duration `yaml:"failure_retry_delay"`

	// LaunchTimeout is the timeout value for the launched state.
	// If no update is received from Mesos within this timeout value,
	// the task will be re-queued to the resource manager for placement
	// with a new mesos task id.
	LaunchTimeout time.Duration `yaml:"launch_timeout"`

	// JobRuntimeUpdateInterval is the interval at which batch jobs runtime updater is run.
	JobBatchRuntimeUpdateInterval time.Duration `yaml:"job_batch_runtime_update_interval"`
	// JobServiceRuntimeUpdateInterval is the interval at which service jobs runtime updater is run.
	JobServiceRuntimeUpdateInterval time.Duration `yaml:"job_service_runtime_update_interval"`

	// NumWorkerJobThreads is the number of worker threads in the pool
	// serving the job goal state engine. This number indicates the maximum
	// number of jobs which can be parallely processed by the goal state engine.
	NumWorkerJobThreads int `yaml:"job_worker_thread_count"`
	// NumWorkerTaskThreads is the number of worker threads in the pool
	// serving the task goal state engine. This number indicates the maximum
	// number of tasks which can be parallely processed by the goal state engine.
	NumWorkerTaskThreads int `yaml:"task_worker_thread_count"`
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = _defaultMaxRetryDelay
	}
	if c.FailureRetryDelay == 0 {
		c.FailureRetryDelay = _defaultFailureRetryDelay
	}

	if c.LaunchTimeout == 0 {
		c.LaunchTimeout = _defaultLaunchTimeRetryDuration
	}

	if c.JobBatchRuntimeUpdateInterval == 0 {
		c.JobBatchRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}
	if c.JobServiceRuntimeUpdateInterval == 0 {
		c.JobServiceRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}

	if c.NumWorkerJobThreads == 0 {
		c.NumWorkerJobThreads = _defaultJobWorkerThreads
	}
	if c.NumWorkerTaskThreads == 0 {
		c.NumWorkerTaskThreads = _defaultTaskWorkerThreads
	}
}
