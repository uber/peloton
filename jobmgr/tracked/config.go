package tracked

import "time"

const (
	_defaultJobRuntimeUpdateInterval = 1 * time.Second
)

// Config for job related behavior.
type Config struct {
	// JobRuntimeUpdateInterval is the interval at which batch jobs runtime updater is run.
	JobBatchRuntimeUpdateInterval time.Duration `yaml:"job_batch_runtime_update_interval"`
	// JobServiceRuntimeUpdateInterval is the interval at which service jobs runtime updater is run.
	JobServiceRuntimeUpdateInterval time.Duration `yaml:"job_service_runtime_update_interval"`
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.JobBatchRuntimeUpdateInterval == 0 {
		c.JobBatchRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}
	if c.JobServiceRuntimeUpdateInterval == 0 {
		c.JobServiceRuntimeUpdateInterval = _defaultJobRuntimeUpdateInterval
	}
}
