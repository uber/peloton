package job

import "time"

const (
	_defaultStateUpdateInterval = 15 * time.Second
)

// Config for job related behavior.
type Config struct {
	// StateUpdateInterval is the interval at which the job state is checked and
	// persisted.
	StateUpdateInterval time.Duration `yaml:"state_update_interval"`
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.StateUpdateInterval == 0 {
		c.StateUpdateInterval = _defaultStateUpdateInterval
	}
}
