package goalstate

import "time"

const (
	_defaultMaxRetryDelay     = 60 * time.Minute
	_defaultSuccessRetryDelay = 20 * time.Second
	_defaultFailureRetryDelay = 2 * time.Second
)

// Config for the goalstate engine.
type Config struct {
	// MaxRetryDelay is the absolute maximum duration between any retry, capping
	// any backoff to this abount.
	MaxRetryDelay time.Duration `yaml:"max_retry_delay"`
	// SuccessRetryDelay is the delay for retry, if an operation was successful
	// but no state transition was observed. Backoff will be applied for up
	// to MaxRetryDelay.
	SuccessRetryDelay time.Duration `yaml:"success_retry_delay"`
	// FailureRetryDelay is the delay for retry, if an operation failed. Backoff
	// will be applied for up to MaxRetryDelay.
	FailureRetryDelay time.Duration `yaml:"failure_retry_delay"`
}

// normalize configuration by setting unassigned fields to default values.
func (c *Config) normalize() {
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = _defaultMaxRetryDelay
	}
	if c.SuccessRetryDelay == 0 {
		c.SuccessRetryDelay = _defaultSuccessRetryDelay
	}
	if c.FailureRetryDelay == 0 {
		c.FailureRetryDelay = _defaultFailureRetryDelay
	}
}
