package updatesvc

import "time"

const (
	_defaultReloadInterval   = 60 * time.Second
	_defaultProgressInterval = 5 * time.Second
)

// Config for updates, and how they are progressed.
type Config struct {
	ReloadInterval   time.Duration `yaml:"reload_interval"`
	ProgressInterval time.Duration `yaml:"progress_interval"`
}

func (c *Config) normalize() {
	if c.ReloadInterval == 0 {
		c.ReloadInterval = _defaultReloadInterval
	}

	if c.ProgressInterval == 0 {
		c.ProgressInterval = _defaultProgressInterval
	}
}
