package health

import (
	"time"
)

// Config is the placeholder for health related configs
type Config struct {
	// Heartbeat interval
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}
