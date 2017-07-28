package task

import "time"

// Config is Resource Manager Task specific configuration
type Config struct {
	// Timeout for rm task in statemachine from launching to ready state
	LaunchingTimeout time.Duration `yaml:"launching_timeout"`
	// Timeout for rm task in statemachine from placing to ready state
	PlacingTimeout time.Duration `yaml:"placing_timeout"`
}
