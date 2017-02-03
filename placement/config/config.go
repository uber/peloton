package config

import (
	"code.uber.internal/infra/peloton/common/metrics"
)

// Config holds all configuration necessary to run a placement engine.
type Config struct {
	Metrics metrics.Config `yaml:"metrics"`
	// TaskDequeueLimit is the max number of tasks to dequeue in a request
	Placement PlacementConfig `yaml:"placement"`
}

// PlacementConfig is placement engine specific config
type PlacementConfig struct {
	TaskDequeueLimit int `yaml:"task_dequeue_limit"`
	// OfferDequeueLimit is the max Number of Offers to dequeue in a request
	OfferDequeueLimit int `yaml:"offer_dequeue_limit"`
}
