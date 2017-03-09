package placement

import (
	"time"
)

// Config is Placement engine specific config
type Config struct {
	Port int `yaml:"port"`
	// TaskDequeueLimit is the max number of tasks to dequeue in a request
	TaskDequeueLimit int `yaml:"task_dequeue_limit"`
	// OfferDequeueLimit is the max Number of HostOffers to dequeue in
	// a request
	OfferDequeueLimit int `yaml:"offer_dequeue_limit"`
	// MaxPlacementDuration is the max time duration to place tasks for a task group
	MaxPlacementDuration time.Duration `yaml:"max_placement_duration"`
}
