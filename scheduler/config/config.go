package config

// Config is the peloton scheduler specific configuration
type Config struct {
	// TaskDequeueLimit is the max number of tasks to dequeue in a request
	TaskDequeueLimit int `yaml:"task_dequeue_limit"`
	// OfferDequeueLimit is the max Number of Offers to dequeue in a request
	OfferDequeueLimit int `yaml:"offer_dequeue_limit"`
}
