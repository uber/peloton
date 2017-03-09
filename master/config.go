package master

// Config is the Peloton master specific configuration
type Config struct {
	Port int `yaml:"port"`
	// Time to hold offer for in seconds
	OfferHoldTimeSec int `yaml:"offer_hold_time_sec"`
	// Frequency of running offer pruner
	OfferPruningPeriodSec int `yaml:"offer_pruning_period_sec"`
	// FIXME(gabe): this isnt really the DB write concurrency. This is
	// only used for processing task updates and should be moved into
	// the storage namespace, and made clearer what this controls
	// (threads? rows? statements?)
	DbWriteConcurrency int `yaml:"db_write_concurrency"`
	// Number of go routines that will ack for status updates to mesos
	TaskUpdateAckConcurrency int `yaml:"taskupdate_ack_concurrency"`
	// Size of the channel buffer of the status updates
	TaskUpdateBufferSize int `yaml:"taskupdate_buffer_size"`
}
