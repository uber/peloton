package hostmgr

import (
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/leader"
	sc "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all configuration necessary to run a host manager server.
type Config struct {
	Metrics     metrics.Config        `yaml:"metrics"`
	Storage     sc.StorageConfig      `yaml:"storage"`
	HostManager HostManagerConfig     `yaml:"host_manager"`
	Mesos       mesos.Config          `yaml:"mesos"`
	Election    leader.ElectionConfig `yaml:"election"`
}

// HostManagerConfig is framework specific configuration
type HostManagerConfig struct {
	Port                  int `yaml:"port"`
	OfferHoldTimeSec      int `yaml:"offer_hold_time_sec"`      // Time to hold offer for in seconds
	OfferPruningPeriodSec int `yaml:"offer_pruning_period_sec"` // Frequency of running offer pruner

	// FIXME(gabe): this isnt really the DB write concurrency. This is only used for processing task updates
	// and should be moved into the storage namespace, and made clearer what this controls (threads? rows? statements?)
	DbWriteConcurrency int `yaml:"db_write_concurrency"`

	// Number of go routines that will ack for status updates to mesos
	TaskUpdateAckConcurrency int `yaml:"taskupdate_ack_concurrency"`

	// Size of the channel buffer of the status updates
	TaskUpdateBufferSize int `yaml:"taskupdate_buffer_size"`
}
