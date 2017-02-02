package config

import (
	cconfig "code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/leader"
	schedulerconfig "code.uber.internal/infra/peloton/scheduler/config"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/storage/stapi"
)

// FrameworkURLPath is where the RPC endpoint lives for peloton
const FrameworkURLPath = "/api/v1"

// Config encapulates the master runtime config
type Config struct {
	Metrics       metricsConfiguration   `yaml:"metrics"`
	StorageConfig StorageConfig          `yaml:"storage"`
	Master        MasterConfig           `yaml:"master"`
	Mesos         mesos.Config           `yaml:"mesos"`
	Scheduler     schedulerconfig.Config `yaml:"scheduler"`
	Election      leader.ElectionConfig  `yaml:"election"`
}

// StorageConfig contains the different DB config values for each supported backend
type StorageConfig struct {
	MySQLConfig mysql.Config `yaml:"mysql"`
	STAPIConfig stapi.Config `yaml:"stapi"`
}

// MasterConfig is framework specific configuration
type MasterConfig struct {
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

type metricsConfiguration struct {
	Prometheus *prometheusConfiguration `yaml:"prometheus"`
	Statsd     *statsdConfiguration     `yaml:"statsd"`
}
type prometheusConfiguration struct {
	Enable bool `yaml:"enable"`
}
type statsdConfiguration struct {
	Enable   bool   `yaml:"enable"`
	Endpoint string `yaml:"endpoint"`
}

// New loads the given configs in order, merges them together, and returns
// the Config
func New(configs ...string) (*Config, error) {
	config := &Config{}
	if err := cconfig.Parse(config, configs...); err != nil {
		return nil, err
	}
	return config, nil
}
