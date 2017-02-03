package config

import (
	cconfig "code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage/config"
)

// FrameworkURLPath is where the RPC endpoint lives for peloton
const FrameworkURLPath = "/api/v1"

// Config for Resource Manager specific configuration
type Config struct {
	Metrics  metrics.Config        `yaml:"metrics"`
	Storage  config.StorageConfig  `yaml:"storage"`
	ResMgr   ResMgrConfig          `yaml:"resmgr"`
	Election leader.ElectionConfig `yaml:"election"`
}

// ResMgrConfig contains the resource Manager config
type ResMgrConfig struct {
	Port               int `yaml:"port"`
	DbWriteConcurrency int `yaml:"db_write_concurrency"`
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
