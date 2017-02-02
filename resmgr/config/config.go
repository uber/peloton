package resmgr

import (
	"code.uber.internal/infra/peloton/leader"
)

// Config for Resource Manager specific configuration
type Config struct {
	Election leader.ElectionConfig `yaml:"election"`
	Port     int                   `yaml:"port"`
}
