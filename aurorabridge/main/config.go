package main

import (
	"github.com/uber/peloton/aurorabridge"
	"github.com/uber/peloton/common/health"
	"github.com/uber/peloton/common/logging"
	"github.com/uber/peloton/common/metrics"
	"github.com/uber/peloton/leader"
)

// Config defines aurorabridge configuration.
type Config struct {
	Metrics      metrics.Config               `yaml:"metrics"`
	Health       health.Config                `yaml:"health"`
	SentryConfig logging.SentryConfig         `yaml:"sentry"`
	Election     leader.ElectionConfig        `yaml:"election"`
	Bootstrap    aurorabridge.BootstrapConfig `yaml:"bootstrap"`
}
