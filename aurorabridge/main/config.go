package main

import (
	"code.uber.internal/infra/peloton/aurorabridge"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/leader"
)

// Config defines aurorabridge configuration.
type Config struct {
	Metrics      metrics.Config               `yaml:"metrics"`
	Health       health.Config                `yaml:"health"`
	SentryConfig logging.SentryConfig         `yaml:"sentry"`
	Election     leader.ElectionConfig        `yaml:"election"`
	Bootstrap    aurorabridge.BootstrapConfig `yaml:"bootstrap"`
}
