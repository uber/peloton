package main

import (
	"code.uber.internal/infra/peloton/archiver"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/leader"
)

// Config holds all config to run a peloton-archiver server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Election     leader.ElectionConfig `yaml:"election"`
	Archiver     archiver.Config       `yaml:"archiver"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
}
