package main

import (
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/leader"
	storage "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all configs to run a peloton-hostmgr server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Storage      storage.Config        `yaml:"storage"`
	HostManager  hostmgr.Config        `yaml:"host_manager"`
	Mesos        mesos.Config          `yaml:"mesos"`
	Election     leader.ElectionConfig `yaml:"election"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
}
