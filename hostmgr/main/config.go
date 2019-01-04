package main

import (
	"github.com/uber/peloton/common/health"
	"github.com/uber/peloton/common/logging"
	"github.com/uber/peloton/common/metrics"
	"github.com/uber/peloton/hostmgr/config"
	"github.com/uber/peloton/hostmgr/mesos"
	"github.com/uber/peloton/leader"
	storage "github.com/uber/peloton/storage/config"
)

// Config holds all configs to run a peloton-hostmgr server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Storage      storage.Config        `yaml:"storage"`
	HostManager  config.Config         `yaml:"host_manager"`
	Mesos        mesos.Config          `yaml:"mesos"`
	Election     leader.ElectionConfig `yaml:"election"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
}
