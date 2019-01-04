package main

import (
	"github.com/uber/peloton/common/health"
	"github.com/uber/peloton/common/logging"
	"github.com/uber/peloton/common/metrics"
	"github.com/uber/peloton/leader"
	"github.com/uber/peloton/resmgr"
	storage "github.com/uber/peloton/storage/config"
)

// Config holds all configs to run a peloton-resmgr server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Storage      storage.Config        `yaml:"storage"`
	ResManager   resmgr.Config         `yaml:"resmgr"`
	Election     leader.ElectionConfig `yaml:"election"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
}
