package main

import (
	"github.com/uber/peloton/common/health"
	"github.com/uber/peloton/common/logging"
	"github.com/uber/peloton/common/metrics"
	"github.com/uber/peloton/jobmgr"
	"github.com/uber/peloton/leader"
	storage "github.com/uber/peloton/storage/config"
)

// Config holds all config to run a peloton-jobmgr server.
type Config struct {
	Metrics      metrics.Config        `yaml:"metrics"`
	Storage      storage.Config        `yaml:"storage"`
	Election     leader.ElectionConfig `yaml:"election"`
	JobManager   jobmgr.Config         `yaml:"job_manager"`
	Health       health.Config         `yaml:"health"`
	SentryConfig logging.SentryConfig  `yaml:"sentry"`
}
