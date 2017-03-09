package main

import (
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/leader"
	storage "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all config to run a peloton-jobmgr server.
type Config struct {
	Metrics    metrics.Config        `yaml:"metrics"`
	Storage    storage.Config        `yaml:"storage"`
	Election   leader.ElectionConfig `yaml:"election"`
	JobManager jobmgr.Config         `yaml:"job_manager"`
}
