package main

import (
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr"
	storage "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all configs to run a peloton-resmgr server.
type Config struct {
	Metrics    metrics.Config        `yaml:"metrics"`
	Storage    storage.Config        `yaml:"storage"`
	ResManager resmgr.Config         `yaml:"resmgr"`
	Election   leader.ElectionConfig `yaml:"election"`
}
