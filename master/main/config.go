package main

import (
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master"
	"code.uber.internal/infra/peloton/placement"
	"code.uber.internal/infra/peloton/resmgr"
	storage "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all config necessary to run a peloton-master server.
type Config struct {
	Metrics     metrics.Config        `yaml:"metrics"`
	Storage     storage.Config        `yaml:"storage"`
	Master      master.Config         `yaml:"master"`
	Mesos       mesos.Config          `yaml:"mesos"`
	JobManager  jobmgr.Config         `yaml:"job_manager"`
	Placement   placement.Config      `yaml:"placement"`
	ResManager  resmgr.Config         `yaml:"resmgr"`
	Election    leader.ElectionConfig `yaml:"election"`
	HostManager hostmgr.Config        `yaml:"hostmgr"`
}
