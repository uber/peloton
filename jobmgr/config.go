package jobmgr

import (
	"code.uber.internal/infra/peloton/common/metrics"
	sc "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all configuration necessary to run a job manager server.
type Config struct {
	Metrics    metrics.Config   `yaml:"metrics"`
	Storage    sc.StorageConfig `yaml:"storage"`
	JobManager JobManagerConfig `yaml:"job_manager"`
	//TODO: add service discovery for resmgr / hostmgr
}

// JobManagerConfig is JobManager specific configuration
type JobManagerConfig struct {
	Port int `yaml:"port"`

	// FIXME(gabe): this isnt really the DB write concurrency. This is only used for processing task updates
	// and should be moved into the storage namespace, and made clearer what this controls (threads? rows? statements?)
	DbWriteConcurrency int `yaml:"db_write_concurrency"`
}
