package jobmgr

import (
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/jobmgr/task/preemptor"
	"code.uber.internal/infra/peloton/jobmgr/upgrade"
)

// Config is JobManager specific configuration
type Config struct {
	// HTTP port which JobMgr is listening on
	HTTPPort int `yaml:"http_port"`

	// gRPC port which JobMgr is listening on
	GRPCPort int `yaml:"grpc_port"`

	// FIXME(gabe): this isnt really the DB write concurrency. This is
	// only used for processing task updates and should be moved into
	// the storage namespace, and made clearer what this controls
	// (threads? rows? statements?)
	DbWriteConcurrency int `yaml:"db_write_concurrency"`

	// Job general configuration.
	Job job.Config

	// Task launcher specific configs
	TaskLauncher launcher.Config `yaml:"task_launcher"`

	// GoalState configuration
	GoalState goalstate.Config `yaml:"goal_state"`

	// Upgrade specific configuration.
	Upgrade upgrade.Config

	// Preemption related config
	Preemptor preemptor.Config `yaml:"task_preemptor"`
}
