package jobmgr

import (
	"time"

	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/jobsvc"
	"code.uber.internal/infra/peloton/jobmgr/task/deadline"
	"code.uber.internal/infra/peloton/jobmgr/task/placement"
	"code.uber.internal/infra/peloton/jobmgr/task/preemptor"
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

	// Task launcher specific configs
	Placement placement.Config `yaml:"task_launcher"`

	// GoalState configuration
	GoalState goalstate.Config `yaml:"goal_state"`

	// Preemption related config
	Preemptor preemptor.Config `yaml:"task_preemptor"`

	Deadline deadline.Config `yaml:"deadline"`

	// Job service specific configuration
	JobSvcCfg jobsvc.Config `yaml:"job_service"`

	// Period in sec for updating active cache
	ActiveTaskUpdatePeriod time.Duration `yaml:"active_task_update_period"`
}
