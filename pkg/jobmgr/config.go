// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobmgr

import (
	"time"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc"
	"github.com/uber/peloton/pkg/jobmgr/task/deadline"
	"github.com/uber/peloton/pkg/jobmgr/task/evictor"
	"github.com/uber/peloton/pkg/jobmgr/task/placement"
	"github.com/uber/peloton/pkg/jobmgr/watchsvc"
	"github.com/uber/peloton/pkg/jobmgr/workflow/progress"
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

	// Eviction related config
	Evictor evictor.Config `yaml:"task_evictor"`

	Deadline deadline.Config `yaml:"deadline"`

	// Job service specific configuration
	JobSvcCfg jobsvc.Config `yaml:"job_service"`

	// Watch API specific configuration
	Watch watchsvc.Config `yaml:"watch"`

	// WorkflowProgressCheck specific configuration
	WorkflowProgressCheck progress.Config `yaml:"workflow_progress_check"`

	// Period in sec for updating active cache
	ActiveTaskUpdatePeriod time.Duration `yaml:"active_task_update_period"`

	// HostManagerAPIVersion is the API version that the Resource Manager
	// should use to talk to Host Manager.
	HostManagerAPIVersion api.Version `yaml:"hostmgr_api_version"`

	// ThemrosExecutor is config used to generate mesos CommandInfo / ExecutorInfo
	// for Thermos executor
	ThermosExecutor config.ThermosExecutorConfig `yaml:"thermos_executor"`
}
