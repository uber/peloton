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

package jobsvc

import "github.com/uber/peloton/pkg/common/config"

const (
	_defaultMaxTasksPerJob uint32 = 100000

	// Represents number of goroutine workers to fetch instance workflow events
	_defaultLowInstanceWorkflowEventsWorker  = 25
	_defaultMedInstanceWorkflowEventsWorker  = 50
	_defaultMedInstanceCount                 = 500
	_defaultHighInstanceWorkflowEventsWorker = 100
	_defaultHighInstanceCount                = 1000
)

// Config for job service
type Config struct {
	// Maximum number of tasks allowed per job
	MaxTasksPerJob uint32 `yaml:"max_tasks_per_job"`

	// Flag to enable handling peloton secrets
	EnableSecrets bool `yaml:"enable_secrets"`

	// ThemrosExecutor is config used to generate mesos CommandInfo / ExecutorInfo
	// for Thermos executor
	ThermosExecutor config.ThermosExecutorConfig `yaml:"thermos_executor"`

	// T-shirt sizing for instance count in one job, used to determine
	// how many go-routines to run. Higher the instance count, more go-routines
	// to get data faster from DB.
	MedInstanceCount  int `yaml:"med_instance_count"`
	HighInstanceCount int `yaml:"high_instance_count"`

	// Number of go-routines to get workflow events depending on
	// job's instance count
	LowGetWorkflowEventsWorkers  int `yaml:"low_get_workflow_events_workers"`
	MedGetWorkflowEventsWorkers  int `yaml:"med_get_workflow_events_workers"`
	HighGetWorkflowEventsWorkers int `yaml:"high_get_workflow_events_workers"`
}

func (c *Config) normalize() {
	if c.MaxTasksPerJob == 0 {
		c.MaxTasksPerJob = _defaultMaxTasksPerJob
	}
	if c.MedInstanceCount == 0 {
		c.MedInstanceCount = _defaultMedInstanceCount
	}
	if c.HighInstanceCount == 0 {
		c.HighInstanceCount = _defaultHighInstanceCount
	}
	if c.LowGetWorkflowEventsWorkers == 0 {
		c.LowGetWorkflowEventsWorkers = _defaultLowInstanceWorkflowEventsWorker
	}
	if c.MedGetWorkflowEventsWorkers == 0 {
		c.MedGetWorkflowEventsWorkers = _defaultMedInstanceWorkflowEventsWorker
	}
	if c.HighGetWorkflowEventsWorkers == 0 {
		c.HighGetWorkflowEventsWorkers = _defaultHighInstanceWorkflowEventsWorker
	}
}
