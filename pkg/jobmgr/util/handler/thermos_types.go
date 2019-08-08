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

package handler

import "go.uber.org/thriftrw/ptr"

// ExecutorData is the main data structure used by Thermos executor,
// represents a single thermos job.
type ExecutorData struct {
	Role                *string            `json:"role,omitempty"`
	Environment         *string            `json:"environment,omitempty"`
	Name                *string            `json:"name,omitempty"`
	Priority            *int32             `json:"priority,omitempty"`
	MaxTaskFailures     *int32             `json:"max_task_failures,omitempty"`
	Production          *bool              `json:"production,omitempty"`
	Tier                *string            `json:"tier,omitempty"`
	HealthCheckConfig   *HealthCheckConfig `json:"health_check_config,omitempty"`
	CronCollisionPolicy *string            `json:"cron_collision_policy,omitempty"`
	EnableHooks         *bool              `json:"enable_hooks,omitempty"`
	Cluster             *string            `json:"cluster,omitempty"`
	Task                *Task              `json:"task,omitempty"`
}

// NewExecutorData initializes a new instance of ExecutorData.
func NewExecutorData() *ExecutorData {
	return &ExecutorData{}
}

// HealthCheckConfig defines the health check rules for a Thermos job.
type HealthCheckConfig struct {
	HealthChecker           *HealthCheckerConfig `json:"health_checker,omitempty"`
	InitialIntervalSecs     *float64             `json:"initial_interval_secs,omitempty"`
	IntervalSecs            *float64             `json:"interval_secs,omitempty"`
	MaxConsecutiveFailures  *int32               `json:"max_consecutive_failures,omitempty"`
	MinConsecutiveSuccesses *int32               `json:"min_consecutive_successes,omitempty"`
	TimeoutSecs             *float64             `json:"timeout_secs,omitempty"`
}

// NewHealthCheckConfig initializes a new instance of HealthCheckConfig
// along with defaults values.
func NewHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		InitialIntervalSecs:     ptr.Float64(15.0),
		IntervalSecs:            ptr.Float64(10.0),
		MaxConsecutiveFailures:  ptr.Int32(3),
		MinConsecutiveSuccesses: ptr.Int32(1),
		TimeoutSecs:             ptr.Float64(20.0),
	}
}

// HealthCheckerConfig defines the actual health check implementation
// (http or shell) to be used inside a HealthCheckConfig.
type HealthCheckerConfig struct {
	Http  *HttpHealthChecker  `json:"http,omitempty"`
	Shell *ShellHealthChecker `json:"shell,omitempty"`
}

// NewHealthCheckerConfig initializes a new instance of HealthCheckerConfig.
func NewHealthCheckerConfig() *HealthCheckerConfig {
	return &HealthCheckerConfig{}
}

// HttpHealthChecker defines the health checker for testing against an HTTP
// endpoint.
type HttpHealthChecker struct {
	Endpoint             *string `json:"endpoint,omitempty"`
	ExpectedResponse     *string `json:"expected_response,omitempty"`
	ExpectedResponseCode *int32  `json:"expected_response_code,omitempty"`
}

// NewHttpHealthChecker initializes a new instance of HttpHealthChecker
// along with default values.
func NewHttpHealthChecker() *HttpHealthChecker {
	return &HttpHealthChecker{
		ExpectedResponseCode: ptr.Int32(200),
	}
}

// ShellHealthChecker defines the health checker for running a shell command.
type ShellHealthChecker struct {
	ShellCommand *string `json:"shell_command,omitempty"`
}

// NewShellHealthChecker initializes a new instance of ShellHealthChecker.
func NewShellHealthChecker() *ShellHealthChecker {
	return &ShellHealthChecker{}
}

// Task represents the task to be run inside a Thermos job.
type Task struct {
	Name             *string       `json:"name,omitempty"`
	FinalizationWait *int32        `json:"finalization_wait,omitempty"`
	MaxFailures      *int32        `json:"max_failures,omitempty"`
	MaxConcurrency   *int32        `json:"max_concurrency,omitempty"`
	Resources        *Resources    `json:"resources,omitempty"`
	Processes        []*Process    `json:"processes,omitempty"`
	Constraints      []*Constraint `json:"constraints,omitempty"`
}

// NewTask initializes a new instance of Task along with default values.
func NewTask() *Task {
	return &Task{
		FinalizationWait: ptr.Int32(30),
		MaxFailures:      ptr.Int32(1),
		MaxConcurrency:   ptr.Int32(0),
	}
}

// Resources represents the task resources.
type Resources struct {
	Cpu       *float64 `json:"cpu,omitempty"`
	RamBytes  *int64   `json:"ram,omitempty"`
	DiskBytes *int64   `json:"disk,omitempty"`
	Gpu       *int32   `json:"gpu,omitempty"`
}

// NewResources initializes a new instance of Resources.
func NewResources() *Resources {
	return &Resources{}
}

// Process represents the single process inside a Thermos task.
type Process struct {
	Cmdline     *string `json:"cmdline,omitempty"`
	Name        *string `json:"name,omitempty"`
	MaxFailures *int32  `json:"max_failures,omitempty"`
	Daemon      *bool   `json:"daemon,omitempty"`
	Ephemeral   *bool   `json:"ephemeral,omitempty"`
	MinDuration *int32  `json:"min_duration,omitempty"`
	Final       *bool   `json:"final,omitempty"`
}

// NewProcess initializes a new instance of Process along with default values.
func NewProcess() *Process {
	return &Process{
		Daemon:      ptr.Bool(false),
		Ephemeral:   ptr.Bool(false),
		Final:       ptr.Bool(false),
		MaxFailures: ptr.Int32(1),
		MinDuration: ptr.Int32(5),
	}
}

// Constraint represents launching constraints for processes defined in
// a Thermos task.
type Constraint struct {
	Order []*string `json:"order,omitempty"`
}

// NewConstraint initializes a new instance of Constraint.
func NewConstraint() *Constraint {
	return &Constraint{}
}
