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

package aurorabridge

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/pkg/common/config"
)

// ServiceHandlerConfig defines ServiceHandler configuration.
type ServiceHandlerConfig struct {
	GetJobUpdateWorkers           int `yaml:"get_job_update_workers"`
	GetJobsWorkers                int `yaml:"get_jobs_workers"`
	GetJobSummaryWorkers          int `yaml:"get_job_summary_workers"`
	StopPodWorkers                int `yaml:"stop_pod_workers"`
	CreateJobSpecForUpdateWorkers int `yaml:"create_job_spec_for_update_workers"`

	// Config for number of workers for getTasksWithoutConfigs endpoint.
	GetTasksWithoutConfigsWorkers         int `yaml:"get_tasks_without_configs_workers"`
	GetTasksWithoutConfigsMediumWorkers   int `yaml:"get_tasks_without_configs_medium_workers"`
	GetTasksWithoutConfigsMediumThreshold int `yaml:"get_tasks_without_configs_medium_threshold"`
	GetTasksWithoutConfigsLargeWorkers    int `yaml:"get_tasks_without_configs_large_workers"`
	GetTasksWithoutConfigsLargeThreshold  int `yaml:"get_tasks_without_configs_large_threshold"`

	// getTasksWithoutConfigs task querying depth. It limits the number
	// of pods to be included in the return result - return pods from
	// current run if set to 1, return pods from current and previous one
	// run if set to 2, etc.
	// In Aurora, getTasksWithoutConfigs will return all the current and
	// completed tasks stored in the DB. However, in Peloton, since we
	// keep the complete history of pods, this parameter is used to limit
	// the number of pods returned.
	PodRunsDepth int `yaml:"pod_runs_depth"`

	// Maximum number of pods that will get returned, while meeting
	// minPodRunsDepth requirement.
	GetTasksPodMax int `yaml:"get_tasks_pod_max"`

	// QueryJobsLimit specifies Limit parameter passed to QueryJobs request
	QueryJobsLimit uint32 `yaml:"query_jobs_limit"`

	// InstanceEventsLimit specifies the limit on number of events per instance
	InstanceEventsLimit uint32 `yaml:"instance_events_limit"`

	// UpdatesLimit specifies the limit on number of updates to include per job
	UpdatesLimit uint32 `yaml:"updates_limit"`

	// ThemrosExecutor is config used to generate mesos CommandInfo / ExecutorInfo
	// for Thermos executor
	ThermosExecutor config.ThermosExecutorConfig `yaml:"thermos_executor"`

	// Enable Peloton inplace update
	EnableInPlace bool `yaml:"enable-inplace-update"`
}

func (c *ServiceHandlerConfig) normalize() {
	if c.GetJobUpdateWorkers == 0 {
		c.GetJobUpdateWorkers = 25
	}
	if c.GetJobsWorkers == 0 {
		c.GetJobsWorkers = 25
	}
	if c.GetJobSummaryWorkers == 0 {
		c.GetJobSummaryWorkers = 25
	}
	if c.GetTasksWithoutConfigsWorkers == 0 {
		c.GetTasksWithoutConfigsWorkers = 100
	}
	if c.GetTasksWithoutConfigsMediumWorkers == 0 {
		c.GetTasksWithoutConfigsMediumWorkers = 250
	}
	if c.GetTasksWithoutConfigsMediumThreshold == 0 {
		c.GetTasksWithoutConfigsMediumThreshold = 500
	}
	if c.GetTasksWithoutConfigsLargeWorkers == 0 {
		c.GetTasksWithoutConfigsLargeWorkers = 500
	}
	if c.GetTasksWithoutConfigsLargeThreshold == 0 {
		c.GetTasksWithoutConfigsLargeThreshold = 1000
	}
	if c.StopPodWorkers == 0 {
		c.StopPodWorkers = 25
	}
	if c.CreateJobSpecForUpdateWorkers == 0 {
		c.CreateJobSpecForUpdateWorkers = 25
	}
	if c.PodRunsDepth <= 0 {
		c.PodRunsDepth = 1
	}
	if c.GetTasksPodMax == 0 {
		c.GetTasksPodMax = 1000
	}
	if c.QueryJobsLimit == 0 {
		c.QueryJobsLimit = 1000
	}
	if c.InstanceEventsLimit == 0 {
		c.InstanceEventsLimit = 100
	}
	if c.UpdatesLimit == 0 {
		c.UpdatesLimit = 10
	}
}

func (c *ServiceHandlerConfig) getTasksWithoutConfigsWorkers(size int) int {
	workers := c.GetTasksWithoutConfigsWorkers
	if size >= c.GetTasksWithoutConfigsLargeThreshold {
		workers = c.GetTasksWithoutConfigsLargeWorkers
	} else if size >= c.GetTasksWithoutConfigsMediumThreshold {
		workers = c.GetTasksWithoutConfigsMediumWorkers
	}
	return workers
}

func (c *ServiceHandlerConfig) validate() error {
	if err := c.ThermosExecutor.Validate(); err != nil {
		return err
	}
	return nil
}

// RespoolLoaderConfig defines RespoolLoader configuration.
type RespoolLoaderConfig struct {
	RetryInterval      time.Duration      `yaml:"retry_interval"`
	RespoolPath        string             `yaml:"respool_path"`
	GPURespoolPath     string             `yaml:"gpu_respool_path"`
	DefaultRespoolSpec DefaultRespoolSpec `yaml:"default_respool_spec"`
}

// DefaultRespoolSpec defines parameters used to create a default respool for
// bridge when boostrapping a new cluster.
type DefaultRespoolSpec struct {
	OwningTeam      string                    `yaml:"owning_team"`
	LDAPGroups      []string                  `yaml:"ldap_groups"`
	Description     string                    `yaml:"description"`
	Resources       []*respool.ResourceConfig `yaml:"resources"`
	Policy          respool.SchedulingPolicy  `yaml:"policy"`
	ControllerLimit *respool.ControllerLimit  `yaml:"controller_limit"`
	SlackLimit      *respool.SlackLimit       `yaml:"slack_limit"`
}

func (c *RespoolLoaderConfig) normalize() {
	if c.RetryInterval == 0 {
		c.RetryInterval = 5 * time.Second
	}
}

// EventPublisherConfig represents config for publishing task state change
// events to kafks
type EventPublisherConfig struct {
	// KafkaURL represents the stream on which task state changes to publish
	KafkaURL string `yaml:"kafka_url"`

	// PublishEvents defines whether to publish task state changes to kafka
	PublishEvents bool `yaml:"publish_events"`

	// GRPCMsgSize defines the max payload size that can be send and recv
	GRPCMsgSize int `yaml:"grpc_msg_size"`
}
