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

	"github.com/uber/peloton/pkg/aurorabridge/atop"
)

// ServiceHandlerConfig defines ServiceHandler configuration.
type ServiceHandlerConfig struct {
	GetJobUpdateWorkers           int `yaml:"get_job_update_workers"`
	GetTasksWithoutConfigsWorkers int `yaml:"get_tasks_without_configs_workers"`
	StopPodWorkers                int `yaml:"stop_pod_workers"`
	CreateJobSpecForUpdateWorkers int `yaml:"create_job_spec_for_update_workers"`

	// getTasksWithoutConfigs task querying depth. It limits the number
	// of pods to be included in the return result - return pods from
	// current run if set to 1, return pods from current and previous one
	// run if set to 2, etc.
	// In Aurora, getTasksWithoutConfigs will return all the current and
	// completed tasks stored in the DB. However, in Peloton, since we
	// keep the complete history of pods, this parameter is used to limit
	// the number of pods returned.
	PodRunsDepth int `yaml:"pod_runs_depth"`

	ThermosExecutor atop.ThermosExecutorConfig `yaml:"thermos_executor"`
}

func (c *ServiceHandlerConfig) normalize() {
	if c.GetJobUpdateWorkers == 0 {
		c.GetJobUpdateWorkers = 25
	}
	if c.GetTasksWithoutConfigsWorkers == 0 {
		c.GetTasksWithoutConfigsWorkers = 25
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
