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
)

// ServiceHandlerConfig defines ServiceHandler configuration.
type ServiceHandlerConfig struct {
	GetJobUpdateWorkers int `yaml:"get_job_update_workers"`
	StopPodWorkers      int `yaml:"stop_pod_workers"`
}

func (c *ServiceHandlerConfig) normalize() {
	if c.GetJobUpdateWorkers == 0 {
		c.GetJobUpdateWorkers = 25
	}
	if c.StopPodWorkers == 0 {
		c.StopPodWorkers = 25
	}
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
