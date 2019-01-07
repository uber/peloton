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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/respool"
)

// BootstrapConfig defines configuration for boostrapping bridge.
type BootstrapConfig struct {
	Timeout            time.Duration      `yaml:"timeout"`
	RespoolPath        string             `yaml:"respool_path"`
	DefaultRespoolSpec DefaultRespoolSpec `yaml:"default_respool_spec"`
}

// DefaultRespoolSpec defines parameters used to create a default respool for
// bridge when boostrapping a new cluster.
type DefaultRespoolSpec struct {
	OwningTeam      string                   `yaml:"owning_team"`
	LDAPGroups      []string                 `yaml:"ldap_groups"`
	Description     string                   `yaml:"description"`
	Resources       []*respool.ResourceSpec  `yaml:"resources"`
	Policy          respool.SchedulingPolicy `yaml:"policy"`
	ControllerLimit *respool.ControllerLimit `yaml:"controller_limit"`
	SlackLimit      *respool.SlackLimit      `yaml:"slack_limit"`
}

func (c *BootstrapConfig) normalize() {
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
}
