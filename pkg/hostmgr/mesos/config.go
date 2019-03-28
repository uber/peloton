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

package mesos

// Config for Mesos specific configuration
type Config struct {
	Framework *FrameworkConfig `yaml:"framework"`
	ZkPath    string           `yaml:"zk_path"`
	Encoding  string           `yaml:"encoding"`
}

// FrameworkConfig for framework specific configuration
type FrameworkConfig struct {
	User                        string  `yaml:"user"`
	Name                        string  `yaml:"name"`
	Role                        string  `yaml:"role"`
	Principal                   string  `yaml:"principal"`
	FailoverTimeout             float64 `yaml:"failover_timeout"`
	GPUSupported                bool    `yaml:"gpu_supported"`
	TaskKillingStateSupported   bool    `yaml:"task_killing_state"`
	PartitionAwareSupported     bool    `yaml:"partition_aware"`
	RevocableResourcesSupported bool    `yaml:"revocable_resources"`
	MaxConnectionsToMesosMaster int     `yaml:"max_connections_to_mesos_master"`
}
