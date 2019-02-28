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

const (
	_defaultMaxTasksPerJob uint32 = 100000
)

// Config for job service
type Config struct {
	// Maximum number of tasks allowed per job
	MaxTasksPerJob uint32 `yaml:"max_tasks_per_job"`

	// Flag to enable handling peloton secrets
	EnableSecrets bool `yaml:"enable_secrets"`
}

func (c *Config) normalize() {
	if c.MaxTasksPerJob == 0 {
		c.MaxTasksPerJob = _defaultMaxTasksPerJob
	}
}
