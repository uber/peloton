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

package progress

import "time"

const (
	_defaultWorkflowProgressCheckPeriod = 30 * time.Minute
	_defaultStaleWorkflowThreshold      = 30 * time.Minute
)

type Config struct {
	WorkflowProgressCheckPeriod time.Duration `yaml:"workflow_progress_check_period"`
	StaleWorkflowThreshold      time.Duration `yaml:"stale_workflow_threshold"`
}

func (c *Config) normalize() {
	if c.WorkflowProgressCheckPeriod == time.Duration(0) {
		c.WorkflowProgressCheckPeriod = _defaultWorkflowProgressCheckPeriod
	}

	if c.StaleWorkflowThreshold == time.Duration(0) {
		c.StaleWorkflowThreshold = _defaultStaleWorkflowThreshold
	}
}
