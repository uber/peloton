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

package ptoa

import (
	"fmt"

	"go.uber.org/thriftrw/ptr"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/aurorabridge/label"
)

// NewTaskConfig returns aurora task config for a provided peloton pod spec
func NewTaskConfig(
	jobKey *api.JobKey,
	podSpec *pod.PodSpec) (*api.TaskConfig, error) {

	metadata, err := label.ParseAuroraMetadata(podSpec.GetLabels())
	if err != nil {
		return nil, fmt.Errorf("parse aurora metadata: %s", err)
	}

	return &api.TaskConfig{
		Job:       jobKey,
		IsService: ptr.Bool(true),
		Metadata:  metadata,
		// TODO: sort these attributes in to be implemented or not used (maybe remove them)
		// Refactor scheduled_task to use NewTaskConfig()
		// NumCpus:          nil,
		// RamMb:            nil,
		// DiskMb:           nil,
		// Priority:         nil,
		// MaxTaskFailures:  nil,
		// Production:       nil,
		// Tier:             nil,
		// Resources:        nil,
		// Constraints:      nil,
		// RequestedPorts:   map[string]struct{}{},
		// ExecutorConfig:   nil,
		// Container: nil,
	}, nil
}
