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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewConfigGroup gets new config group for provided instance ranges and
// aurora task config
func NewConfigGroup(
	jobKey *api.JobKey,
	instanceIDList []uint32,
	podSpec *pod.PodSpec,
) (*api.ConfigGroup, error) {

	ranges := NewRange(instanceIDList)

	taskConfig, err := NewTaskConfig(jobKey, podSpec)
	if err != nil {
		return nil, err
	}

	return &api.ConfigGroup{
		Config:    taskConfig,
		Instances: ranges,
	}, nil
}
