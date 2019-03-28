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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/common/util"
)

// NewConfigGroup gets new config group for provided instance ranges and
// aurora task config
func NewConfigGroup(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
	instanceIDList []uint32,
) (*api.ConfigGroup, error) {

	ranges := NewRange(util.ConvertInstanceIDListToInstanceRange(instanceIDList))

	taskConfig, err := NewTaskConfig(jobSummary, podSpec)
	if err != nil {
		return nil, err
	}

	return &api.ConfigGroup{
		Config:    taskConfig,
		Instances: ranges,
	}, nil
}

// NewConfigGroupWithoutTaskConfig returns a list of ConfigGroup without
// TaskConfig field based on input InstanceIDRange from Peloton.
// If the input is empty, nil will be returned.
func NewConfigGroupWithoutTaskConfig(
	instanceIDRange []*pod.InstanceIDRange,
) []*api.ConfigGroup {
	if len(instanceIDRange) == 0 {
		return nil
	}

	return []*api.ConfigGroup{{
		Instances: NewRange(instanceIDRange),
	}}
}
