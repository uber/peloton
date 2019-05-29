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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
)

// NewJobUpdateInstructions gets new job update instructions
// Since Aggregator ignores task config, it is not set.
func NewJobUpdateInstructions(
	prevWorkflow *stateless.WorkflowInfo,
	workflow *stateless.WorkflowInfo,
) (*api.JobUpdateInstructions, error) {

	initialState := make([]*api.InstanceTaskConfig, 0)

	// Skip populating initialState if
	// 1. Current workflow is the first update (i.e. no prevWorkflow)
	// 2. OpaqueData is empty - we do not expect this case to happen, but
	//    if it does, skip to avoid crashing aggregator
	if prevWorkflow != nil && len(prevWorkflow.GetOpaqueData().GetData()) > 0 {
		d, err := opaquedata.Deserialize(prevWorkflow.GetOpaqueData())
		if err != nil {
			return nil, fmt.Errorf("deserialize opaque data: %s", err)
		}

		initialState = append(initialState, &api.InstanceTaskConfig{
			Instances: NewRange(append(workflow.GetInstancesUpdated(),
				workflow.GetInstancesRemoved()...)),
			Task: &api.TaskConfig{
				Metadata: d.UpdateMetadata,
			},
		})
	}

	return &api.JobUpdateInstructions{
		InitialState: initialState,
		DesiredState: &api.InstanceTaskConfig{
			Instances: NewRange(append(workflow.GetInstancesUpdated(),
				workflow.GetInstancesAdded()...)),
		},
		Settings: NewJobUpdateSettings(workflow.GetUpdateSpec()),
	}, nil
}
