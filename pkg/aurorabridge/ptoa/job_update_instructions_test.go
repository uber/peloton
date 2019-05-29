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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

// TestNewJobUpdateInstructions checks success case for
// NewJobUpdateInstructions util function.
func TestNewJobUpdateInstructions(t *testing.T) {
	um := []*api.Metadata{{
		Key:   ptr.String("key"),
		Value: ptr.String("value"),
	}}
	d := &opaquedata.Data{
		UpdateID:       uuid.New(),
		UpdateMetadata: um,
	}
	od, err := d.Serialize()
	assert.NoError(t, err)

	prevWorkflow := fixture.PelotonWorkflowInfo("")
	prevWorkflow.OpaqueData = od

	workflow := fixture.PelotonWorkflowInfo("")
	workflow.InstancesAdded = []*pod.InstanceIDRange{{From: 0, To: 2}}
	workflow.InstancesUpdated = []*pod.InstanceIDRange{{From: 4, To: 6}}
	workflow.InstancesRemoved = []*pod.InstanceIDRange{{From: 8, To: 10}}

	instructions, err := NewJobUpdateInstructions(prevWorkflow, workflow)
	assert.NoError(t, err)
	assert.Equal(t, &api.JobUpdateInstructions{
		InitialState: []*api.InstanceTaskConfig{
			{
				Task: &api.TaskConfig{
					Metadata: um,
				},
				Instances: []*api.Range{
					{First: ptr.Int32(4), Last: ptr.Int32(6)},
					{First: ptr.Int32(8), Last: ptr.Int32(10)},
				},
			},
		},
		DesiredState: &api.InstanceTaskConfig{
			Instances: []*api.Range{
				{First: ptr.Int32(4), Last: ptr.Int32(6)},
				{First: ptr.Int32(0), Last: ptr.Int32(2)},
			},
		},
		Settings: NewJobUpdateSettings(nil),
	}, instructions)
}

// TestNewJobUpdateInstructions_NoPrevWorkflow checks NewJobUpdateInstructions
// util function should not return nil InitialState in JobUpdateInstructions
// when previous workflow is not passed.
func TestNewJobUpdateInstructions_NoPrevWorkflow(t *testing.T) {
	workflow := fixture.PelotonWorkflowInfo("")

	instructions, err := NewJobUpdateInstructions(nil, workflow)
	assert.NoError(t, err)
	assert.NotNil(t, instructions.GetInitialState())
	assert.Empty(t, instructions.GetInitialState())
}

// TestNewJobUpdateInstructions_EmptyOpaqueData checks NewJobUpdateInstructions
// util function should not include InitialState if OpaqueData in prevWorkflow
// is empty.
func TestNewJobUpdateInstructions_EmptyOpaqueData(t *testing.T) {
	prevWorkflow := fixture.PelotonWorkflowInfo("")
	prevWorkflow.OpaqueData = &peloton.OpaqueData{Data: ""}

	workflow := fixture.PelotonWorkflowInfo("")
	workflow.InstancesAdded = []*pod.InstanceIDRange{{From: 0, To: 2}}
	workflow.InstancesUpdated = []*pod.InstanceIDRange{{From: 4, To: 6}}
	workflow.InstancesRemoved = []*pod.InstanceIDRange{{From: 8, To: 10}}

	instructions, err := NewJobUpdateInstructions(prevWorkflow, workflow)
	assert.NoError(t, err)
	assert.Equal(t, &api.JobUpdateInstructions{
		InitialState: []*api.InstanceTaskConfig{},
		DesiredState: &api.InstanceTaskConfig{
			Instances: []*api.Range{
				{First: ptr.Int32(4), Last: ptr.Int32(6)},
				{First: ptr.Int32(0), Last: ptr.Int32(2)},
			},
		},
		Settings: NewJobUpdateSettings(nil),
	}, instructions)
}
