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
	"sort"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"

	"go.uber.org/thriftrw/ptr"
)

// NewJobUpdateDetails converts a workflow into JobUpdateDetails.
func NewJobUpdateDetails(
	k *api.JobKey,
	prevWorkflow *stateless.WorkflowInfo,
	workflow *stateless.WorkflowInfo,
) (*api.JobUpdateDetails, error) {

	summary, err := NewJobUpdateSummary(k, workflow)
	if err != nil {
		return nil, fmt.Errorf("new job update summary: %s", err)
	}

	d, err := opaquedata.Deserialize(workflow.GetOpaqueData())
	if err != nil {
		return nil, fmt.Errorf("deserialize opaque data: %s", err)
	}

	workflowEvents := workflow.GetEvents()
	updateEvents := []*api.JobUpdateEvent{}
	for i := range workflowEvents {
		// Assuming peloton workflow events are sorted in descending order
		pe := workflowEvents[len(workflowEvents)-1-i]
		ae, err := NewJobUpdateEvent(pe, d)
		if err != nil {
			return nil, fmt.Errorf("new job update event: %s", err)
		}
		updateEvents = append(updateEvents, ae)
	}

	jobUpdateInstructions, err := NewJobUpdateInstructions(
		prevWorkflow,
		workflow,
	)
	if err != nil {
		return nil, err
	}

	instanceUpdateEvents := []*api.JobInstanceUpdateEvent{}
	for _, events := range workflow.GetInstanceEvents() {
		id := events.GetInstanceId()
		instanceEvents := events.GetEvents()
		for i := range instanceEvents {
			// Assuming peloton instance events are sorted in descending order
			pe := instanceEvents[len(instanceEvents)-1-i]
			ae, err := NewJobInstanceUpdateEvent(id, pe, d)
			if err != nil {
				return nil, fmt.Errorf("new job instance update event: %s", err)
			}
			instanceUpdateEvents = append(instanceUpdateEvents, ae)
		}
	}
	// Sorted by ascending timestamp for all instances combined.
	sort.Sort(JobInstanceUpdateEventsByTimestamp(instanceUpdateEvents))

	return &api.JobUpdateDetails{
		Update: &api.JobUpdate{
			Summary:      summary,
			Instructions: jobUpdateInstructions,
		},
		UpdateEvents:   updateEvents,
		InstanceEvents: instanceUpdateEvents,
	}, nil
}

var _rollbackAndTerminalStatuses = common.NewJobUpdateStatusSet(
	api.JobUpdateStatusRollingBack,
	api.JobUpdateStatusRollBackPaused,
	api.JobUpdateStatusRollBackAwaitingPulse,
	api.JobUpdateStatusRolledBack,
	api.JobUpdateStatusAborted,
	api.JobUpdateStatusError,
	api.JobUpdateStatusFailed,
)

// JoinRollbackJobUpdateDetails joins two updates which together represent an
// update followed by a manually rollback. Assumes that both updates have the
// same update id.
func JoinRollbackJobUpdateDetails(d1, d2 *api.JobUpdateDetails) *api.JobUpdateDetails {
	// Swap d1 and d2 such that d1 precedes d2.
	t1 := d1.GetUpdate().GetSummary().GetState().GetCreatedTimestampMs()
	t2 := d2.GetUpdate().GetSummary().GetState().GetCreatedTimestampMs()
	if t1 > t2 {
		d1, d2 = d2, d1
	}

	// Stitch together the events of the two updates in descending order.
	updateEvents := []*api.JobUpdateEvent{}
	for _, e := range d1.GetUpdateEvents() {
		if _rollbackAndTerminalStatuses.Has(e.GetStatus()) {
			// Ignore any rollback / terminal statuses from the first
			// update's events.
			continue
		}
		updateEvents = append(updateEvents, e)
	}
	for _, e := range d2.GetUpdateEvents() {
		// NOTE: Assumes these have already been converted to the proper
		// rollback statuses due to the presence of rollback opaque data.
		updateEvents = append(updateEvents, e)
	}

	// Stitch together the instance events of the two updates in descending order.
	instanceEvents := []*api.JobInstanceUpdateEvent{}
	for _, e := range d1.GetInstanceEvents() {
		instanceEvents = append(instanceEvents, e)
	}
	for _, e := range d2.GetInstanceEvents() {
		// NOTE: Assumes these have already been converted to the proper
		// rollback actions due to the presence of rollback opaque data.
		instanceEvents = append(instanceEvents, e)
	}

	s1 := d1.GetUpdate().GetSummary()
	s2 := d2.GetUpdate().GetSummary()

	return &api.JobUpdateDetails{
		Update: &api.JobUpdate{
			Summary: &api.JobUpdateSummary{
				Key:  s1.GetKey(),
				User: ptr.String(s1.GetUser()),
				State: &api.JobUpdateState{
					Status:                  s2.GetState().GetStatus().Ptr(),
					CreatedTimestampMs:      ptr.Int64(s1.GetState().GetCreatedTimestampMs()),
					LastModifiedTimestampMs: ptr.Int64(s2.GetState().GetLastModifiedTimestampMs()),
				},
				Metadata: s1.GetMetadata(),
			},
			Instructions: d1.GetUpdate().GetInstructions(),
		},
		UpdateEvents:   updateEvents,
		InstanceEvents: instanceEvents,
	}
}
