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
	"github.com/uber/peloton/aurorabridge/opaquedata"
)

// NewJobUpdateStatus translates peloton job update state to
// aurora job update status.
// Peloton workflows in INITIALIZED state are to be ignored, and state translation
// must not be performed on this state.
// TODO: If workflow state is in PAUSED state, use opaque data
// to identify if workflow was ROLLING_FORWARD or ROLLING_BACKWARD
// since Aurora identifies PAUSED state for them separately whereas
// Peloton does not.
func NewJobUpdateStatus(
	s stateless.WorkflowState,
	d *opaquedata.Data,
) (api.JobUpdateStatus, error) {

	switch s {
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD:
		return api.JobUpdateStatusRollingForward, nil
	case stateless.WorkflowState_WORKFLOW_STATE_PAUSED:
		// TODO(codyg): Paused rollback states.
		if d.IsLatestUpdateAction(opaquedata.StartPulsed) {
			return api.JobUpdateStatusRollForwardAwaitingPulse, nil
		}
		return api.JobUpdateStatusRollForwardPaused, nil
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD:
		return api.JobUpdateStatusRollingBack, nil
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK:
		return api.JobUpdateStatusRolledBack, nil
	case stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED:
		return api.JobUpdateStatusRolledForward, nil
	case stateless.WorkflowState_WORKFLOW_STATE_ABORTED:
		return api.JobUpdateStatusAborted, nil
	case stateless.WorkflowState_WORKFLOW_STATE_FAILED:
		return api.JobUpdateStatusFailed, nil
	default:
		return api.JobUpdateStatusError,
			fmt.Errorf("unknown peloton workflow state %d", s)
	}
}
