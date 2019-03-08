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

// NewJobUpdateStatus translates peloton job update state to
// aurora job update status.
func NewJobUpdateStatus(
	s stateless.WorkflowState,
	d *opaquedata.Data,
) (api.JobUpdateStatus, error) {

	rollback := d.ContainsUpdateAction(opaquedata.Rollback)

	awaitingPulse :=
		d.ContainsUpdateAction(opaquedata.StartPulsed) &&
			!d.ContainsUpdateAction(opaquedata.Pulse)

	switch s {
	// Treat INITIALIZED and ROLLING_FORWARD as the same state, since there is
	// no equivalent INITIALIZED state in Aurora. Note, updates started in
	// a paused state (e.g. awaiting pulse) skip INITIALIZED.
	case stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD:
		if rollback {
			return api.JobUpdateStatusRollingBack, nil
		}
		return api.JobUpdateStatusRollingForward, nil

	case stateless.WorkflowState_WORKFLOW_STATE_PAUSED:
		if rollback && awaitingPulse {
			return api.JobUpdateStatusRollBackAwaitingPulse, nil
		} else if rollback && !awaitingPulse {
			return api.JobUpdateStatusRollBackPaused, nil
		} else if !rollback && awaitingPulse {
			return api.JobUpdateStatusRollForwardAwaitingPulse, nil
		}
		// !rollback && !awaitingPulse
		return api.JobUpdateStatusRollForwardPaused, nil

	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD:
		return api.JobUpdateStatusRollingBack, nil

	case stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK:
		return api.JobUpdateStatusRolledBack, nil

	case stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED:
		if rollback {
			return api.JobUpdateStatusRolledBack, nil
		}
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
