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

package atop

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewWorkflowState converts aurora update status
// to peloton job update state
func NewWorkflowState(
	astate api.JobUpdateStatus) (stateless.WorkflowState, error) {
	switch astate {
	case api.JobUpdateStatusRollForwardAwaitingPulse:
		return stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD, nil
	case api.JobUpdateStatusRollBackAwaitingPulse:
		return stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD, nil
	case api.JobUpdateStatusRollingForward:
		return stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD, nil
	case api.JobUpdateStatusRolledForward:
		return stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED, nil
	case api.JobUpdateStatusRollingBack:
		return stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD, nil
	case api.JobUpdateStatusRolledBack:
		return stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK, nil
	case api.JobUpdateStatusRollForwardPaused:
		return stateless.WorkflowState_WORKFLOW_STATE_PAUSED, nil
	case api.JobUpdateStatusRollBackPaused:
		return stateless.WorkflowState_WORKFLOW_STATE_PAUSED, nil
	case api.JobUpdateStatusAborted:
		return stateless.WorkflowState_WORKFLOW_STATE_ABORTED, nil
	case api.JobUpdateStatusFailed:
		return stateless.WorkflowState_WORKFLOW_STATE_FAILED, nil
	case api.JobUpdateStatusError:
		return stateless.WorkflowState_WORKFLOW_STATE_FAILED, nil
	default:
		return stateless.WorkflowState_WORKFLOW_STATE_INVALID,
			fmt.Errorf("unknown aurora job update status: %d", astate)
	}
}
