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
)

// NewJobUpdateAction returns the job update action for aurora
// from peloton update action
// TODO: use opaque data to identify is peloton workflow failed on
// rolling forward or rolling backward. For now peloton workflow failed
// is mapped to rolling forward failed for Aurora.
func NewJobUpdateAction(
	s stateless.WorkflowState,
) (*api.JobUpdateAction, error) {

	switch s {
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD:
		return api.JobUpdateActionInstanceUpdating.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED:
		return api.JobUpdateActionInstanceUpdated.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD:
		return api.JobUpdateActionInstanceRollingBack.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK:
		return api.JobUpdateActionInstanceRolledBack.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_ABORTED:
		return api.JobUpdateActionInstanceUpdateFailed.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_FAILED:
		return api.JobUpdateActionInstanceUpdateFailed.Ptr(), nil
	default:
		return nil, fmt.Errorf("unknown job update state %d", s)
	}
}
