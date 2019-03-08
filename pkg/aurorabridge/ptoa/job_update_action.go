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

// NewJobUpdateAction returns the job update action for aurora
// from peloton workflow state
func NewJobUpdateAction(
	s stateless.WorkflowState,
	d *opaquedata.Data,
) (*api.JobUpdateAction, error) {

	rollback := d.ContainsUpdateAction(opaquedata.Rollback)

	switch s {
	case stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		stateless.WorkflowState_WORKFLOW_STATE_PAUSED:
		if rollback {
			return api.JobUpdateActionInstanceRollingBack.Ptr(), nil
		}
		return api.JobUpdateActionInstanceUpdating.Ptr(), nil

	case stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED:
		if rollback {
			return api.JobUpdateActionInstanceRolledBack.Ptr(), nil
		}
		return api.JobUpdateActionInstanceUpdated.Ptr(), nil

	case stateless.WorkflowState_WORKFLOW_STATE_FAILED,
		stateless.WorkflowState_WORKFLOW_STATE_ABORTED:
		if rollback {
			return api.JobUpdateActionInstanceRollbackFailed.Ptr(), nil
		}
		return api.JobUpdateActionInstanceUpdateFailed.Ptr(), nil

	case stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD:
		return api.JobUpdateActionInstanceRollingBack.Ptr(), nil
	case stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK:
		return api.JobUpdateActionInstanceRolledBack.Ptr(), nil

	default:
		return nil, fmt.Errorf("unknown peloton workflow state %d", s)
	}
}
