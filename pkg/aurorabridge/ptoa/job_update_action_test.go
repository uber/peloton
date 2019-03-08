package ptoa

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
)

func TestNewJobUpdateAction(t *testing.T) {
	testCases := []struct {
		name       string
		state      stateless.WorkflowState
		actions    []opaquedata.UpdateAction
		wantAction *api.JobUpdateAction
	}{
		// INITIALIZED
		{
			"initialized to rolling forward",
			stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
			nil,
			api.JobUpdateActionInstanceUpdating.Ptr(),
		},
		{
			"initialized to rolling backward",
			stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRollingBack.Ptr(),
		},

		// ROLLING_FORWARD
		{
			"rolling forward to rolling forward",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			nil,
			api.JobUpdateActionInstanceUpdating.Ptr(),
		}, {
			"rolling forward to rolling back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRollingBack.Ptr(),
		},

		// PAUSED
		{
			"paused to updating",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			nil,
			api.JobUpdateActionInstanceUpdating.Ptr(),
		},
		{
			"paused to rolling back",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRollingBack.Ptr(),
		},

		// ROLLING_BACKWARD
		{
			"rolling backward to rolling back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
			nil,
			api.JobUpdateActionInstanceRollingBack.Ptr(),
		},

		// ROLLED_BACK
		{
			"rolled back to rolled back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK,
			nil,
			api.JobUpdateActionInstanceRolledBack.Ptr(),
		},

		// SUCCEEDED
		{
			"succeeded to rolled back",
			stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRolledBack.Ptr(),
		},
		{
			"succeeded to rolled forward",
			stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
			nil,
			api.JobUpdateActionInstanceUpdated.Ptr(),
		},

		// ABORTED
		{
			"roll back to aborted",
			stateless.WorkflowState_WORKFLOW_STATE_ABORTED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRollbackFailed.Ptr(),
		},
		{
			"roll forward to aborted",
			stateless.WorkflowState_WORKFLOW_STATE_ABORTED,
			nil,
			api.JobUpdateActionInstanceUpdateFailed.Ptr(),
		},

		// FAILED
		{
			"rolling forward to succeeded",
			stateless.WorkflowState_WORKFLOW_STATE_FAILED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateActionInstanceRollbackFailed.Ptr(),
		},
		{
			"rolling backward to succeeded",
			stateless.WorkflowState_WORKFLOW_STATE_FAILED,
			nil,
			api.JobUpdateActionInstanceUpdateFailed.Ptr(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &opaquedata.Data{UpdateActions: tc.actions}
			s, err := NewJobUpdateAction(tc.state, d)
			require.NoError(t, err)
			require.Equal(t, tc.wantAction, s, "want %s, got %s", tc.wantAction, s)
		})
	}
}
