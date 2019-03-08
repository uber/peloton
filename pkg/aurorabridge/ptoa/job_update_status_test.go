package ptoa

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
)

func TestNewJobUpdateStatus(t *testing.T) {
	testCases := []struct {
		name       string
		state      stateless.WorkflowState
		actions    []opaquedata.UpdateAction
		wantStatus api.JobUpdateStatus
	}{
		// INITIALIZED
		{
			"initialized to rolling forward",
			stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
			nil,
			api.JobUpdateStatusRollingForward,
		},

		// ROLLING_FORWARD
		{
			"rolling forward to rolling forward",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			nil,
			api.JobUpdateStatusRollingForward,
		}, {
			"rolling forward to rolling back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateStatusRollingBack,
		},

		// PAUSED
		{
			"paused to roll back awaiting pulse",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{opaquedata.StartPulsed, opaquedata.Rollback},
			api.JobUpdateStatusRollBackAwaitingPulse,
		}, {
			"paused no pulse to roll back paused",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateStatusRollBackPaused,
		}, {
			"paused already pulsed to roll back paused",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{
				opaquedata.StartPulsed, opaquedata.Rollback, opaquedata.Pulse},
			api.JobUpdateStatusRollBackPaused,
		}, {
			"paused to roll forward awaiting pulse",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{opaquedata.StartPulsed},
			api.JobUpdateStatusRollForwardAwaitingPulse,
		}, {
			"paused no pulse to roll forward paused",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			nil,
			api.JobUpdateStatusRollForwardPaused,
		}, {
			"paused already pulsed to roll forward paused",
			stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
			[]opaquedata.UpdateAction{opaquedata.StartPulsed, opaquedata.Pulse},
			api.JobUpdateStatusRollForwardPaused,
		},

		// ROLLING_BACKWARD
		{
			"rolling backward to rolling back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
			nil,
			api.JobUpdateStatusRollingBack,
		},

		// ROLLED_BACK
		{
			"rolled back to rolled back",
			stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK,
			nil,
			api.JobUpdateStatusRolledBack,
		},

		// SUCCEEDED
		{
			"succeeded to rolled back",
			stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
			[]opaquedata.UpdateAction{opaquedata.Rollback},
			api.JobUpdateStatusRolledBack,
		}, {
			"succeeded to rolled forward",
			stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
			nil,
			api.JobUpdateStatusRolledForward,
		},

		// ABORTED
		{
			"aborted to aborted",
			stateless.WorkflowState_WORKFLOW_STATE_ABORTED,
			nil,
			api.JobUpdateStatusAborted,
		},

		// FAILED
		{
			"failed to failed",
			stateless.WorkflowState_WORKFLOW_STATE_FAILED,
			nil,
			api.JobUpdateStatusFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &opaquedata.Data{UpdateActions: tc.actions}
			s, err := NewJobUpdateStatus(tc.state, d)
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, s, "want %s, got %s", tc.wantStatus, s)
		})
	}
}
