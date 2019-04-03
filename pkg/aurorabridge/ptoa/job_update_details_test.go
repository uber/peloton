package ptoa

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"

	"github.com/stretchr/testify/require"
	"go.uber.org/thriftrw/ptr"
)

func TestJoinRollbackJobUpdateDetails(t *testing.T) {
	// Original update.
	d1 := &api.JobUpdateDetails{
		Update: &api.JobUpdate{
			Summary: &api.JobUpdateSummary{
				State: &api.JobUpdateState{
					Status:                  api.JobUpdateStatusAborted.Ptr(),
					CreatedTimestampMs:      ptr.Int64(1000),
					LastModifiedTimestampMs: ptr.Int64(1800),
				},
			},
		},
		UpdateEvents: []*api.JobUpdateEvent{
			{Status: api.JobUpdateStatusRollingForward.Ptr()},
			{Status: api.JobUpdateStatusAborted.Ptr()},
		},
		InstanceEvents: []*api.JobInstanceUpdateEvent{
			{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceUpdating.Ptr()},
		},
	}

	// Rollback update.
	d2 := &api.JobUpdateDetails{
		Update: &api.JobUpdate{
			Summary: &api.JobUpdateSummary{
				State: &api.JobUpdateState{
					Status:                  api.JobUpdateStatusRolledBack.Ptr(),
					CreatedTimestampMs:      ptr.Int64(2000),
					LastModifiedTimestampMs: ptr.Int64(3000),
				},
			},
		},
		UpdateEvents: []*api.JobUpdateEvent{
			{Status: api.JobUpdateStatusRollingBack.Ptr()},
			{Status: api.JobUpdateStatusRolledBack.Ptr()},
		},
		InstanceEvents: []*api.JobInstanceUpdateEvent{
			{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceRollingBack.Ptr()},
			{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceRolledBack.Ptr()},
		},
	}

	// Either ordering of d1 or d2 should yield same results.
	testCases := []struct {
		name   string
		result *api.JobUpdateDetails
	}{
		{"in order", JoinRollbackJobUpdateDetails(d1, d2)},
		{"reverse", JoinRollbackJobUpdateDetails(d2, d1)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t,
				[]*api.JobUpdateEvent{
					{Status: api.JobUpdateStatusRollingForward.Ptr()},
					{Status: api.JobUpdateStatusRollingBack.Ptr()},
					{Status: api.JobUpdateStatusRolledBack.Ptr()},
				},
				tc.result.GetUpdateEvents())

			require.Equal(t,
				[]*api.JobInstanceUpdateEvent{
					{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceUpdating.Ptr()},
					{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceRollingBack.Ptr()},
					{InstanceId: ptr.Int32(0), Action: api.JobUpdateActionInstanceRolledBack.Ptr()},
				},
				tc.result.GetInstanceEvents())

			require.Equal(t,
				api.JobUpdateStatusRolledBack,
				tc.result.GetUpdate().GetSummary().GetState().GetStatus())

			require.Equal(t,
				int64(1000),
				tc.result.GetUpdate().GetSummary().GetState().GetCreatedTimestampMs())

			require.Equal(t,
				int64(3000),
				tc.result.GetUpdate().GetSummary().GetState().GetLastModifiedTimestampMs())
		})
	}
}

func TestNewJobUpdateDetailsInstanceEvents(t *testing.T) {
	k := fixture.AuroraJobKey()
	w := fixture.PelotonWorkflowInfo("")
	w.InstanceEvents = []*stateless.WorkflowInfoInstanceWorkflowEvents{
		{
			InstanceId: 0,
			Events: []*stateless.WorkflowEvent{
				{
					Timestamp: "2019-03-08T00:11:00Z",
					State:     stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
				},
				{
					Timestamp: "2019-03-08T00:10:00Z",
					State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
			},
		}, {
			InstanceId: 1,
			Events: []*stateless.WorkflowEvent{
				{
					Timestamp: "2019-03-08T00:13:00Z",
					State:     stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
				},
				{
					Timestamp: "2019-03-08T00:12:00Z",
					State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
			},
		},
	}
	d, err := NewJobUpdateDetails(k, nil, w)
	require.NoError(t, err)
	require.Equal(t, []*api.JobInstanceUpdateEvent{
		{
			InstanceId:  ptr.Int32(0),
			TimestampMs: ptr.Int64(1552003800000),
			Action:      api.JobUpdateActionInstanceUpdating.Ptr(),
		},
		{
			InstanceId:  ptr.Int32(0),
			TimestampMs: ptr.Int64(1552003860000),
			Action:      api.JobUpdateActionInstanceUpdated.Ptr(),
		},
		{
			InstanceId:  ptr.Int32(1),
			TimestampMs: ptr.Int64(1552003920000),
			Action:      api.JobUpdateActionInstanceUpdating.Ptr(),
		},
		{
			InstanceId:  ptr.Int32(1),
			TimestampMs: ptr.Int64(1552003980000),
			Action:      api.JobUpdateActionInstanceUpdated.Ptr(),
		},
	}, d.GetInstanceEvents())
}

func TestNewJobUpdateDetailsUpdateEvents(t *testing.T) {
	k := fixture.AuroraJobKey()
	w := fixture.PelotonWorkflowInfo("")

	op, err := opaquedata.Deserialize(w.OpaqueData)
	require.NoError(t, err)
	op.StartJobUpdateMessage = "job-update-message"
	w.OpaqueData, err = op.Serialize()
	require.NoError(t, err)

	w.Events = []*stateless.WorkflowEvent{
		{
			Timestamp: "2019-03-08T00:11:00Z",
			State:     stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
		},
		{
			Timestamp: "2019-03-08T00:10:00Z",
			State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		},
		{
			Timestamp: "2019-03-08T00:09:00Z",
			State:     stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		},
	}

	d, err := NewJobUpdateDetails(k, nil, w)
	require.NoError(t, err)
	require.Equal(t, []*api.JobUpdateEvent{
		{
			Status:      api.JobUpdateStatusRollingForward.Ptr(),
			TimestampMs: ptr.Int64(1552003740000),
			Message:     ptr.String("job-update-message"),
		},
		{
			Status:      api.JobUpdateStatusRollingForward.Ptr(),
			TimestampMs: ptr.Int64(1552003800000),
			Message:     ptr.String("job-update-message"),
		},
		{
			Status:      api.JobUpdateStatusRolledForward.Ptr(),
			TimestampMs: ptr.Int64(1552003860000),
		},
	}, d.GetUpdateEvents())
}
