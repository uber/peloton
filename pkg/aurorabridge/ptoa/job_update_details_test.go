package ptoa

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
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
			{Status: api.JobUpdateStatusAborted.Ptr()},
			{Status: api.JobUpdateStatusRollingForward.Ptr()},
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
			{Status: api.JobUpdateStatusRolledBack.Ptr()},
			{Status: api.JobUpdateStatusRollingBack.Ptr()},
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
					{Status: api.JobUpdateStatusRolledBack.Ptr()},
					{Status: api.JobUpdateStatusRollingBack.Ptr()},
					{Status: api.JobUpdateStatusRollingForward.Ptr()},
				},
				tc.result.GetUpdateEvents())

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
