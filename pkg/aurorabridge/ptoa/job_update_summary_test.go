package ptoa

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
)

func TestNewJobUpdateSummary_Timestamps(t *testing.T) {
	testCases := []struct {
		name                        string
		events                      []*stateless.WorkflowEvent
		wantCreatedTimestampMs      int64
		wantLastModifiedTimestampMs int64
	}{
		{
			"no events", nil, 0, 0,
		}, {
			"one event",
			[]*stateless.WorkflowEvent{
				{Timestamp: "2019-02-04T16:43:20Z"},
			},
			1549298600000,
			1549298600000,
		}, {
			"two events",
			[]*stateless.WorkflowEvent{
				{Timestamp: "2019-02-04T18:25:10Z"},
				{Timestamp: "2019-02-04T16:43:20Z"},
			},
			1549298600000,
			1549304710000,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			k := fixture.AuroraJobKey()
			w := &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				Events: tc.events,
			}
			s, err := NewJobUpdateSummary(k, w)
			require.NoError(t, err)
			require.Equal(t,
				tc.wantCreatedTimestampMs,
				s.GetState().GetCreatedTimestampMs())
			require.Equal(t,
				tc.wantLastModifiedTimestampMs,
				s.GetState().GetLastModifiedTimestampMs())
		})
	}
}
