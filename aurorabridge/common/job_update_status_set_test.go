package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

func TestJobUpdateStatusSetHas(t *testing.T) {
	s := NewJobUpdateStatusSet(
		api.JobUpdateStatusRollingForward,
		api.JobUpdateStatusRollingBack,
	)
	require.True(t, s.Has(api.JobUpdateStatusRollingForward))
	require.False(t, s.Has(api.JobUpdateStatusAborted))
}

func TestJobUpdateStatusSetString(t *testing.T) {
	s := NewJobUpdateStatusSet(
		api.JobUpdateStatusRollingForward,
		api.JobUpdateStatusRollingBack,
		api.JobUpdateStatusFailed,
	)
	require.Equal(t, "{FAILED, ROLLING_BACK, ROLLING_FORWARD}", s.String())
}
