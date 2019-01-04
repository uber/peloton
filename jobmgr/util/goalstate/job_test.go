package goalstate

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"

	"github.com/stretchr/testify/assert"
)

func TestGetDefaultJobGoalState(t *testing.T) {
	assert.Equal(t,
		GetDefaultJobGoalState(job.JobType_SERVICE),
		job.JobState_RUNNING)

	assert.Equal(t,
		GetDefaultJobGoalState(job.JobType_BATCH),
		job.JobState_SUCCEEDED)
}
