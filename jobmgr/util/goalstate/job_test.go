package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"

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
