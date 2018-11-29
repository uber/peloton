package job

import (
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"

	"github.com/stretchr/testify/assert"
)

func TestConstructSystemLabels(t *testing.T) {
	jobConfig := &pbjob.JobConfig{
		Name:       "test_name",
		OwningTeam: "test_team",
		Type:       pbjob.JobType_BATCH,
	}
	respoolPath := "/test"

	labels := ConstructSystemLabels(jobConfig, respoolPath)

	assert.Len(t, labels, 5)
}
