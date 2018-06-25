package task

import (
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
)

func TestRegenerateMesosTaskIDDiff(t *testing.T) {
	jobID := uuid.New()
	instanceID := uint32(0)
	previousMesosID := fmt.Sprintf(
		"%s-%d-%s",
		jobID,
		instanceID,
		uuid.New())

	diff := RegenerateMesosTaskIDDiff(
		&peloton.JobID{Value: jobID},
		instanceID,
		&mesos.TaskID{Value: &previousMesosID},
	)

	assert.Equal(t, diff[cached.StateField], task.TaskState_INITIALIZED)
	assert.Equal(t, *diff[cached.PrevMesosTaskIDField].(*mesos.TaskID).Value,
		previousMesosID)
	assert.NotEqual(t, *diff[cached.MesosTaskIDField].(*mesos.TaskID).Value,
		previousMesosID)
}
