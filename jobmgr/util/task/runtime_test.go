package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
)

func TestRegenerateMesosTaskIDDiff(t *testing.T) {
	testTable := []struct {
		jobID              string
		instanceID         uint32
		curMesosTaskID     string
		desiredMesosTaskID string
		newMesosTaskID     string
	}{
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1690f7cf-9691-42ea-8fd3-7e417246b830",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-3",
		},
	}

	for _, tt := range testTable {
		runtime := &task.RuntimeInfo{
			MesosTaskId:        &mesos.TaskID{Value: &tt.curMesosTaskID},
			DesiredMesosTaskId: &mesos.TaskID{Value: &tt.desiredMesosTaskID},
		}
		diff := RegenerateMesosTaskIDDiff(
			&peloton.JobID{Value: tt.jobID},
			tt.instanceID,
			runtime,
		)

		assert.Equal(t, diff[cached.StateField], task.TaskState_INITIALIZED)
		assert.Equal(t, *diff[cached.PrevMesosTaskIDField].(*mesos.TaskID).Value,
			tt.curMesosTaskID)
		assert.Equal(t, *diff[cached.MesosTaskIDField].(*mesos.TaskID).Value,
			tt.newMesosTaskID)
		assert.Equal(t, *diff[cached.DesiredMesosTaskIDField].(*mesos.TaskID).Value,
			tt.newMesosTaskID)
	}
}
