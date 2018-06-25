package task

import (
	"fmt"

	"github.com/pborman/uuid"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
)

// RegenerateMesosTaskIDDiff returns a diff for patch with the previous mesos
// task id set to the current mesos task id, a regenerated mesos task id
// and state set to INITIALIZED.
func RegenerateMesosTaskIDDiff(
	jobID *peloton.JobID,
	instanceID uint32,
	prevMesosTaskID *mesos.TaskID) map[string]interface{} {
	mesosTaskID := fmt.Sprintf(
		"%s-%d-%s",
		jobID.GetValue(),
		instanceID,
		uuid.New())

	return map[string]interface{}{
		cached.PrevMesosTaskIDField: prevMesosTaskID,
		cached.StateField:           task.TaskState_INITIALIZED,
		cached.MesosTaskIDField: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
}
