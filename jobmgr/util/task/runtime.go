package task

import (
	"fmt"
	"strconv"
	"strings"

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
	// TODO: deprecate the check once mesos task id migration is complete from
	// uuid-int-uuid -> uuid(job ID)-int(instance ID)-int(monotonically incremental)
	// If uuid has all digits from uuid-int-uuid then it will increment from
	// that value and not default to 1.
	var runID = 0
	splitPrevMesosTaskID := strings.Split(prevMesosTaskID.GetValue(), "-")
	if len(splitPrevMesosTaskID) > 0 {
		var err error
		if runID, err = strconv.Atoi(
			splitPrevMesosTaskID[len(splitPrevMesosTaskID)-1]); err != nil {
			runID = 0
		}
	}
	mesosTaskID := fmt.Sprintf(
		"%s-%d-%d",
		jobID.GetValue(),
		instanceID,
		runID+1)

	return map[string]interface{}{
		cached.PrevMesosTaskIDField: prevMesosTaskID,
		cached.StateField:           task.TaskState_INITIALIZED,
		cached.MesosTaskIDField: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
}
