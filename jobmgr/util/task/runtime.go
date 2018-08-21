package task

import (
	"fmt"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/util"
)

// RegenerateMesosTaskIDDiff returns a diff for patch with the previous mesos
// task id set to the current mesos task id, a regenerated mesos task id
// and state set to INITIALIZED.
func RegenerateMesosTaskIDDiff(
	jobID *peloton.JobID,
	instanceID uint32,
	taskRuntime *task.RuntimeInfo) map[string]interface{} {
	mesosTaskID := getMesosTaskID(jobID, instanceID, taskRuntime)

	return map[string]interface{}{
		cached.PrevMesosTaskIDField:    taskRuntime.GetMesosTaskId(),
		cached.StateField:              task.TaskState_INITIALIZED,
		cached.MesosTaskIDField:        mesosTaskID,
		cached.DesiredMesosTaskIDField: mesosTaskID,
	}
}

func getMesosTaskID(
	jobID *peloton.JobID,
	instanceID uint32,
	taskRuntime *task.RuntimeInfo) *mesos.TaskID {
	// desired mesos task id is not equal to current mesos task id,
	// update current mesos task id to desired mesos task id.
	// This is used for task restart in which case desired mesos task id
	// is changed.
	if taskRuntime.GetMesosTaskId().GetValue() !=
		taskRuntime.GetDesiredMesosTaskId().GetValue() {
		return taskRuntime.GetDesiredMesosTaskId()
	}

	// desired mesos task id is equal to current mesos task id,
	// increment the runID part of mesos task id.
	// This is used for task restart such as failure retry,
	// in which case expected runID is not changed
	// TODO: deprecate the check once mesos task id migration is complete
	// and every task has runID populated
	prevRunID, err := util.ParseRunID(taskRuntime.GetMesosTaskId().GetValue())
	if err != nil || prevRunID < 0 {
		prevRunID = 0
	}
	mesosTaskID := fmt.Sprintf(
		"%s-%d-%d",
		jobID.GetValue(),
		instanceID,
		prevRunID+1)
	return &mesos.TaskID{
		Value: &mesosTaskID,
	}
}
