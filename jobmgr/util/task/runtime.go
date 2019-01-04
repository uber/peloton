package task

import (
	"strings"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	jobmgrcommon "github.com/uber/peloton/jobmgr/common"
	"github.com/uber/peloton/util"
)

// GetInitialHealthState returns the initial health State
// The initial health state is UNKNOWN or DISABLED
// depends on health check is enabled or not
func GetInitialHealthState(taskConfig *task.TaskConfig) task.HealthState {
	if taskConfig.GetHealthCheck().GetEnabled() {
		return task.HealthState_HEALTH_UNKNOWN
	}
	return task.HealthState_DISABLED
}

// RegenerateMesosTaskRuntime changes the runtime to INITIALIZED state
// with correct initial health state and a regenerated mesos task id
// and the previous mesos task id set to the current value.
func RegenerateMesosTaskRuntime(
	jobID *peloton.JobID,
	instanceID uint32,
	taskRuntime *task.RuntimeInfo,
	initHealthyField task.HealthState,
) {
	mesosTaskID := getMesosTaskID(jobID, instanceID, taskRuntime)
	taskRuntime.PrevMesosTaskId = taskRuntime.GetMesosTaskId()
	taskRuntime.State = task.TaskState_INITIALIZED
	taskRuntime.MesosTaskId = mesosTaskID
	taskRuntime.DesiredMesosTaskId = mesosTaskID
	taskRuntime.Healthy = initHealthyField
}

// RegenerateMesosTaskIDDiff returns a diff for patch with the previous mesos
// task id set to the current mesos task id, a regenerated mesos task id, a
// proper initial health state, and task state set to INITIALIZED.
func RegenerateMesosTaskIDDiff(
	jobID *peloton.JobID,
	instanceID uint32,
	taskRuntime *task.RuntimeInfo,
	initHealthyField task.HealthState) map[string]interface{} {
	mesosTaskID := getMesosTaskID(jobID, instanceID, taskRuntime)

	return map[string]interface{}{
		jobmgrcommon.PrevMesosTaskIDField:    taskRuntime.GetMesosTaskId(),
		jobmgrcommon.StateField:              task.TaskState_INITIALIZED,
		jobmgrcommon.MesosTaskIDField:        mesosTaskID,
		jobmgrcommon.DesiredMesosTaskIDField: mesosTaskID,
		jobmgrcommon.HealthyField:            initHealthyField,
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
	if taskRuntime.GetDesiredMesosTaskId() != nil &&
		taskRuntime.GetMesosTaskId().GetValue() !=
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
	if err != nil {
		prevRunID = 0
	}
	return util.CreateMesosTaskID(jobID, instanceID, prevRunID+1)
}

// IsSystemFailure returns true is failure is due to a system failure like
// container launch failure or container terminated with signal broken pipe.
// System failures should be tried MaxSystemFailureAttempts irrespective of
// the maximum retries in the job configuration.
func IsSystemFailure(runtime *task.RuntimeInfo) bool {
	if runtime.GetReason() == mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String() {
		return true
	}

	if runtime.GetReason() == mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String() {
		if strings.Contains(runtime.GetMessage(), "Container terminated with signal Broken pipe") {
			return true
		}
	}
	return false
}
