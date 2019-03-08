// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"strconv"
	"strings"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// _exitStatusPrefix is prefix of Mesos message that contains exit status
	_exitStatusPrefix = "Command exited with status "

	// _termSignalMsg is the signature portion of Mesos message that contains
	// termination signal
	_termSignalMsg = " terminated with signal "
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

	taskRuntime.AgentID = nil
	taskRuntime.StartTime = ""
	taskRuntime.CompletionTime = ""
	taskRuntime.Host = ""
	taskRuntime.Ports = make(map[string]uint32)
	taskRuntime.TerminationStatus = nil
	taskRuntime.Reason = ""
	taskRuntime.Message = ""
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

		jobmgrcommon.AgentIDField:           nil,
		jobmgrcommon.StartTimeField:         "",
		jobmgrcommon.CompletionTimeField:    "",
		jobmgrcommon.HostField:              "",
		jobmgrcommon.PortsField:             make(map[string]uint32),
		jobmgrcommon.TerminationStatusField: nil,
		jobmgrcommon.MessageField:           "",
		jobmgrcommon.ReasonField:            "",
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

	if runtime.GetReason() == mesos.TaskStatus_REASON_INVALID_OFFERS.String() {
		return true
	}

	if runtime.GetReason() == mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String() {
		if strings.Contains(runtime.GetMessage(), "Container terminated with signal Broken pipe") {
			return true
		}
	}
	return false
}

// GetExitStatusFromMessage extracts the container exit code from message.
func GetExitStatusFromMessage(message string) (uint32, error) {
	if strings.HasPrefix(message, _exitStatusPrefix) {
		s := message[len(_exitStatusPrefix):]
		code64, err := strconv.ParseUint(s, 10, 32)
		return uint32(code64), err
	}
	return 0, yarpcerrors.NotFoundErrorf("Exit status not found")
}

// GetSignalFromMessage extracts the container termination signal from message.
func GetSignalFromMessage(message string) (string, error) {
	if idx := strings.Index(message, _termSignalMsg); idx != -1 {
		return message[idx+len(_termSignalMsg):], nil
	}
	return "", yarpcerrors.NotFoundErrorf("Termination signal not found")
}
