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

package cached

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	updateutil "github.com/uber/peloton/pkg/jobmgr/util/update"
)

const (
	_updateTaskMessage   = "Job configuration updated via API"
	_rollbackTaskMessage = "Job configuration updated due to rollback"
	_restartTaskMessage  = "Task restarted via API"
	_startTaskMessage    = "Task started via API"
	_stopTaskMessage     = "Task stopped via API"
)

// WorkflowStrategy is the strategy of driving instances to
// the desired state of the workflow
type WorkflowStrategy interface {
	// IsInstanceComplete returns if an instance has reached the state
	// desired by the workflow
	IsInstanceComplete(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool
	// IsInstanceInProgress returns if an instance in the process of getting
	// to the state desired by the workflow
	IsInstanceInProgress(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool
	// IsInstanceFailed returns if an instance is failed when getting
	// to the state desired by the workflow
	// TODO: now a task can both get true for IsInstanceInProgress and
	// IsInstanceFailed, it should get true for only one of the func.
	// Now the correctness of code is guarded by order of func call.
	IsInstanceFailed(runtime *pbtask.RuntimeInfo, maxAttempts uint32) bool
	// GetRuntimeDiff accepts the current task runtime of an instance and the desired
	// job config, it returns the RuntimeDiff to move the instance to the state desired
	// by the workflow. Return nil if no action is needed.
	GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff
}

func getWorkflowStrategy(
	updateState pbupdate.State,
	workflowType models.WorkflowType) WorkflowStrategy {
	switch workflowType {
	case models.WorkflowType_START:
		return newStartStrategy()
	case models.WorkflowType_STOP:
		return newStopStrategy()
	case models.WorkflowType_RESTART:
		return newRestartStrategy()
	}

	if updateState == pbupdate.State_ROLLING_BACKWARD {
		return newRollbackStrategy()
	}

	return newUpdateStrategy()
}

func newUpdateStrategy() *updateStrategy {
	return &updateStrategy{}
}

type updateStrategy struct{}

func (s *updateStrategy) IsInstanceComplete(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// for a running task, update is completed if:
	// 1. runtime desired configuration is set to desiredConfigVersion
	// 2. runtime configuration is set to desired configuration
	// 3. healthy state is DISABLED or HEALTHY
	if runtime.GetState() == pbtask.TaskState_RUNNING {
		return runtime.GetDesiredConfigVersion() == desiredConfigVersion &&
			runtime.GetConfigVersion() == runtime.GetDesiredConfigVersion() &&
			(runtime.GetHealthy() == pbtask.HealthState_DISABLED ||
				runtime.GetHealthy() == pbtask.HealthState_HEALTHY)
	}

	// for a terminated task, update is completed if:
	// 1. runtime desired configuration is set to desiredConfigVersion
	// runtime configuration does not matter as it will be set to
	// runtime desired configuration  when it starts
	if util.IsPelotonStateTerminal(runtime.GetState()) &&
		util.IsPelotonStateTerminal(runtime.GetGoalState()) {
		return runtime.GetDesiredConfigVersion() == desiredConfigVersion
	}

	return false
}

func (s *updateStrategy) IsInstanceFailed(
	runtime *pbtask.RuntimeInfo,
	maxAttempts uint32) bool {
	return updateutil.HasFailedUpdate(runtime, maxAttempts)
}

func (s *updateStrategy) IsInstanceInProgress(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// runtime desired config version has been set to the desired,
	// but update has not completed
	return runtime.GetDesiredConfigVersion() == desiredConfigVersion &&
		!s.IsInstanceComplete(desiredConfigVersion, runtime)
}

func (s *updateStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff {
	return jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.MessageField:              _updateTaskMessage,
		// when updating a task, failure count due to old version should be reset
		jobmgrcommon.FailureCountField: uint32(0),
		jobmgrcommon.ReasonField:       "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
		},
	}
}

// rollbackStrategy inherits upgradeStrategy
func newRollbackStrategy() *rollbackStrategy {
	return &rollbackStrategy{newUpdateStrategy()}
}

type rollbackStrategy struct {
	WorkflowStrategy
}

func (s *rollbackStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff {
	return jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.MessageField:              _rollbackTaskMessage,
		// when updating a task, failure count due to old version should be reset
		jobmgrcommon.FailureCountField: uint32(0),
		jobmgrcommon.ReasonField:       "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
		},
	}
}

// restartStrategy inherits upgradeStrategy
func newRestartStrategy() *restartStrategy {
	return &restartStrategy{newUpdateStrategy()}
}

type restartStrategy struct {
	WorkflowStrategy
}

func (s *restartStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff {
	return jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField:            getDefaultTaskGoalState(jobConfig.GetType()),
		jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.MessageField:              _restartTaskMessage,
		jobmgrcommon.ReasonField:               "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		},
	}
}

// newStartStrategy inherits upgradeStrategy
func newStartStrategy() *startStrategy {
	return &startStrategy{newUpdateStrategy()}
}

type startStrategy struct {
	WorkflowStrategy
}

func (s *startStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff {
	// always update both config version and desired config version, no matter
	// the current task state. If startStrategy only update desired config
	// version when task is in terminal state, it is possible that the task
	// transits into a non-terminal state in another thread, and the action would
	// become a restart action.
	return jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField:            getDefaultTaskGoalState(jobConfig.GetType()),
		jobmgrcommon.ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.MessageField:              _startTaskMessage,
		jobmgrcommon.ReasonField:               "",
	}
}

func newStopStrategy() *stopStrategy {
	return &stopStrategy{}
}

type stopStrategy struct{}

// stopStrategy.IsInstanceComplete does not reuse
// updateStrategy.IsInstanceComplete, because stopStrategy needs to update
// config version in GetRuntimeDiff, which is different for updateStrategy.
func (s *stopStrategy) IsInstanceComplete(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// stop is completed if:
	// 1. runtime desired configuration is set to desiredConfigVersion
	// runtime configuration does not matter as it will be set to
	// runtime desired configuration  when it starts
	// 2. runtime desired configuration is set to desiredConfigVersion,
	// but goal state is not terminal. It means, user may start the
	// task again via task level API. If this case is not handled,
	// the stop workflow can get stuck.
	if runtime.GetDesiredConfigVersion() == desiredConfigVersion {
		return (util.IsPelotonStateTerminal(runtime.GetState()) &&
			util.IsPelotonStateTerminal(runtime.GetGoalState())) ||
			!util.IsPelotonStateTerminal(runtime.GetGoalState())
	}

	return false
}

func (s *stopStrategy) IsInstanceInProgress(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// runtime desired config version has been set to the desired,
	// but stop has not completed
	return runtime.GetDesiredConfigVersion() == desiredConfigVersion &&
		!s.IsInstanceComplete(desiredConfigVersion, runtime)
}

func (s *stopStrategy) IsInstanceFailed(
	runtime *pbtask.RuntimeInfo,
	maxAttempts uint32) bool {
	return false
}

func (s *stopStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) jobmgrcommon.RuntimeDiff {
	// always set goal state to KILLED to prevent any failure retry
	return jobmgrcommon.RuntimeDiff{
		jobmgrcommon.ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		jobmgrcommon.GoalStateField:            pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:              _stopTaskMessage,
		jobmgrcommon.ReasonField:               "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		},
	}
}

// TODO: reuse the function in jobmgr/util, now it would create import cycle.
func getDefaultTaskGoalState(jobType pbjob.JobType) pbtask.TaskState {
	switch jobType {
	case pbjob.JobType_SERVICE:
		return pbtask.TaskState_RUNNING

	default:
		return pbtask.TaskState_SUCCEEDED
	}
}
