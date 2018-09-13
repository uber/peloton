package cached

import (
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/util"
)

const (
	_updateTaskMessage  = "Job configuration updated via API"
	_restartTaskMessage = "Task restarted via API"
	_startTaskMessage   = "Task started via API"
	_stopTaskMessage    = "Task stopped via API"
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
	// GetRuntimeDiff accepts the current task runtime of an instance and the desired
	// job config, it returns the RuntimeDiff to move the instance to the state desired
	// by the workflow. Return nil if no action is needed.
	GetRuntimeDiff(jobConfig *pbjob.JobConfig) RuntimeDiff
}

func getWorkflowStrategy(workflowType models.WorkflowType) WorkflowStrategy {
	switch workflowType {
	case models.WorkflowType_START:
		return newStartStrategy()
	case models.WorkflowType_STOP:
		return newStopStrategy()
	case models.WorkflowType_RESTART:
		return newRestartStrategy()
	default:
		return newUpdateStrategy()
	}
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

func (s *updateStrategy) IsInstanceInProgress(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// runtime desired config version has been set to the desired,
	// but update has not completed
	return runtime.GetDesiredConfigVersion() == desiredConfigVersion &&
		!s.IsInstanceComplete(desiredConfigVersion, runtime)
}

func (s *updateStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) RuntimeDiff {
	return RuntimeDiff{
		DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		MessageField:              _updateTaskMessage,
	}
}

// restartStrategy inherits upgradeStrategy
func newRestartStrategy() *restartStrategy {
	return &restartStrategy{newUpdateStrategy()}
}

type restartStrategy struct {
	WorkflowStrategy
}

func (s *restartStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) RuntimeDiff {
	return RuntimeDiff{
		GoalStateField:            getDefaultTaskGoalState(jobConfig.GetType()),
		DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		MessageField:              _restartTaskMessage,
	}
}

// newStartStrategy inherits upgradeStrategy
func newStartStrategy() *startStrategy {
	return &startStrategy{newUpdateStrategy()}
}

type startStrategy struct {
	WorkflowStrategy
}

func (s *startStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) RuntimeDiff {
	// always update both config version and desired config version, no matter
	// the current task state. If startStrategy only update desired config
	// version when task is in terminal state, it is possible that the task
	// transits into a non-terminal state in another thread, and the action would
	// become a restart action.
	return RuntimeDiff{
		GoalStateField:            getDefaultTaskGoalState(jobConfig.GetType()),
		ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
		DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		MessageField:              _startTaskMessage,
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
	if util.IsPelotonStateTerminal(runtime.GetState()) &&
		util.IsPelotonStateTerminal(runtime.GetGoalState()) {
		return runtime.GetDesiredConfigVersion() == desiredConfigVersion
	}

	return false
}

func (s *stopStrategy) IsInstanceInProgress(desiredConfigVersion uint64, runtime *pbtask.RuntimeInfo) bool {
	// runtime desired config version has been set to the desired,
	// but stop has not completed
	return runtime.GetDesiredConfigVersion() == desiredConfigVersion &&
		!s.IsInstanceComplete(desiredConfigVersion, runtime)
}

func (s *stopStrategy) GetRuntimeDiff(jobConfig *pbjob.JobConfig) RuntimeDiff {
	// always set goal state to KILLED to prevent any failure retry
	return RuntimeDiff{
		ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
		DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		GoalStateField:            pbtask.TaskState_KILLED,
		MessageField:              _stopTaskMessage,
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
