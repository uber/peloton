package cached

import (
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/util"
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
	GetRuntimeDiff(taskRuntime *pbtask.RuntimeInfo, jobConfig *pbjob.JobConfig) RuntimeDiff
}

func getWorkflowStrategy(workflowType models.WorkflowType) WorkflowStrategy {
	switch workflowType {
	default:
		return &updateStrategy{}
	}
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

func (s *updateStrategy) GetRuntimeDiff(taskRuntime *pbtask.RuntimeInfo, jobConfig *pbjob.JobConfig) RuntimeDiff {
	return RuntimeDiff{
		DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		MessageField:              "Job configuration updated via API",
	}
}
