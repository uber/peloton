package task

import (
	"code.uber.internal/infra/peloton/util"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) *task.RuntimeInfo {
	runtime := &task.RuntimeInfo{
		ConfigVersion:        jobConfig.GetRevision().GetVersion(),
		DesiredConfigVersion: jobConfig.GetRevision().GetVersion(),
		State:                task.TaskState_INITIALIZED,
		GoalState:            GetDefaultGoalState(jobConfig.GetType()),
	}

	util.RegenerateMesosTaskID(jobID, instanceID, runtime)
	return runtime
}

// GetDefaultGoalState from the job type.
func GetDefaultGoalState(jobType job.JobType) task.TaskGoalState {
	switch jobType {
	case job.JobType_SERVICE:
		return task.TaskGoalState_RUN

	default:
		return task.TaskGoalState_SUCCEED
	}
}
