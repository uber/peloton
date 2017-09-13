package task

import (
	"code.uber.internal/infra/peloton/jobmgr/task/config"
	"code.uber.internal/infra/peloton/util"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) (*task.TaskInfo, error) {
	// Get task-specific config.
	taskConfig, err := config.GetTaskConfig(jobID, jobConfig, instanceID)
	if err != nil {
		return nil, err
	}

	runtime := &task.RuntimeInfo{
		ConfigVersion:        jobConfig.GetChangeLog().GetVersion(),
		DesiredConfigVersion: jobConfig.GetChangeLog().GetVersion(),
	}

	// Set type-specific properties.
	switch jobConfig.GetType() {
	case job.JobType_SERVICE:
		runtime.GoalState = task.TaskState_RUNNING

	default:
		runtime.GoalState = task.TaskState_SUCCEEDED
	}

	t := &task.TaskInfo{
		Runtime:    runtime,
		Config:     taskConfig,
		InstanceId: instanceID,
		JobId:      jobID,
	}
	util.RegenerateMesosTaskID(t)
	return t, nil
}
