package task

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/jobmgr/task/config"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"github.com/pborman/uuid"
)

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) (*task.TaskInfo, error) {
	// Get task-specific config.
	taskConfig, err := config.GetTaskConfig(jobID, jobConfig, instanceID)
	if err != nil {
		return nil, err
	}

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.Value, instanceID, uuid.NewUUID().String())

	runtime := &task.RuntimeInfo{
		State: task.TaskState_INITIALIZED,
		MesosTaskId: &mesos_v1.TaskID{
			Value: &mesosTaskID,
		},
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

	return &task.TaskInfo{
		Runtime:    runtime,
		Config:     taskConfig,
		InstanceId: instanceID,
		JobId:      jobID,
	}, nil
}
