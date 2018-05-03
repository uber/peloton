package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/util"
)

// TaskInitialize does the following:
// 1. Sets the current state to TaskState_INITIALIZED
// 2. Sets the goal state depending on the JobType
// 3. Regenerates a new mesos task ID
func TaskInitialize(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	taskID := &peloton.TaskID{
		Value: taskEnt.GetID(),
	}

	taskInfo, err := goalStateDriver.taskStore.GetTaskByID(ctx, taskID.GetValue())
	if err != nil || taskInfo == nil {
		return fmt.Errorf("task info not found for %v", taskID.GetValue())
	}

	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, taskEnt.jobID)
	if err != nil {
		return fmt.Errorf("job config not found for %v", taskEnt.jobID)
	}

	util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)

	// update task runtime
	taskInfo.Runtime.GoalState = jobmgr_task.GetDefaultTaskGoalState(jobConfig.GetType())
	taskInfo.Runtime.StartTime = ""
	taskInfo.Runtime.CompletionTime = ""
	taskInfo.Runtime.Message = "Initialize task"
	taskInfo.Runtime.Reason = ""

	err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: taskInfo.GetRuntime()}, cached.UpdateCacheAndDB)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	}
	return err
}
