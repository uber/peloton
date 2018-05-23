package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
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

	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
		}).Error("task is nil in cache with valid job")
		return nil
	}
	runtime, err := cachedTask.GetRunTime(ctx)
	if err != nil {
		return err
	}

	// TODO get job type from cache instead of DB
	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, taskEnt.jobID)
	if err != nil {
		return fmt.Errorf("job config not found for %v", taskEnt.jobID)
	}

	updatedRuntime := util.RegenerateMesosTaskID(taskEnt.jobID, taskEnt.instanceID, runtime.GetMesosTaskId())

	// update task runtime
	updatedRuntime.GoalState = jobmgr_task.GetDefaultTaskGoalState(jobConfig.GetType())
	updatedRuntime.StartTime = ""
	updatedRuntime.CompletionTime = ""
	updatedRuntime.Message = "Initialize task"
	updatedRuntime.Reason = ""

	err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: updatedRuntime}, cached.UpdateCacheAndDB)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
