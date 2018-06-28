package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"

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

	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithField("job_id", taskEnt.jobID).
			WithError(err).
			Debug("Failed to get job config")
		return err
	}

	runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
		taskEnt.jobID, taskEnt.instanceID, runtime.GetMesosTaskId())

	// update task runtime
	runtimeDiff[cached.GoalStateField] = jobmgr_task.GetDefaultTaskGoalState(cachedConfig.GetType())
	runtimeDiff[cached.StartTimeField] = ""
	runtimeDiff[cached.CompletionTimeField] = ""
	runtimeDiff[cached.MessageField] = "Initialize task"
	runtimeDiff[cached.ReasonField] = ""

	err = cachedJob.PatchTasks(ctx,
		map[uint32]cached.RuntimeDiff{taskEnt.instanceID: runtimeDiff})
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
