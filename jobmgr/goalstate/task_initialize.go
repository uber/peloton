package goalstate

import (
	"context"
	"time"

	"github.com/uber/peloton/common/goalstate"
	jobmgrcommon "github.com/uber/peloton/jobmgr/common"
	jobmgr_task "github.com/uber/peloton/jobmgr/task"
	taskutil "github.com/uber/peloton/jobmgr/util/task"

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

	taskConfig, _, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		runtime.GetDesiredConfigVersion())
	if err != nil {
		return err
	}

	healthState := taskutil.GetInitialHealthState(taskConfig)
	runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
		taskEnt.jobID, taskEnt.instanceID, runtime, healthState)

	// update task runtime
	runtimeDiff[jobmgrcommon.GoalStateField] = jobmgr_task.GetDefaultTaskGoalState(cachedConfig.GetType())
	runtimeDiff[jobmgrcommon.StartTimeField] = ""
	runtimeDiff[jobmgrcommon.CompletionTimeField] = ""
	runtimeDiff[jobmgrcommon.MessageField] = "Initialize task"
	runtimeDiff[jobmgrcommon.ReasonField] = ""

	// If the task is being updated, then move the configuration version to
	// the desired configuration version.
	if runtime.GetConfigVersion() != runtime.GetDesiredConfigVersion() {
		// TBD should the failure count be cleaned up as well?
		runtimeDiff[jobmgrcommon.ConfigVersionField] =
			runtime.GetDesiredConfigVersion()
	}

	err = cachedJob.PatchTasks(ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff})
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
