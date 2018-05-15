package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/goalstate"

	log "github.com/sirupsen/logrus"
)

// sendLaunchInfoToResMgr lets resource manager that the task has been launched.
func sendLaunchInfoToResMgr(ctx context.Context, taskEnt *taskEntity) error {
	var launchedTaskList []*peloton.TaskID

	taskID := &peloton.TaskID{
		Value: taskEnt.GetID(),
	}
	goalStateDriver := taskEnt.driver

	launchedTaskList = append(launchedTaskList, taskID)

	req := &resmgrsvc.MarkTasksLaunchedRequest{
		Tasks: launchedTaskList,
	}
	_, err := goalStateDriver.resmgrClient.MarkTasksLaunched(ctx, req)

	goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now().Add(goalStateDriver.cfg.LaunchTimeout))

	return err
}

// TaskLaunchRetry retries the launch after launch timeout as well as
// sends a message to resource manager to let it know that task has been launched.
func TaskLaunchRetry(ctx context.Context, entity goalstate.Entity) error {
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

	switch cachedTask.CurrentState().State {
	case task.TaskState_LAUNCHED:
		if time.Now().Sub(cachedTask.GetLastRuntimeUpdateTime()) < goalStateDriver.cfg.LaunchTimeout {
			// LAUNCHED not times out, just send it to resource manager
			return sendLaunchInfoToResMgr(ctx, taskEnt)
		}
		goalStateDriver.mtx.taskMetrics.TaskLaunchTimeout.Inc(1)
	case task.TaskState_STARTING:
		if time.Now().Sub(cachedTask.GetLastRuntimeUpdateTime()) < goalStateDriver.cfg.StartTimeout {
			// the job is STARTING on mesos, enqueue the task in case the start timeout
			goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now().Add(goalStateDriver.cfg.StartTimeout))
			return nil
		}
		goalStateDriver.mtx.taskMetrics.TaskStartTimeout.Inc(1)
	default:
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
			"state":       cachedTask.CurrentState().State,
		}).Error("unexpected task state, expecting LAUNCHED or STARTING state")
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		return nil
	}

	// start or launch timed out, re-initialize the task to re-launch.
	log.WithFields(log.Fields{
		"job_id":      taskEnt.jobID.GetValue(),
		"instance_id": taskEnt.instanceID,
		"mesos_id":    cachedTask.GetRunTime().GetMesosTaskId().GetValue(),
		"state":       cachedTask.CurrentState().State.String(),
	}).Info("task timed out, reinitializing the task")

	// TODO kill the old task as well instead of waiting for the orphaned task
	return TaskInitialize(ctx, entity)
}
