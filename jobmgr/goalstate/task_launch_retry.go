package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
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
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)

	if time.Now().Sub(cachedTask.GetLastRuntimeUpdateTime()) < goalStateDriver.cfg.LaunchTimeout {
		// launch not times out, just send it to resource manager
		return sendLaunchInfoToResMgr(ctx, taskEnt)
	}

	// launch timed out, re-initialize the task to re-launch.
	log.WithField("job_id", taskEnt.jobID.GetValue()).
		WithField("instance_id", taskEnt.instanceID).
		Info("task launch timed out, reinitializing the task")
	goalStateDriver.mtx.taskMetrics.TaskLaunchTimeout.Inc(1)
	// TODO kill the old task as well instead of waiting for the orphaned task
	return TaskInitialize(ctx, entity)
}
