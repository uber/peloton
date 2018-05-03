package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"

	log "github.com/sirupsen/logrus"
)

// _defaultShutdownExecutorTimeout is the kill message timeout. If a task
// has not been killed till this duration, then a shutdown is sent to mesos.
const (
	_defaultShutdownExecutorTimeout = 10 * time.Minute
)

// TaskStop kills the task.
func TaskStop(ctx context.Context, entity goalstate.Entity) error {
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
	runtime := cachedTask.GetRunTime()

	if runtime == nil {
		return fmt.Errorf("task has no runtime info in cache")
	}

	switch {
	case runtime.GetState() == task.TaskState_INITIALIZED,
		runtime.GetState() == task.TaskState_PENDING:
		// kill in resource manager
		return stopInitializedTask(ctx, taskEnt)

	case runtime.GetMesosTaskId() != nil:
		// kill in  mesos
		return stopMesosTask(ctx, taskEnt, runtime)
	}

	return nil
}

func stopInitializedTask(ctx context.Context, taskEnt *taskEntity) error {
	// If initializing, store state as killed and remove from resmgr.
	// TODO: Due to missing atomic updates in DB, there is a race
	// where we accidentially may start off the task, even though we
	// have marked it as KILLED.
	taskID := taskEnt.GetID()
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	req := &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: taskID,
			},
		},
	}
	// Calling resmgr Kill API
	res, err := goalStateDriver.resmgrClient.KillTasks(ctx, req)
	if err != nil {
		return err
	}

	if e := res.GetError(); e != nil {
		// TODO: As of now this function supports one task
		// We need to do it for batch
		if e[0].GetNotFound() != nil {
			log.WithFields(log.Fields{
				"Task":  e[0].GetNotFound().Task.Value,
				"Error": e[0].GetNotFound().Message,
			}).Info("Task not found in resmgr")
		} else {
			return fmt.Errorf("Task %s can not be killed due to %s",
				e[0].GetKillError().Task.Value,
				e[0].GetKillError().Message)
		}
	}

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)
	if err != nil {
		return err
	}

	// If it had changed, update to current and abort.
	if runtime.GetState() != task.TaskState_INITIALIZED &&
		runtime.GetState() != task.TaskState_PENDING {
		cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: runtime}, cached.UpdateCacheOnly)
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		return nil
	}

	runtime.State = task.TaskState_KILLED
	runtime.Message = "Non-running task killed"
	runtime.Reason = ""

	err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: runtime}, cached.UpdateCacheAndDB)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	}
	return err
}

func stopMesosTask(ctx context.Context, taskEnt *taskEntity, runtime *task.RuntimeInfo) error {
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

	// Send kill signal to mesos first time, shutdown the executor if timeout
	if cachedTask.GetKillAttempts() == 0 {
		err := jobmgr_task.KillTask(ctx, goalStateDriver.hostmgrClient, runtime.GetMesosTaskId())
		if err != nil {
			return err
		}
		cachedTask.IncrementKillAttempts()
		// timeout for task kill
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now().Add(_defaultShutdownExecutorTimeout))
		return nil
	}

	goalStateDriver.mtx.taskMetrics.ExecutorShutdown.Inc(1)
	log.WithField("job_id", taskEnt.jobID).
		WithField("instance_id", taskEnt.instanceID).
		Info("task kill timed out, try to shutdown executor")

	return jobmgr_task.ShutdownMesosExecutor(ctx, goalStateDriver.hostmgrClient, runtime.GetMesosTaskId(), runtime.GetAgentID())
}
