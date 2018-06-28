package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"

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
	runtime, err := cachedTask.GetRunTime(ctx)

	if err != nil {
		return err
	}

	if cached.IsResMgrOwnedState(runtime.GetState()) || runtime.GetState() == task.TaskState_INITIALIZED {
		// kill in resource manager
		return stopInitializedTask(ctx, taskEnt)
	}

	if runtime.GetMesosTaskId() != nil {
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

	// If it had changed, update to current and abort.
	if !cached.IsResMgrOwnedState(runtime.GetState()) &&
		runtime.GetState() != task.TaskState_INITIALIZED {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		return nil
	}

	runtimeDiff := cached.RuntimeDiff{
		cached.StateField:   task.TaskState_KILLED,
		cached.MessageField: "Non-running task killed",
		cached.ReasonField:  "",
	}

	err = cachedJob.PatchTasks(ctx,
		map[uint32]cached.RuntimeDiff{taskEnt.instanceID: runtimeDiff})
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
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

	// Send kill signal to mesos first time
	err := jobmgrtask.KillTask(ctx, goalStateDriver.hostmgrClient, runtime.GetMesosTaskId())
	if err != nil {
		return err
	}

	runtimeDiff := cached.RuntimeDiff{
		cached.StateField:   task.TaskState_KILLING,
		cached.MessageField: "Killing the task",
		cached.ReasonField:  "",
	}
	err = cachedJob.PatchTasks(ctx,
		map[uint32]cached.RuntimeDiff{taskEnt.instanceID: runtimeDiff})

	if err == nil {
		// timeout for task kill
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID,
			time.Now().Add(_defaultShutdownExecutorTimeout))
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
