package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
)

// startStatefulTask starts stateful tasks.
func startStatefulTask(ctx context.Context, taskEnt *taskEntity, taskInfo *task.TaskInfo, goalStateDriver *driver) error {
	// Volume is in CREATED state so launch the task directly to hostmgr.
	if goalStateDriver.taskLauncher == nil {
		return fmt.Errorf("task launcher not available")
	}

	if taskInfo.GetRuntime().GetGoalState() == task.TaskState_KILLED {
		return nil
	}

	pelotonTaskID := &peloton.TaskID{
		Value: taskEnt.GetID(),
	}

	taskInfos, err := goalStateDriver.taskLauncher.GetLaunchableTasks(
		ctx,
		[]*peloton.TaskID{pelotonTaskID},
		taskInfo.GetRuntime().GetHost(),
		taskInfo.GetRuntime().GetAgentID(),
		nil,
	)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to get launchable tasks")
		return err
	}

	// GetLaunchableTasks have updated the task runtime to LAUNCHED state and
	// the placement operation. So update the runtime in cache and DB.
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: taskInfos[pelotonTaskID.Value].GetRuntime()}, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to update task runtime during launch")
		return err
	}

	launchableTasks := launcher.CreateLaunchableTasks(taskInfos)
	var selectedPorts []uint32
	runtimePorts := taskInfo.GetRuntime().GetPorts()
	for _, port := range runtimePorts {
		selectedPorts = append(selectedPorts, port)
	}

	return goalStateDriver.taskLauncher.LaunchStatefulTasks(ctx, launchableTasks, taskInfo.GetRuntime().GetHost(), selectedPorts, false /* checkVolume */)
}

// TaskStart sends the task to resource manager for placement and changes the state to PENDING.
func TaskStart(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)

	// Retrieves job config and task info from data stores.
	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, taskEnt.jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			Error("failed to fetch job config in task start")
		return err
	}

	if jobConfig.GetSla().GetMaximumRunningInstances() > 0 {
		goalStateDriver.EnqueueJob(taskEnt.jobID, time.Now())
		return nil
	}

	taskID := taskEnt.GetID()
	taskInfo, err := goalStateDriver.taskStore.GetTaskByID(ctx, taskID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to fetch task info in task start")
		return err
	}
	if taskInfo == nil {
		return fmt.Errorf("task info not found for %v", taskID)
	}

	stateful := taskInfo.GetConfig().GetVolume() != nil && len(taskInfo.GetRuntime().GetVolumeID().GetValue()) > 0
	if stateful {
		volumeID := &peloton.VolumeID{
			Value: taskInfo.GetRuntime().GetVolumeID().GetValue(),
		}
		pv, err := goalStateDriver.volumeStore.GetPersistentVolume(ctx, volumeID)
		if err != nil {
			_, ok := err.(*storage.VolumeNotFoundError)
			if !ok {
				return fmt.Errorf("failed to read volume %v for task %v", volumeID, taskID)
			}
			// Volume not exist so enqueue as normal task going through placement.
		} else if pv.GetState() == volume.VolumeState_CREATED {
			return startStatefulTask(ctx, taskEnt, taskInfo, goalStateDriver)
		}
	}

	// TODO: Investigate how to create proper gangs for scheduling (currently, task are treat independently)
	err = jobmgr_task.EnqueueGangs(ctx, []*task.TaskInfo{taskInfo}, jobConfig, goalStateDriver.resmgrClient)
	if err != nil {
		return err
	}
	// Update task state to PENDING
	runtime := taskInfo.GetRuntime()
	if runtime.GetState() != task.TaskState_PENDING {
		runtime.State = task.TaskState_PENDING
		runtime.Message = "Task sent for placement"
		err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: runtime}, cached.UpdateCacheAndDB)
	}
	return err
}
