// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"

	"github.com/uber/peloton/pkg/common/goalstate"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	jobmgr_task "github.com/uber/peloton/pkg/jobmgr/task"
	"github.com/uber/peloton/pkg/jobmgr/task/launcher"
	"github.com/uber/peloton/pkg/storage"

	log "github.com/sirupsen/logrus"
)

const (
	// Job Enqueue should have already happened, hence we should have a
	// large multiplier for job enqueue in task start to prevent same
	// job from being enqueued too many times into the goal state engine.
	// Currently, setting the value to 6, but it requires some investigation
	// in production if we can set an even larger value.
	_jobEnqueueMultiplierOnTaskStart = 6
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

	launchableTasks, _, err := goalStateDriver.taskLauncher.GetLaunchableTasks(
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

	// update the runtime in cache and DB with runtimeDiff returned by
	// GetLaunchableTasks
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

	launchableTask := launchableTasks[pelotonTaskID.Value]
	// safety check, should not happen
	if launchableTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.Value,
			"instance_id": taskEnt.instanceID,
		}).Error("unexpected nil launchableTask")
		return nil
	}

	err = cachedJob.PatchTasks(ctx, map[uint32]jobmgrcommon.RuntimeDiff{
		taskEnt.instanceID: launchableTask.RuntimeDiff,
	})
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to update task runtime during launch")
		return err
	}

	newRuntime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to fetch task runtime from cache")
		return err
	}

	taskInfos := map[string]*launcher.LaunchableTaskInfo{
		pelotonTaskID.Value: {
			TaskInfo: &task.TaskInfo{
				Runtime:    newRuntime,
				Config:     launchableTask.Config,
				JobId:      taskEnt.jobID,
				InstanceId: taskEnt.instanceID,
			},
			ConfigAddOn: launchableTask.ConfigAddOn,
		},
	}

	// ignoring skippedTaskInfo for now since this is stateful task
	tasksToBeLaunched, _ :=
		goalStateDriver.taskLauncher.CreateLaunchableTasks(ctx, taskInfos)
	var selectedPorts []uint32
	runtimePorts := taskInfo.GetRuntime().GetPorts()
	for _, port := range runtimePorts {
		selectedPorts = append(selectedPorts, port)
	}

	return goalStateDriver.taskLauncher.LaunchStatefulTasks(
		ctx,
		tasksToBeLaunched,
		taskInfo.GetRuntime().GetHost(),
		selectedPorts,
		nil,   // TODO persist host offer id for stateful
		false, /* checkVolume */
	)
}

// TaskStart sends the task to resource manager for placement and changes the state to PENDING.
func TaskStart(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("Failed to get job config for task")
		return err
	}

	if cachedConfig.GetSLA().GetMaximumRunningInstances() > 0 {
		// Tasks are enqueued into goal state in INITIALiZED state either
		// during recovery or due to task restart due to failure/task lost
		// or due to launch/starting state timeouts. In all these cases,
		// job is enqueued into goal state as well. So, this is merely a safety
		// check, hence enqueue with a large delay to prevent too many
		// enqueues of the same job during recovery.
		goalStateDriver.EnqueueJob(taskEnt.jobID, time.Now().Add(
			_jobEnqueueMultiplierOnTaskStart*
				goalStateDriver.JobRuntimeDuration(cachedConfig.GetType())))
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
	err = jobmgr_task.EnqueueGangs(
		ctx,
		[]*task.TaskInfo{taskInfo},
		cachedConfig,
		goalStateDriver.resmgrClient)
	if err != nil {
		return err
	}
	// Update task state to PENDING
	runtime := taskInfo.GetRuntime()
	if runtime.GetState() != task.TaskState_PENDING {
		runtimeDiff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.StateField:   task.TaskState_PENDING,
			jobmgrcommon.MessageField: "Task sent for placement",
		}
		err = cachedJob.PatchTasks(ctx,
			map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff})
	}
	return err
}
