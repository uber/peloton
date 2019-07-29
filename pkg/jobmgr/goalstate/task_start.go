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

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	jobmgr_task "github.com/uber/peloton/pkg/jobmgr/task"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// Job Enqueue should have already happened, hence we should have a
	// large multiplier for job enqueue in task start to prevent same
	// job from being enqueued too many times into the goal state engine.
	// Currently, setting the value to 6, but it requires some investigation
	// in production if we can set an even larger value.
	_jobEnqueueMultiplierOnTaskStart = 6
)

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

	// TODO: Investigate how to create proper gangs for scheduling (currently, task are treat independently)
	response, err := jobmgr_task.EnqueueGangs(
		ctx,
		[]*task.TaskInfo{taskInfo},
		cachedConfig,
		goalStateDriver.resmgrClient)

	// Parse the EnqueueGangs response to determine if the task is successfully enqueued
	// or has been previously enqueued, and should transition to PENDING state.
	enqueued := func(res *resmgrsvc.EnqueueGangsResponse, e error) bool {
		if e == nil && res.GetError() == nil {
			return true
		}

		if res.GetError().GetFailure().GetFailed() != nil {
			failed := response.GetError().GetFailure().GetFailed()
			if len(failed) == 1 && failed[0].Errorcode ==
				resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST {
				jid, instID, err := util.ParseTaskID(failed[0].Task.GetId().GetValue())
				if err == nil || jid == taskEnt.jobID.GetValue() || instID == taskEnt.instanceID {
					return true
				}
			}
		}
		return false
	}(response, err)

	if !enqueued {
		return yarpcerrors.InternalErrorf("failed to enqueue task into resource manager %v", taskID)
	}

	// Update task state to PENDING
	runtime := taskInfo.GetRuntime()
	if runtime.GetState() != task.TaskState_PENDING {
		var instancesToRetry []uint32
		runtimeDiff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.StateField:   task.TaskState_PENDING,
			jobmgrcommon.MessageField: "Task sent for placement",
		}
		_, instancesToRetry, err = cachedJob.PatchTasks(
			ctx,
			map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff},
			false,
		)

		if err == nil && len(instancesToRetry) != 0 {
			// if the task needs to be reloaded into cache, throw error here
			// so that the action is retried by goalstate engine
			return _errTasksNotInCache
		}
	}
	return err
}
