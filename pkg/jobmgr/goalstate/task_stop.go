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

	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	log "github.com/sirupsen/logrus"
)

// _defaultShutdownExecutorTimeout is the kill message timeout. If a task
// has not been killed till this duration, then a shutdown is sent to mesos.
const (
	_defaultShutdownExecutorTimeout = 180 * time.Minute
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
	runtime, err := cachedTask.GetRuntime(ctx)

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

	runtime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	// If it had changed, update to current and abort.
	if !cached.IsResMgrOwnedState(runtime.GetState()) &&
		runtime.GetState() != task.TaskState_INITIALIZED {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		return nil
	}

	// TODO: this is write after read, should use optimistic concurrency control
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:   task.TaskState_KILLED,
		jobmgrcommon.MessageField: "Non-running task killed",
		jobmgrcommon.ReasonField:  "",
	}
	if runtime.GetConfigVersion() != runtime.GetDesiredConfigVersion() {
		// Kill is due to update, reset failure count
		runtimeDiff[jobmgrcommon.FailureCountField] = uint32(0)
	}

	// we do not need to handle `instancesToBeRetried` here since the task
	// is being requeued to the goalstate. Goalstate will reload the task
	// runtime when the task is evaluated the next time
	_, _, err = cachedJob.PatchTasks(
		ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff},
		false,
	)
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
	err := goalStateDriver.lm.Kill(
		ctx,
		runtime.GetMesosTaskId().GetValue(),
		runtime.GetDesiredHost(),
		goalStateDriver.taskKillRateLimiter,
	)
	if err != nil {
		return err
	}

	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:   task.TaskState_KILLING,
		jobmgrcommon.MessageField: "Killing the task",
		jobmgrcommon.ReasonField:  "",
	}

	// we do not need to handle `instancesToBeRetried` here since the task
	// is being requeued to the goalstate. Goalstate will reload the task
	// runtime when the task is evaluated the next time
	_, _, err = cachedJob.PatchTasks(
		ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff},
		false,
	)

	if err == nil {
		// timeout for task kill
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID,
			time.Now().Add(_defaultShutdownExecutorTimeout))
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
