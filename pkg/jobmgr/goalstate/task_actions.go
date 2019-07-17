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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/goalstate"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// TaskReloadRuntime reloads task runtime into cache.
func TaskReloadRuntime(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)

	// task already deleted, no action needed
	if yarpcerrors.IsNotFound(err) {
		cachedJob.RemoveTask(taskEnt.instanceID)
		return nil
	}

	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskConfigV2Ops.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		runtime.GetConfigVersion(),
	)

	if err != nil {
		return err
	}

	taskInfo := &task.TaskInfo{
		InstanceId: taskEnt.instanceID,
		JobId:      taskEnt.jobID,
		Runtime:    runtime,
		Config:     taskConfig,
	}

	cachedJob.ReplaceTasks(map[uint32]*task.TaskInfo{
		taskEnt.instanceID: taskInfo,
	}, false)

	// This function is called when the runtime in cache is nil.
	// The task needs to re-enqueued into the goal state engine
	// so that it the corresponding action can be executed.
	goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	return nil
}

// TaskStateInvalid dumps a sentry error to indicate that the
// task goal state, state combination is not valid
func TaskStateInvalid(_ context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		return nil
	}
	log.WithFields(log.Fields{
		"current_state": cachedTask.CurrentState().State.String(),
		"goal_state":    cachedTask.GoalState().State.String(),
		"job_id":        taskEnt.jobID.Value,
		"instance_id":   cachedTask.ID(),
	}).Error("unexpected task state")
	goalStateDriver.mtx.taskMetrics.TaskInvalidState.Inc(1)
	return nil
}

// TaskDelete delete the task from cache and removes its runtime from the DB.
// It is used to reduce the instance count of a job.
func TaskDelete(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	cachedJob.RemoveTask(taskEnt.instanceID)
	err := goalStateDriver.taskStore.DeleteTaskRuntime(
		ctx, taskEnt.jobID, taskEnt.instanceID)

	// enqueue the job in case the delete is due to an update,
	// this is done even if db op has error, because the entry may
	// actually be removed
	EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	return err
}
