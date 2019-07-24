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

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/goalstate"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"

	log "github.com/sirupsen/logrus"
)

// sendLaunchInfoToResMgr lets resource manager that the task has been launched.
func sendLaunchInfoToResMgr(
	ctx context.Context,
	taskEnt *taskEntity,
	mesosTaskID *mesosv1.TaskID,
) error {
	// Updating resource manager with state as LAUNCHED for the task
	_, err := taskEnt.driver.resmgrClient.UpdateTasksState(
		ctx,
		&resmgrsvc.UpdateTasksStateRequest{
			TaskStates: []*resmgrsvc.UpdateTasksStateRequest_UpdateTaskStateEntry{
				{
					State:       task.TaskState_LAUNCHED,
					MesosTaskId: mesosTaskID,
					Task:        &peloton.TaskID{Value: taskEnt.GetID()},
				},
			},
		},
	)

	// This would only be channel/network error and nothing else
	// and this case we should return it from here.
	if err != nil {
		return err
	}

	// Starting timeout as we need to track if the task is
	// launched within timeout period
	taskEnt.driver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID,
		time.Now().Add(taskEnt.driver.cfg.LaunchTimeout))

	return nil
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

	cachedRuntime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	switch cachedRuntime.State {
	case task.TaskState_LAUNCHED:
		if time.Now().Sub(
			time.Unix(0, int64(cachedRuntime.GetRevision().GetUpdatedAt())),
		) < goalStateDriver.cfg.LaunchTimeout {
			// LAUNCHED not times out, just send it to resource manager
			return sendLaunchInfoToResMgr(
				ctx,
				taskEnt,
				cachedRuntime.GetMesosTaskId(),
			)
		}
		goalStateDriver.mtx.taskMetrics.TaskLaunchTimeout.Inc(1)
	case task.TaskState_STARTING:
		cachedConfig, err := cachedJob.GetConfig(ctx)
		if err != nil {
			return err
		}

		if cachedConfig.GetType() == job.JobType_SERVICE {
			return nil
		}

		if time.Now().Sub(
			time.Unix(0, int64(cachedRuntime.GetRevision().GetUpdatedAt())),
		) < goalStateDriver.cfg.StartTimeout {
			// the job is STARTING on mesos, enqueue the task in case the start timeout
			goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID,
				time.Now().Add(goalStateDriver.cfg.StartTimeout))
			return nil
		}
		goalStateDriver.mtx.taskMetrics.TaskStartTimeout.Inc(1)
	default:
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
			"state":       cachedRuntime.State,
		}).Error("unexpected task state, expecting LAUNCHED or STARTING state")
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		return nil
	}

	// start or launch timed out, re-initialize the task to re-launch.
	log.WithFields(log.Fields{
		"job_id":      taskEnt.jobID.GetValue(),
		"instance_id": taskEnt.instanceID,
		"mesos_id":    cachedRuntime.GetMesosTaskId().GetValue(),
		"state":       cachedRuntime.State.String(),
	}).Info("task timed out, reinitializing the task")

	// kill the old task before intializing a new one as the best effort
	taskConfig, _, err := goalStateDriver.taskConfigV2Ops.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		cachedRuntime.GetConfigVersion())

	taskInfo := &task.TaskInfo{
		InstanceId: taskEnt.instanceID,
		JobId:      taskEnt.jobID,
		Runtime:    cachedRuntime,
		Config:     taskConfig,
	}

	if err == nil {
		jobmgrtask.KillOrphanTask(ctx, goalStateDriver.lm, taskInfo)
	}

	// going to regenerate the mesos id and enqueue to place it again
	return TaskInitialize(ctx, entity)
}
