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
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	jobmgr_task "github.com/uber/peloton/pkg/jobmgr/task"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// JobCreateTasks creates/recovers all tasks in the job
func JobCreateTasks(ctx context.Context, entity goalstate.Entity) error {
	var err error
	var jobConfig *job.JobConfig
	var taskInfos map[uint32]*task.TaskInfo

	startAddTaskTime := time.Now()
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver

	jobConfig, configAddOn, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID.GetValue())
	if err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job config while creating tasks")
		return err
	}

	instances := jobConfig.InstanceCount

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		return yarpcerrors.AbortedErrorf("failed to get job from cache")
	}

	// First create task configs
	if err = cachedJob.CreateTaskConfigs(ctx, jobID, jobConfig, configAddOn); err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to create task configs")
		return err
	}

	// Get task runtimes.
	taskInfos, err = goalStateDriver.taskStore.GetTasksForJob(ctx, jobID)
	if err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get tasks for job")
		return err
	}

	if len(taskInfos) == 0 {
		// New job being created
		err = createAndEnqueueTasks(ctx, jobID, jobConfig, goalStateDriver)
	} else {
		// Recover error in previous creation of job
		err = recoverTasks(ctx, jobID, jobConfig, taskInfos, goalStateDriver)
	}

	if err != nil {
		// Have this check so ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST
		// would not cause alert
		// TODO: remove this check once
		// ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST is handled correctly
		if !strings.Contains(err.Error(),
			resmgrsvc.EnqueueGangsFailure_ErrorCode_name[int32(resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST)]) {
			goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		}
		return err
	}

	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: &job.RuntimeInfo{State: job.JobState_PENDING},
	}, configAddOn,
		cached.UpdateCacheAndDB)
	if err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update job runtime")
		return err
	}

	goalStateDriver.mtx.jobMetrics.JobCreate.Inc(1)
	log.WithField("job_id", id).
		WithField("instance_count", instances).
		WithField("time_spent", time.Since(startAddTaskTime)).
		Info("all tasks created for job")

	return nil
}

// sendTasksToResMgr is a utility function to enqueue tasks in
// a single batch to resource manager.
func sendTasksToResMgr(
	ctx context.Context,
	jobID *peloton.JobID,
	tasks []*task.TaskInfo,
	jobConfig *job.JobConfig,
	goalStateDriver *driver) error {

	if len(tasks) == 0 {
		return nil
	}

	// Send tasks to resource manager
	err := jobmgr_task.EnqueueGangs(
		ctx,
		tasks,
		jobConfig,
		goalStateDriver.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to enqueue tasks to rm")
		return err
	}

	// Move all task states to pending
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	for _, tt := range tasks {
		instID := tt.GetInstanceId()
		runtimeDiff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.StateField:   task.TaskState_PENDING,
			jobmgrcommon.MessageField: "Task sent for placement",
		}
		runtimeDiffs[instID] = runtimeDiff
	}

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		// job has been untracked.
		return nil
	}

	err = cachedJob.PatchTasks(ctx, runtimeDiffs)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to update task runtime to pending")
		return err
	}
	return nil
}

// recoverTasks recovers partially created jobs.
func recoverTasks(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *job.JobConfig,
	taskInfos map[uint32]*task.TaskInfo,
	goalStateDriver *driver) error {
	var tasks []*task.TaskInfo

	cachedJob := goalStateDriver.jobFactory.AddJob(jobID)
	maxRunningInstances := jobConfig.GetSLA().GetMaximumRunningInstances()
	taskRuntimeInfoMap := make(map[uint32]*task.RuntimeInfo)
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		if _, ok := taskInfos[i]; ok {
			taskInfo := &task.TaskInfo{
				JobId:      jobID,
				InstanceId: i,
				Runtime:    taskInfos[i].GetRuntime(),
				Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
			}

			if taskInfos[i].GetRuntime().GetState() == task.TaskState_INITIALIZED {
				// Task exists, just send to resource manager
				if maxRunningInstances > 0 && taskInfos[i].GetRuntime().GetState() == task.TaskState_INITIALIZED {
					// add task to cache if not already present
					if cachedJob.GetTask(i) == nil {
						cachedJob.ReplaceTasks(
							map[uint32]*task.TaskInfo{i: taskInfo},
							false,
						)
					}
					// run the runtime updater to start instances
					EnqueueJobWithDefaultDelay(
						jobID, goalStateDriver, cachedJob)
				} else {
					tasks = append(tasks, taskInfo)
					// add task to cache if not already present
					if cachedJob.GetTask(i) == nil {
						replaceTaskInfo := make(map[uint32]*task.TaskInfo)
						replaceTaskInfo[i] = taskInfo
						cachedJob.ReplaceTasks(taskInfos, false)
					}
				}
			}
			continue
		}

		// Task does not exist in taskStore, create runtime and then send to resource manager
		log.WithField("job_id", jobID.GetValue()).
			WithField("task_instance", i).
			Info("Creating missing task")

		runtime := jobmgr_task.CreateInitializingTask(jobID, i, jobConfig)
		taskRuntimeInfoMap[i] = runtime

		if maxRunningInstances == 0 {
			taskInfo := &task.TaskInfo{
				JobId:      jobID,
				InstanceId: i,
				Runtime:    runtime,
				Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
			}
			tasks = append(tasks, taskInfo)
		}
	}

	if err := cachedJob.CreateTaskRuntimes(ctx, taskRuntimeInfoMap, jobConfig.OwningTeam); err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to create runtime for tasks")
		return err
	}

	if maxRunningInstances > 0 {
		// run the runtime updater to start instances
		EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
	}

	return sendTasksToResMgr(ctx, jobID, tasks, jobConfig, goalStateDriver)
}

// createAndEnqueueTasks creates all tasks in the job and enqueues them to resource manager.
func createAndEnqueueTasks(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *job.JobConfig,
	goalStateDriver *driver) error {
	instances := jobConfig.InstanceCount

	cachedJob := goalStateDriver.jobFactory.AddJob(jobID)

	// Create task runtimes
	tasks := make([]*task.TaskInfo, instances)
	runtimes := make(map[uint32]*task.RuntimeInfo)
	for i := uint32(0); i < instances; i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, jobConfig)
		runtimes[i] = runtime
		tasks[i] = &task.TaskInfo{
			JobId:      jobID,
			InstanceId: i,
			Runtime:    runtime,
			Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
		}
	}

	err := cachedJob.CreateTaskRuntimes(ctx, runtimes, jobConfig.OwningTeam)
	nTasks := int64(len(tasks))
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			WithField("number_of_tasks", nTasks).
			Error("Failed to create tasks for job")
		goalStateDriver.mtx.taskMetrics.TaskCreateFail.Inc(nTasks)
		return err
	}
	goalStateDriver.mtx.taskMetrics.TaskCreate.Inc(nTasks)

	maxRunningInstances := jobConfig.GetSLA().GetMaximumRunningInstances()

	if maxRunningInstances > 0 {
		var uTasks []*task.TaskInfo
		for i := uint32(0); i < maxRunningInstances; i++ {
			// Only send maxRunningInstances number of tasks to resource manager
			uTasks = append(uTasks, tasks[i])
		}
		return sendTasksToResMgr(ctx, jobID, uTasks, jobConfig, goalStateDriver)
	}
	return sendTasksToResMgr(ctx, jobID, tasks, jobConfig, goalStateDriver)
}
