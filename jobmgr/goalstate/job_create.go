package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"

	log "github.com/sirupsen/logrus"
)

// JobCreateTasks creates/recovers all tasks in the job
func JobCreateTasks(ctx context.Context, entity goalstate.Entity) error {
	var err error
	var jobConfig *job.JobConfig
	var taskInfos map[uint32]*task.TaskInfo
	var jobRuntime *job.RuntimeInfo

	startAddTaskTime := time.Now()
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver

	jobConfig, err = goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job config while creating tasks")
		return err
	}

	instances := jobConfig.InstanceCount

	// First create task configs
	if err = goalStateDriver.taskStore.CreateTaskConfigs(ctx, jobID, jobConfig); err != nil {
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
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		return err
	}

	// Get job runtime and update job state to pending
	jobRuntime, err = goalStateDriver.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		goalStateDriver.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime")
		return err
	}

	jobRuntime.State = job.JobState_PENDING
	err = goalStateDriver.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
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
	err := jobmgr_task.EnqueueGangs(ctx, tasks, jobConfig, goalStateDriver.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to enqueue tasks to rm")
		return err
	}

	// Move all task states to pending
	runtimes := make(map[uint32]*task.RuntimeInfo)
	for _, tt := range tasks {
		instID := tt.GetInstanceId()
		runtime := &task.RuntimeInfo{
			State:   task.TaskState_PENDING,
			Message: "Task sent for placement",
		}
		runtimes[instID] = runtime
	}

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		// job has been untracked.
		return nil
	}

	err = cachedJob.UpdateTasks(ctx, runtimes, cached.UpdateCacheAndDB)
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
	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		if _, ok := taskInfos[i]; ok {
			if taskInfos[i].GetRuntime().GetState() == task.TaskState_INITIALIZED {
				// Task exists, just send to resource manager
				if maxRunningInstances > 0 && taskInfos[i].GetRuntime().GetState() == task.TaskState_INITIALIZED {
					// add task to cache if not already present
					if cachedJob.GetTask(i) == nil {
						cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{i: taskInfos[i].GetRuntime()}, cached.UpdateCacheOnly)
					}
					// run the runtime updater to start instances
					EnqueueJobWithDefaultDelay(
						jobID, goalStateDriver, cachedJob)
				} else {
					runtimes := make(map[uint32]*task.RuntimeInfo)
					runtimes[i] = taskInfos[i].GetRuntime()
					taskInfo := &task.TaskInfo{
						JobId:      jobID,
						InstanceId: i,
						Runtime:    taskInfos[i].GetRuntime(),
						Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
					}
					tasks = append(tasks, taskInfo)
					// add task to cache if not already present
					if cachedJob.GetTask(i) == nil {
						cachedJob.UpdateTasks(ctx, runtimes, cached.UpdateCacheOnly)
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
		if err := cachedJob.CreateTasks(ctx, map[uint32]*task.RuntimeInfo{i: runtime}, jobConfig.OwningTeam); err != nil {
			goalStateDriver.mtx.taskMetrics.TaskCreateFail.Inc(1)
			log.WithError(err).
				WithField("job_id", jobID.GetValue()).
				WithField("id", i).
				Error("failed to create task")
			return err
		}
		goalStateDriver.mtx.taskMetrics.TaskCreate.Inc(1)

		if maxRunningInstances > 0 {
			// run the runtime updater to start instances
			EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
		} else {
			taskInfo := &task.TaskInfo{
				JobId:      jobID,
				InstanceId: i,
				Runtime:    runtime,
				Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
			}
			tasks = append(tasks, taskInfo)
		}
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

	err := cachedJob.CreateTasks(ctx, runtimes, jobConfig.OwningTeam)
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

	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()

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
