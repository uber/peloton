package tracked

import (
	"context"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/taskconfig"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"

	log "github.com/sirupsen/logrus"
)

func (j *job) createTasks(ctx context.Context) (bool, error) {
	var err error
	var jobConfig *pb_job.JobConfig
	var taskInfos map[uint32]*pb_task.TaskInfo
	var jobRuntime *pb_job.RuntimeInfo

	startAddTaskTime := time.Now()

	jobConfig, err = j.m.jobStore.GetJobConfig(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to get job config")
		return true, err
	}

	instances := jobConfig.InstanceCount

	// First create task configs
	if err = j.m.taskStore.CreateTaskConfigs(ctx, j.id, jobConfig); err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to create task configs")
		return true, err
	}

	// Get task runtimes.
	taskInfos, err = j.m.taskStore.GetTasksForJob(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id).
			Error("failed to get tasks for job")
		return true, err
	}

	if len(taskInfos) == 0 {
		// New job being created
		err = j.createAndEnqueueTasks(ctx, jobConfig)
	} else {
		// Recover error in previous creation of job
		err = j.recoverTasks(ctx, jobConfig, taskInfos)
	}

	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		return true, err
	}

	// Get job runtime and update job state to pending
	jobRuntime, err = j.m.jobStore.GetJobRuntime(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to get job runtime")
		return true, err
	}

	jobRuntime.State = pb_job.JobState_PENDING
	err = j.m.jobStore.UpdateJobRuntime(ctx, j.id, jobRuntime)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to update job runtime")
		return true, err
	}

	// TBD add a timeout per task in case placement is not returned in some time

	j.m.mtx.jobMetrics.JobCreate.Inc(1)
	log.WithField("job_id", j.id.GetValue()).
		WithField("instance_count", instances).
		WithField("time_spent", time.Since(startAddTaskTime)).
		Info("all tasks created for job")

	return false, nil
}

func (j *job) sendTasksToResMgr(ctx context.Context, tasks []*pb_task.TaskInfo, jobConfig *pb_job.JobConfig) error {
	if len(tasks) == 0 {
		return nil
	}

	// Send tasks to resource manager
	err := jobmgr_task.EnqueueGangs(ctx, tasks, jobConfig, j.m.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.ID().GetValue()).
			Error("failed to enqueue tasks to rm")
		return err
	}

	// Move all task states to pending
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	for _, task := range tasks {
		instID := task.GetInstanceId()
		runtime := task.GetRuntime()
		runtime.State = pb_task.TaskState_PENDING
		runtime.Message = "Task sent for placement"
		runtimes[instID] = runtime
	}

	err = j.m.UpdateTaskRuntimes(ctx, j.ID(), runtimes, UpdateOnly)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.ID().GetValue()).
			Error("failed to update task runtime to pending")
		return err
	}
	return nil
}

func (j *job) recoverTasks(ctx context.Context, jobConfig *pb_job.JobConfig, taskInfos map[uint32]*pb_task.TaskInfo) error {
	var tasks []*pb_task.TaskInfo

	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		if _, ok := taskInfos[i]; ok {
			if taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_INITIALIZED {
				// Task exists, just send to resource manager
				if maxRunningInstances > 0 && taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_INITIALIZED {
					// add task to cache
					j.setTask(i, taskInfos[i].GetRuntime())
					// run the runtime updater to start instances
					j.m.ScheduleJob(j, time.Now().Add(j.m.GetJobRuntimeDuration(jobConfig.GetType())))
				} else {
					runtimes := make(map[uint32]*pb_task.RuntimeInfo)
					runtimes[i] = taskInfos[i].GetRuntime()
					taskInfo := &pb_task.TaskInfo{
						JobId:      j.ID(),
						InstanceId: i,
						Runtime:    taskInfos[i].GetRuntime(),
						Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
					}
					tasks = append(tasks, taskInfo)
					j.m.SetTasks(j.id, runtimes, UpdateOnly)
				}

			}
			continue
		}

		// Task does not exist in taskStore, create runtime and then send to resource manager
		log.WithField("job_id", j.id.GetValue()).
			WithField("task_instance", i).
			Info("Creating missing task")

		runtime := jobmgr_task.CreateInitializingTask(j.id, i, jobConfig)
		if err := j.m.taskStore.CreateTaskRuntime(ctx, j.id, i, runtime, jobConfig.OwningTeam); err != nil {
			j.m.mtx.taskMetrics.TaskCreateFail.Inc(1)
			log.WithError(err).
				WithField("job_id", j.id.GetValue()).
				WithField("id", i).
				Error("failed to create task")
			return err
		}
		j.m.mtx.taskMetrics.TaskCreate.Inc(1)

		if maxRunningInstances > 0 {
			// add task to cache
			j.setTask(i, runtime)
			// run the runtime updater to start instances
			j.m.ScheduleJob(j, time.Now().Add(j.m.GetJobRuntimeDuration(jobConfig.GetType())))
		} else {
			runtimes := make(map[uint32]*pb_task.RuntimeInfo)
			runtimes[i] = runtime
			taskInfo := &pb_task.TaskInfo{
				JobId:      j.ID(),
				InstanceId: i,
				Runtime:    runtime,
				Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
			}
			tasks = append(tasks, taskInfo)
			j.m.SetTasks(j.id, runtimes, UpdateOnly)
		}
	}

	return j.sendTasksToResMgr(ctx, tasks, jobConfig)
}

func (j *job) createAndEnqueueTasks(ctx context.Context, jobConfig *pb_job.JobConfig) error {
	instances := jobConfig.InstanceCount

	// Create task runtimes
	tasks := make([]*pb_task.TaskInfo, instances)
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	for i := uint32(0); i < instances; i++ {
		runtime := jobmgr_task.CreateInitializingTask(j.id, i, jobConfig)
		runtimes[i] = runtime
		tasks[i] = &pb_task.TaskInfo{
			JobId:      j.id,
			InstanceId: i,
			Runtime:    runtime,
			Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
		}
	}

	err := j.m.taskStore.CreateTaskRuntimes(ctx, j.id, runtimes, jobConfig.OwningTeam)
	nTasks := int64(len(tasks))
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			WithField("number_of_tasks", nTasks).
			Error("Failed to create tasks for job")
		j.m.mtx.taskMetrics.TaskCreateFail.Inc(nTasks)
		return err
	}
	j.m.mtx.taskMetrics.TaskCreate.Inc(nTasks)

	// Add task to tracked manager in-memory DB
	for i := uint32(0); i < instances; i++ {
		j.setTask(i, runtimes[i])
	}

	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()

	if maxRunningInstances > 0 {
		uRuntimes := make(map[uint32]*pb_task.RuntimeInfo)
		for i := uint32(0); i < maxRunningInstances; i++ {
			uRuntimes[i] = runtimes[i]
		}
		j.m.SetTasks(j.ID(), uRuntimes, UpdateAndSchedule)
	} else {
		// Send tasks to resource manager
		return j.sendTasksToResMgr(ctx, tasks, jobConfig)
	}

	return nil
}
