package tracked

import (
	"context"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/common/taskconfig"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

const (
	retryCount    int           = 3
	retryDuration time.Duration = 15 * time.Second
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

// To be used only with MaximumRunningInstances set to non-zero in SLA.
// This function should be called while holding both manager and job locks.
func (j *job) scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances uint32, instanceID uint32, runtime *pb_task.RuntimeInfo) {
	if !j.m.running {
		return
	}

	if runtime.GetState() != pb_task.TaskState_INITIALIZED {
		log.WithField("state", runtime.GetState()).
			WithField("job_id", j.id.GetValue()).
			WithField("instance_id", instanceID).
			Debug("skipping instance")
		return
	}

	if j.currentScheduledTasks >= maxRunningInstances {
		log.WithField("current_scheduled_tasks", j.currentScheduledTasks).
			WithField("job_id", j.id.GetValue()).
			WithField("instance_id", instanceID).
			Debug("skipping instance")
		return
	}

	t, ok := j.tasks[instanceID]
	if !ok {
		t = newTask(j, instanceID)
		j.tasks[instanceID] = t
	}
	t.UpdateRuntime(runtime)

	j.m.taskScheduler.schedule(t, time.Now())
	j.currentScheduledTasks++
	// TODO move this message to debug after MaximumRunningInstances feature is stabilized
	log.WithField("job_id", j.id.GetValue()).
		WithField("current_scheduled_tasks", j.currentScheduledTasks).
		Info("update current scheduled tasks")
}

func (j *job) recoverTasks(ctx context.Context, jobConfig *pb_job.JobConfig, taskInfos map[uint32]*pb_task.TaskInfo) error {
	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		if _, ok := taskInfos[i]; ok {
			if taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_INITIALIZED || taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_PENDING {
				// Task exists, just send to resource manager
				if maxRunningInstances > 0 && taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_INITIALIZED {
					j.m.Lock()
					j.Lock()
					j.scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances, i, taskInfos[i].GetRuntime())
					j.Unlock()
					j.m.Unlock()
				} else {
					j.m.SetTask(j.id, i, taskInfos[i].GetRuntime())
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
			j.m.Lock()
			j.Lock()
			j.scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances, i, runtime)
			j.Unlock()
			j.m.Unlock()
		} else {
			j.m.SetTask(j.id, i, runtime)
		}
	}

	return nil
}

func (j *job) createAndEnqueueTasks(ctx context.Context, jobConfig *pb_job.JobConfig) error {
	instances := jobConfig.InstanceCount

	// Create task runtimes
	tasks := make([]*pb_task.TaskInfo, instances)
	runtimes := make([]*pb_task.RuntimeInfo, instances)
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
		j.m.Lock()
		j.Lock()
		// Only schedule maxRunningInstances number of tasks for placement
		for i := uint32(0); i < maxRunningInstances; i++ {
			j.scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances, i, runtimes[i])
		}
		j.Unlock()
		j.m.Unlock()
	} else {
		// Send tasks to resource manager
		err = jobmgr_task.EnqueueGangs(ctx, tasks, jobConfig, j.m.resmgrClient)
		if err != nil {
			log.WithError(err).
				WithField("job_id", j.id.GetValue()).
				Error("failed to enqueue tasks to RM")
			return err
		}

		// Move all task states to pending
		for i := uint32(0); i < instances; i++ {
			runtimes[i].State = pb_task.TaskState_PENDING
			runtimes[i].Message = "Task sent for placement"
		}
		err = j.m.taskStore.UpdateTaskRuntimes(ctx, j.id, runtimes)
		if err != nil {
			log.WithError(err).
				WithField("job_id", j.id.GetValue()).
				Error("failed to update task runtime to pending")
			return err
		}
	}

	return nil
}

// Thread should be holding both manager and job locks before calling this function.
func (j *job) evaluateJobSLA() error {
	if j.config == nil || j.config.sla == nil {
		return nil
	}

	maxRunningInstances := j.config.sla.GetMaximumRunningInstances()
	scheduledList := make(map[uint32]bool)

	// Get job runtime, and if there is no more initialized tasks remaining, just return.
	jobRuntime, err := j.m.jobStore.GetJobRuntime(context.Background(), j.id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id).
			Error("failed to get job runtime while evaluating job sla")
		return err
	}

	if util.IsPelotoJobStateTerminal(jobRuntime.GetState()) {
		return nil
	}

	taskStats := jobRuntime.GetTaskStats()

	if jobRuntime.GetState() == pb_job.JobState_RUNNING && taskStats[pb_task.TaskState_INITIALIZED.String()] == uint32(0) {
		return nil
	}

	if maxRunningInstances == 0 {
		return nil
	}

	if j.currentScheduledTasks < maxRunningInstances {
		// Try to find a task in the cache which has not run yet,
		// validate its runtime with DB, and then schedule it.
		for _, t := range j.tasks {
			if t.GetRunTime() == nil {
				continue
			}

			if j.currentScheduledTasks >= maxRunningInstances {
				break
			}

			if t.GetRunTime().GetState() == pb_task.TaskState_INITIALIZED {
				runtime, err := j.m.taskStore.GetTaskRuntime(context.Background(), j.id, t.ID())
				if err != nil {
					log.WithError(err).
						WithField("job_id", j.id.GetValue()).
						WithField("instance_id", t.ID()).
						Error("failed to get task runtime while evaluating job sla")
					continue
				}

				t.UpdateRuntime(runtime)
				j.scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances, t.ID(), runtime)
				scheduledList[t.ID()] = true
			}
		}
	}

	if j.currentScheduledTasks >= maxRunningInstances {
		return nil
	}

	// Did not find enough initialized tasks in cache.
	// So, load runtimes from DB and find one now.
	// TODO once task goal state cache is always in sync with DB, this code can be removed.
	tasks, err := j.m.taskStore.GetTasksForJob(context.Background(), j.id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id).
			Error("failed to get task runtimes for job during sla evaluation")
		return err
	}

	for instanceID, task := range tasks {
		if _, ok := scheduledList[instanceID]; ok {
			continue
		}

		runtime := task.GetRuntime()
		if t, ok := j.tasks[instanceID]; ok {
			t.UpdateRuntime(runtime)
		}

		j.scheduleTaskWithMaxRunningInstancesInSLA(maxRunningInstances, instanceID, runtime)
		scheduledList[instanceID] = true
	}

	return nil
}

func (j *job) isRecoveryRetryable(err error) bool {
	log.WithError(err).
		Warn("job sla evaluation is failing during recovery")
	return true
}

// Only to be used during job receovery. TODO move to job goal state.
func (j *job) recoverJobWithSLA() {
	// First up currentScheduledTasks.
	currentScheduledTasks := uint32(0)
	j.RLock()
	for _, t := range j.tasks {
		runtime := t.GetRunTime()
		if runtime.GetState() != pb_task.TaskState_INITIALIZED && !util.IsPelotonStateTerminal(runtime.GetState()) {
			currentScheduledTasks++
		}
	}
	j.RUnlock()

	retryPolicy := backoff.NewRetryPolicy(retryCount, retryDuration)

	j.m.Lock()
	defer j.m.Unlock()
	j.Lock()
	defer j.Unlock()
	j.currentScheduledTasks = currentScheduledTasks
	// TODO move this message to debug after MaximumRunningInstances feature is stabilized
	log.WithField("job_id", j.id.GetValue()).
		WithField("current_scheduled_tasks", j.currentScheduledTasks).
		Info("current scheduled tasks set after recovery")
	backoff.Retry(func() error { return j.evaluateJobSLA() }, retryPolicy, j.isRecoveryRetryable)
}
