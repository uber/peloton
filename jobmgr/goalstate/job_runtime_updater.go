package goalstate

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

// taskStatesAfterStart is the set of Peloton task states which
// indicate a task is being or has already been started.
var taskStatesAfterStart = []task.TaskState{
	task.TaskState_STARTING,
	task.TaskState_RUNNING,
	task.TaskState_SUCCEEDED,
	task.TaskState_FAILED,
	task.TaskState_LOST,
	task.TaskState_PREEMPTING,
	task.TaskState_KILLING,
	task.TaskState_KILLED,
}

// taskStatesScheduled is the set of Peloton task states which
// indicate a task has been sent to resource manager, or has been
// placed by the resource manager, and has not reached a terminal state.
// It will be used to determine which tasks in DB (or cache) have not yet
// been sent to resource manager for getting placed.
var taskStatesScheduled = []task.TaskState{
	task.TaskState_RUNNING,
	task.TaskState_PENDING,
	task.TaskState_LAUNCHED,
	task.TaskState_READY,
	task.TaskState_PLACING,
	task.TaskState_PLACED,
	task.TaskState_LAUNCHING,
	task.TaskState_STARTING,
	task.TaskState_PREEMPTING,
	task.TaskState_KILLING,
}

// formatTime converts a Unix timestamp to a string format of the
// given layout in UTC. See https://golang.org/pkg/time/ for possible
// time layout in golang. For example, it will return RFC3339 format
// string like 2017-01-02T11:00:00.123456789Z if the layout is
// time.RFC3339Nano
func formatTime(timestamp float64, layout string) string {
	seconds := int64(timestamp)
	nanoSec := int64((timestamp - float64(seconds)) *
		float64(time.Second/time.Nanosecond))
	return time.Unix(seconds, nanoSec).UTC().Format(layout)
}

// JobEvaluateMaxRunningInstancesSLA evaluates the maximum running instances job SLA
// and determines instances to start if any.
func JobEvaluateMaxRunningInstancesSLA(ctx context.Context, entity goalstate.Entity) error {
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver
	cachedJob := goalStateDriver.jobFactory.AddJob(jobID)

	// TODO SLA is stored in cache. Fetch from cache instead of DB.
	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job config in start instances")
		return err
	}

	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()
	if maxRunningInstances == 0 {
		return nil
	}

	runtime, err := goalStateDriver.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime during start instances")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	if runtime.GetGoalState() == job.JobState_KILLED {
		return nil
	}

	stateCounts := runtime.GetTaskStats()

	currentScheduledInstances := uint32(0)
	for _, state := range taskStatesScheduled {
		currentScheduledInstances += stateCounts[state.String()]
	}

	if currentScheduledInstances >= maxRunningInstances {
		if currentScheduledInstances > maxRunningInstances {
			log.WithFields(log.Fields{
				"current_scheduled_tasks": currentScheduledInstances,
				"max_running_instances":   maxRunningInstances,
				"job_id":                  id,
			}).Info("scheduled instances exceed max running instances")
			goalStateDriver.mtx.jobMetrics.JobMaxRunningInstancesExcceeding.Inc(int64(currentScheduledInstances - maxRunningInstances))
		}
		log.WithField("current_scheduled_tasks", currentScheduledInstances).
			WithField("job_id", id).
			Debug("no instances to start")
		return nil
	}
	tasksToStart := maxRunningInstances - currentScheduledInstances

	initializedTasks, err := goalStateDriver.taskStore.GetTaskIDsForJobAndState(ctx, jobID, task.TaskState_INITIALIZED.String())
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch initialized task list")
		return err
	}

	log.WithFields(log.Fields{
		"job_id":                      id,
		"max_running_instances":       maxRunningInstances,
		"current_scheduled_instances": currentScheduledInstances,
		"length_initialized_tasks":    len(initializedTasks),
		"tasks_to_start":              tasksToStart,
	}).Debug("find tasks to start")

	var tasks []*task.TaskInfo
	for _, instID := range initializedTasks {
		if tasksToStart <= 0 {
			break
		}

		// MV view may run behind. So, make sure that task state is indeed INITIALIZED.
		taskRuntime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, jobID, instID)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id).
				WithField("instance_id", instID).
				Error("failed to fetch task runtimeme")
			continue
		}

		if taskRuntime.GetState() != task.TaskState_INITIALIZED {
			// Task wrongly set to INITIALIZED, ignore.
			tasksToStart--
			continue
		}

		t := cachedJob.GetTask(instID)
		if t == nil {
			// Add the task to cache if not found.
			cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{instID: taskRuntime}, cached.UpdateCacheOnly)
		}

		if goalStateDriver.IsScheduledTask(jobID, instID) {
			continue
		}

		taskinfo := &task.TaskInfo{
			JobId:      jobID,
			InstanceId: instID,
			Runtime:    taskRuntime,
			Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[instID]),
		}
		tasks = append(tasks, taskinfo)
		tasksToStart--
	}

	// Keeping the commented code when we have write through cache, then we
	// can read from cache instead of DB.
	/*j.RLock()
	defer j.RUnlock()

	for _, task := range j.initializedTasks {
		if tasksToStart == 0 {
			// TBD remove this log after testing
			log.WithField("job_id", j.id.GetValue()).
				WithField("started_tasks", (maxRunningInstances - currentScheduledInstances)).
				Info("scheduled tasks")
			break
		}

		if task.IsScheduled() {
			continue
		}

		j.m.taskScheduler.schedule(task, time.Now())
		tasksToStart--
	}*/
	return sendTasksToResMgr(ctx, jobID, tasks, jobConfig, goalStateDriver)
}

// determineJobRuntimeState determines the job state based on current
// job runtime state and task state counts.
func determineJobRuntimeState(jobRuntime *job.RuntimeInfo,
	stateCounts map[string]uint32,
	instances uint32,
	goalStateDriver *driver,
	cachedJob cached.Job) job.JobState {
	var jobState job.JobState

	if jobRuntime.State == job.JobState_INITIALIZED && cachedJob.IsPartiallyCreated() {
		// do not do any thing as all tasks have not been created yet
		jobState = job.JobState_INITIALIZED
	} else if stateCounts[task.TaskState_SUCCEEDED.String()] == instances {
		jobState = job.JobState_SUCCEEDED
		goalStateDriver.mtx.jobMetrics.JobSucceeded.Inc(1)
	} else if stateCounts[task.TaskState_SUCCEEDED.String()]+
		stateCounts[task.TaskState_FAILED.String()] == instances {
		jobState = job.JobState_FAILED
		goalStateDriver.mtx.jobMetrics.JobFailed.Inc(1)
	} else if stateCounts[task.TaskState_KILLED.String()] > 0 &&
		(stateCounts[task.TaskState_SUCCEEDED.String()]+
			stateCounts[task.TaskState_FAILED.String()]+
			stateCounts[task.TaskState_KILLED.String()] == instances) {
		jobState = job.JobState_KILLED
		goalStateDriver.mtx.jobMetrics.JobKilled.Inc(1)
	} else if stateCounts[task.TaskState_RUNNING.String()] > 0 {
		jobState = job.JobState_RUNNING
	} else {
		jobState = job.JobState_PENDING
	}

	return jobState
}

// JobRuntimeUpdater updates the job runtime.
// When the jobmgr leader fails over, the goal state driver runs syncFromDB which enqueues all recovered jobs
// into goal state, which will then run the job runtime updater and update the out-of-date runtime info.
func JobRuntimeUpdater(ctx context.Context, entity goalstate.Entity) error {
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver
	cachedJob := goalStateDriver.jobFactory.AddJob(jobID)

	log.WithField("job_id", id).
		Info("running job runtime update")

	jobRuntime, err := goalStateDriver.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	instances := cachedJob.GetInstanceCount()
	if instances <= 0 {
		jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id).
				Error("failed to get job config in runtime updater")
			goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
			return err
		}
		cachedJob.Update(ctx, &job.JobInfo{
			Config:  jobConfig,
			Runtime: jobRuntime,
		}, cached.UpdateCacheOnly)
		instances = cachedJob.GetInstanceCount()
	}

	// Keeping the commented code when we have write through cache, then we
	// can read from cache instead of DB.
	/*stateCounts := make(map[string]uint32)
	taskMap := j.GetTasks()
	j.clearInitializedTaskMap()

	for _, task := range taskMap {
		runtime := task.GetRunTime()
		retry := 0
		for retry < 1000 {
			if runtime != nil {
				break
			}
			time.Sleep(1 * time.Millisecond)
			log.WithField("job_id", j.ID()).
			    WithField("instance_id", task.ID()).
			    Info("reloading the task runtime within job runtime updater")
			task.reloadRuntime(ctx)
			retry++
		}
		if runtime == nil {
			return true, fmt.Errorf("cannot fetch task runtime")
		}

		stateCounts[runtime.GetState().String()]++

		if runtime.GetState() == task.TaskState_INITIALIZED && runtime.GetGoalState() != task.TaskState_KILLED {
			j.addTaskToInitializedTaskMap(task.(*task))
		}
	}*/

	stateCounts, err := goalStateDriver.taskStore.GetTaskStateSummaryForJob(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch task state summary")
		return err
	}

	totalInstanceCount := uint32(0)
	for _, state := range task.TaskState_name {
		totalInstanceCount += stateCounts[state]
	}

	if totalInstanceCount != instances {
		if jobRuntime.GetState() == job.JobState_KILLED && jobRuntime.GetGoalState() == job.JobState_KILLED {
			// Job already killed, do not do anything
			return nil
		}
		// Either MV view has not caught up or all instances have not been created
		if cachedJob.IsPartiallyCreated() {
			// all instances have not been created, trigger recovery
			jobRuntime.State = job.JobState_INITIALIZED
		} else {
			// MV has not caught up, wait for it to catch up before doing anything
			return fmt.Errorf("dbs are not in sync")
		}
	}

	jobState := determineJobRuntimeState(jobRuntime, stateCounts, instances, goalStateDriver, cachedJob)

	if jobRuntime.GetTaskStats() != nil && reflect.DeepEqual(stateCounts, jobRuntime.GetTaskStats()) && jobRuntime.GetState() == jobState {
		log.WithField("job_id", id).
			WithField("task_stats", stateCounts).
			Debug("Task stats did not change, return")
		return nil
	}

	getFirstTaskUpdateTime := cachedJob.GetFirstTaskUpdateTime()
	if getFirstTaskUpdateTime != 0 && jobRuntime.StartTime == "" {
		count := uint32(0)
		for _, state := range taskStatesAfterStart {
			count += stateCounts[state.String()]
		}

		if count > 0 {
			jobRuntime.StartTime = formatTime(getFirstTaskUpdateTime, time.RFC3339Nano)
		}
	}

	jobRuntime.State = jobState
	if util.IsPelotonJobStateTerminal(jobState) {
		// In case a job moved from PENDING/INITIALIZED to KILLED state,
		// the lastTaskUpdateTime will be 0. In this case, we will use
		// time.Now() as default completion time since a job in terminal
		// state should always have a completion time
		completionTime := time.Now().UTC().Format(time.RFC3339Nano)
		lastTaskUpdateTime := cachedJob.GetLastTaskUpdateTime()
		if lastTaskUpdateTime != 0 {
			completionTime = formatTime(lastTaskUpdateTime, time.RFC3339Nano)
		}
		jobRuntime.CompletionTime = completionTime
	}
	jobRuntime.TaskStats = stateCounts

	// Update the job runtime
	err = goalStateDriver.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update jobRuntime in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) {
		// Evaluate this job immediately as no more task updates will arrive
		goalStateDriver.EnqueueJob(jobID, time.Now())
	}

	log.WithField("job_id", id).
		WithField("updated_state", jobRuntime.State.String()).
		Info("job runtime updater completed")

	goalStateDriver.mtx.jobMetrics.JobRuntimeUpdated.Inc(1)
	if jobRuntime.State == job.JobState_INITIALIZED {
		// This should be hit for only old jobs created with a code version with no job goal state
		return fmt.Errorf("trigger job recovery")
	}
	return nil
}
