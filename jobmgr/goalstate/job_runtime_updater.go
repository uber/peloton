package goalstate

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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
	cachedConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job config")
		return err
	}

	// Save a read to DB if maxRunningInstances is 0
	maxRunningInstances := cachedConfig.GetSLA().GetMaximumRunningInstances()
	if maxRunningInstances == 0 {
		return nil
	}

	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job config in start instances")
		return err
	}
	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job runtime during start instances")
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
			cachedJob.ReplaceTasks(map[uint32]*task.RuntimeInfo{instID: taskRuntime}, false)
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

	return sendTasksToResMgr(ctx, jobID, tasks, jobConfig, goalStateDriver)
}

// jobStateDeterminer determines job state given the current job runtime
type jobStateDeterminer interface {
	getState(jobRuntime *job.RuntimeInfo) job.JobState
}

func jobStateDeterminerFactory(
	stateCounts map[string]uint32,
	cachedJob cached.Job,
	config cached.JobConfig) jobStateDeterminer {
	totalInstanceCount := getTotalInstanceCount(stateCounts)
	if totalInstanceCount < config.GetInstanceCount() &&
		cachedJob.IsPartiallyCreated(config) {
		return &partiallyCreatedJobStateDeterminer{}
	}
	if config.GetType() == job.JobType_SERVICE {
		return &serviceJobStateDeterminer{
			stateCounts: stateCounts,
		}
	}
	return &batchJobStateDeterminer{
		stateCounts: stateCounts,
	}
}

type batchJobStateDeterminer struct {
	stateCounts map[string]uint32
}

func (d *batchJobStateDeterminer) getState(
	jobRuntime *job.RuntimeInfo,
) job.JobState {
	// use totalInstanceCount instead of config.GetInstanceCount,
	// because totalInstanceCount can be larger than config.GetInstanceCount
	// due to a race condition bug. Although the bug is fixed, the change is
	// needed to unblock affected jobs.
	// Also if in the future, similar bug occur again, using totalInstanceCount
	// would ensure the bug would not make a job stuck.
	totalInstanceCount := getTotalInstanceCount(d.stateCounts)
	if d.stateCounts[task.TaskState_SUCCEEDED.String()] == totalInstanceCount {
		return job.JobState_SUCCEEDED
	} else if d.stateCounts[task.TaskState_SUCCEEDED.String()]+
		d.stateCounts[task.TaskState_FAILED.String()] == totalInstanceCount {
		return job.JobState_FAILED
	} else if d.stateCounts[task.TaskState_KILLED.String()] > 0 &&
		(d.stateCounts[task.TaskState_SUCCEEDED.String()]+
			d.stateCounts[task.TaskState_FAILED.String()]+
			d.stateCounts[task.TaskState_KILLED.String()] == totalInstanceCount) {
		return job.JobState_KILLED
	} else if jobRuntime.State == job.JobState_KILLING {
		// jobState is set to KILLING in JobKill to avoid materialized view delay,
		// should keep the state to be KILLING unless job transits to terminal state
		return job.JobState_KILLING
	} else if d.stateCounts[task.TaskState_RUNNING.String()] > 0 {
		return job.JobState_RUNNING
	}
	return job.JobState_PENDING

}

type serviceJobStateDeterminer struct {
	stateCounts map[string]uint32
}

func (d *serviceJobStateDeterminer) getState(
	jobRuntime *job.RuntimeInfo,
) job.JobState {
	// use totalInstanceCount instead of config.GetInstanceCount,
	// because totalInstanceCount can be larger than config.GetInstanceCount
	// due to a race condition bug. Although the bug is fixed, the change is
	// needed to unblock affected jobs.
	// Also if in the future, similar bug occur again, using totalInstanceCount
	// would ensure the bug would not make a job stuck.
	totalInstanceCount := getTotalInstanceCount(d.stateCounts)
	// For tasks of service job, SUCCEEDED and FAILED states are transient
	// states. Task with these states would move to INITIALIZED shortly.
	// Therefore, service jobs should never enter SUCCEEDED/FAILED state,
	// since they should never be terminal unless KILLED.
	if d.stateCounts[task.TaskState_KILLED.String()] == totalInstanceCount {
		return job.JobState_KILLED
	} else if jobRuntime.State == job.JobState_KILLING {
		// jobState is set to KILLING in JobKill to avoid materialized view delay,
		// should keep the state to be KILLING unless job transits to terminal state
		return job.JobState_KILLING
	} else if d.stateCounts[task.TaskState_RUNNING.String()] > 0 {
		return job.JobState_RUNNING
	}
	return job.JobState_PENDING

}

type partiallyCreatedJobStateDeterminer struct{}

func (d *partiallyCreatedJobStateDeterminer) getState(
	jobRuntime *job.RuntimeInfo,
) job.JobState {
	return job.JobState_INITIALIZED
}

// determineJobRuntimeState determines the job state based on current
// job runtime state and task state counts.
// This function is not expected to be called when
// totalInstanceCount < config.GetInstanceCount
func determineJobRuntimeState(jobRuntime *job.RuntimeInfo,
	stateCounts map[string]uint32,
	config cached.JobConfig,
	goalStateDriver *driver,
	cachedJob cached.Job) job.JobState {
	jobStateDeterminer := jobStateDeterminerFactory(stateCounts, cachedJob, config)
	jobState := jobStateDeterminer.getState(jobRuntime)
	switch jobState {
	case job.JobState_SUCCEEDED:
		goalStateDriver.mtx.jobMetrics.JobSucceeded.Inc(1)
	case job.JobState_FAILED:
		goalStateDriver.mtx.jobMetrics.JobFailed.Inc(1)
	case job.JobState_KILLED:
		goalStateDriver.mtx.jobMetrics.JobKilled.Inc(1)

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

	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	config, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job config")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	stateCounts, err := goalStateDriver.taskStore.GetTaskStateSummaryForJob(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch task state summary")
		return err
	}

	var jobState job.JobState
	jobRuntimeUpdate := &job.RuntimeInfo{}
	totalInstanceCount := getTotalInstanceCount(stateCounts)
	// if job is KILLED: do nothing
	// if job is partially created: set job to INITIALIZED and enqueue the job
	// else: return error and reschedule the job
	if totalInstanceCount < config.GetInstanceCount() {
		if jobRuntime.GetState() == job.JobState_KILLED && jobRuntime.GetGoalState() == job.JobState_KILLED {
			// Job already killed, do not do anything
			return nil
		}
		// Either MV view has not caught up or all instances have not been created
		if cachedJob.IsPartiallyCreated(config) {
			// all instances have not been created, trigger recovery
			jobState = job.JobState_INITIALIZED
		} else {
			// MV has not caught up, wait for it to catch up before doing anything
			return fmt.Errorf("dbs are not in sync")
		}
	} else {
		if totalInstanceCount > config.GetInstanceCount() {
			log.WithField("job_id", id).
				WithField("total_instance_count", totalInstanceCount).
				WithField("instances", config.GetInstanceCount()).
				Error("total instance count is greater than expected")
		}

		// case totalInstanceCount > config.GetInstanceCount is handled
		// by determineJobRuntimeState
		jobState = determineJobRuntimeState(jobRuntime, stateCounts,
			config, goalStateDriver, cachedJob)

		if jobRuntime.GetTaskStats() != nil &&
			reflect.DeepEqual(stateCounts, jobRuntime.GetTaskStats()) &&
			jobRuntime.GetState() == jobState {
			log.WithField("job_id", id).
				WithField("task_stats", stateCounts).
				Debug("Task stats did not change, return")
			return nil
		}
	}

	getFirstTaskUpdateTime := cachedJob.GetFirstTaskUpdateTime()
	if getFirstTaskUpdateTime != 0 && jobRuntime.StartTime == "" {
		count := uint32(0)
		for _, state := range taskStatesAfterStart {
			count += stateCounts[state.String()]
		}

		if count > 0 {
			jobRuntimeUpdate.StartTime = formatTime(getFirstTaskUpdateTime, time.RFC3339Nano)
		}
	}

	jobRuntimeUpdate.State = jobState
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
		jobRuntimeUpdate.CompletionTime = completionTime
	}
	jobRuntimeUpdate.TaskStats = stateCounts

	// Update the job runtime
	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: jobRuntimeUpdate,
	}, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update jobRuntime in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	// Evaluate this job immediately when
	// 1. job state is terminal and no more task updates will arrive, or
	// 2. job is partially created and need to create additional tasks
	// (we may have no additional tasks coming in when job is
	// partially created)
	if util.IsPelotonJobStateTerminal(jobRuntimeUpdate.GetState()) ||
		cachedJob.IsPartiallyCreated(config) {
		goalStateDriver.EnqueueJob(jobID, time.Now())
	}

	log.WithField("job_id", id).
		WithField("updated_state", jobRuntime.State.String()).
		Info("job runtime updater completed")

	goalStateDriver.mtx.jobMetrics.JobRuntimeUpdated.Inc(1)
	return nil
}

func getTotalInstanceCount(stateCounts map[string]uint32) uint32 {
	totalInstanceCount := uint32(0)
	for _, state := range task.TaskState_name {
		totalInstanceCount += stateCounts[state]
	}
	return totalInstanceCount
}
