package goalstate

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	updateutil "code.uber.internal/infra/peloton/jobmgr/util/update"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

//  Runtime states of a Job
type transitionType int32

const (
	// When job state is unknown, transition type is also unknown
	transitionTypeUnknown transitionType = 0
	// When job transitions from one active state to another
	transitionTypeActiveActive transitionType = 1
	// When job transitions from active state to terminal state
	transitionTypeActiveTerminal transitionType = 2
	// When job transitions from terminal state to active state
	transitionTypeTerminalActive transitionType = 3
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

	jobConfig, _, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
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

// stateDeterminer determines job state given the current job runtime
type stateDeterminer interface {
	getState(ctx context.Context, jobRuntime *job.RuntimeInfo) (job.JobState, error)
}

func jobStateDeterminerFactory(
	jobRuntime *job.RuntimeInfo,
	stateCounts map[string]uint32,
	cachedJob cached.Job,
	config jobmgrcommon.JobConfig) stateDeterminer {
	totalInstanceCount := getTotalInstanceCount(stateCounts)
	// a job is partially created if:
	// 1. number of total instance count is smaller than configured
	// 2. there is no update going on
	if totalInstanceCount < config.GetInstanceCount() &&
		cachedJob.IsPartiallyCreated(config) &&
		!updateutil.HasUpdate(jobRuntime) {
		return newPartiallyCreatedJobStateDeterminer()
	}

	if cached.HasControllerTask(config) {
		return newControllerTaskJobStateDeterminer(
			cachedJob, stateCounts)
	}

	return newJobStateDeterminer(stateCounts)
}

func newJobStateDeterminer(
	stateCounts map[string]uint32,
) *jobStateDeterminer {
	return &jobStateDeterminer{
		stateCounts: stateCounts,
	}
}

type jobStateDeterminer struct {
	stateCounts map[string]uint32
}

func (d *jobStateDeterminer) getState(
	ctx context.Context,
	jobRuntime *job.RuntimeInfo,
) (job.JobState, error) {
	// use totalInstanceCount instead of config.GetInstanceCount,
	// because totalInstanceCount can be larger than config.GetInstanceCount
	// due to a race condition bug. Although the bug is fixed, the change is
	// needed to unblock affected jobs.
	// Also if in the future, similar bug occur again, using totalInstanceCount
	// would ensure the bug would not make a job stuck.
	totalInstanceCount := getTotalInstanceCount(d.stateCounts)
	// all succeeded -> succeeded
	if d.stateCounts[task.TaskState_SUCCEEDED.String()] == totalInstanceCount {
		return job.JobState_SUCCEEDED, nil
	}

	// some succeeded, some failed, some lost -> failed
	if d.stateCounts[task.TaskState_SUCCEEDED.String()]+
		d.stateCounts[task.TaskState_FAILED.String()]+
		d.stateCounts[task.TaskState_LOST.String()] == totalInstanceCount {
		return job.JobState_FAILED, nil
	}

	// some killed, some succeeded, some failed, some lost -> killed
	if d.stateCounts[task.TaskState_KILLED.String()] > 0 &&
		(d.stateCounts[task.TaskState_SUCCEEDED.String()]+
			d.stateCounts[task.TaskState_FAILED.String()]+
			d.stateCounts[task.TaskState_KILLED.String()]+
			d.stateCounts[task.TaskState_LOST.String()] == totalInstanceCount) {
		return job.JobState_KILLED, nil
	}

	if jobRuntime.State == job.JobState_KILLING {
		// jobState is set to KILLING in JobKill to avoid materialized view delay,
		// should keep the state to be KILLING unless job transits to terminal state
		return job.JobState_KILLING, nil
	}

	if d.stateCounts[task.TaskState_RUNNING.String()] > 0 {
		return job.JobState_RUNNING, nil
	}
	return job.JobState_PENDING, nil

}

func newPartiallyCreatedJobStateDeterminer() *partiallyCreatedJobStateDeterminer {
	return &partiallyCreatedJobStateDeterminer{}
}

type partiallyCreatedJobStateDeterminer struct{}

func (d *partiallyCreatedJobStateDeterminer) getState(
	ctx context.Context,
	jobRuntime *job.RuntimeInfo,
) (job.JobState, error) {
	return job.JobState_INITIALIZED, nil
}

func newControllerTaskJobStateDeterminer(
	cachedJob cached.Job,
	stateCounts map[string]uint32,
) *controllerTaskJobStateDeterminer {
	return &controllerTaskJobStateDeterminer{
		cachedJob:       cachedJob,
		batchDeterminer: newJobStateDeterminer(stateCounts),
	}
}

type controllerTaskJobStateDeterminer struct {
	cachedJob       cached.Job
	batchDeterminer *jobStateDeterminer
}

// If the job will be in terminal state, state of task would be determined by
// controller task. Otherwise it would be de
func (d *controllerTaskJobStateDeterminer) getState(
	ctx context.Context,
	jobRuntime *job.RuntimeInfo,
) (job.JobState, error) {
	jobState, err := d.batchDeterminer.getState(ctx, jobRuntime)
	if err != nil {
		return job.JobState_UNKNOWN, err
	}
	if !util.IsPelotonJobStateTerminal(jobState) {
		return jobState, nil
	}

	// In job config validation, it makes sure controller
	// task would be the first task
	controllerTask, err := d.cachedJob.AddTask(ctx, 0)
	if err != nil {
		return job.JobState_UNKNOWN, err
	}

	controllerTaskRuntime, err := controllerTask.GetRunTime(ctx)
	if err != nil {
		return job.JobState_UNKNOWN, err
	}
	switch controllerTaskRuntime.GetState() {
	case task.TaskState_SUCCEEDED:
		return job.JobState_SUCCEEDED, nil
	case task.TaskState_LOST:
		fallthrough
	case task.TaskState_FAILED:
		return job.JobState_FAILED, nil
	default:
		// only terminal state would enter switch statement,
		// so the state left must be KILLED
		return job.JobState_KILLED, nil
	}
}

// getTransitionType returns the type of state transition for this job.
// for example: a job being restarted would move from a terminal to active
// state and the state transition returned is transitionTypeTerminalActive
func getTransitionType(newState, oldState job.JobState) transitionType {
	newStateIsTerminal := util.IsPelotonJobStateTerminal(newState)
	oldStateIsTerminal := util.IsPelotonJobStateTerminal(oldState)
	if newStateIsTerminal && !oldStateIsTerminal {
		return transitionTypeActiveTerminal
	} else if !newStateIsTerminal && oldStateIsTerminal {
		return transitionTypeTerminalActive
	}
	return transitionTypeActiveActive
}

// determineJobRuntimeState determines the job state based on current
// job runtime state and task state counts.
// This function is not expected to be called when
// totalInstanceCount < config.GetInstanceCount.
// UNKNOWN state would be returned if no enough info is presented in
// cache. Caller should retry later after cache is filled in.
func determineJobRuntimeState(
	ctx context.Context,
	jobRuntime *job.RuntimeInfo,
	stateCounts map[string]uint32,
	config jobmgrcommon.JobConfig,
	goalStateDriver *driver,
	cachedJob cached.Job) (job.JobState, transitionType, error) {
	prevState := jobRuntime.GetState()
	jobStateDeterminer := jobStateDeterminerFactory(
		jobRuntime, stateCounts, cachedJob, config)
	jobState, err := jobStateDeterminer.getState(ctx, jobRuntime)
	if err != nil {
		return job.JobState_UNKNOWN, transitionTypeUnknown, err
	}

	// Check if a batch job is active for a very long time which may indicate
	// that the mv_task_by_state for some of the tasks might be out of sync
	// Also check if total instance count derived from MV is greater than what
	// configured which indicates MV is out of sync, and if
	// jobRuntimeCalculationViaCache flag is on
	// Recalculate job state from cache if this is the case.
	if shouldRecalculateJobState(cachedJob, config.GetType(), jobState,
		stateCounts, goalStateDriver.jobRuntimeCalculationViaCache, config) {
		jobState, err = recalculateJobStateFromCache(
			ctx, jobRuntime, cachedJob, jobState, config)
	}

	switch jobState {
	case job.JobState_SUCCEEDED:
		goalStateDriver.mtx.jobMetrics.JobSucceeded.Inc(1)
	case job.JobState_FAILED:
		goalStateDriver.mtx.jobMetrics.JobFailed.Inc(1)
	case job.JobState_KILLED:
		goalStateDriver.mtx.jobMetrics.JobKilled.Inc(1)

	}
	return jobState, getTransitionType(jobState, prevState), nil
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
		if jobRuntime.GetState() == job.JobState_KILLED &&
			jobRuntime.GetGoalState() == job.JobState_KILLED {
			// Job already killed, do not do anything
			return nil
		}

		if !cachedJob.IsPartiallyCreated(config) {
			// MV has not caught up, wait for it to catch up before doing anything
			return fmt.Errorf("dbs are not in sync")
		}
	} else if totalInstanceCount > config.GetInstanceCount() {
		// this branch can be hit due to MV delay under normal situation.
		// keep the debug log in case of real bug.
		log.WithField("job_id", id).
			WithField("total_instance_count", totalInstanceCount).
			WithField("instances", config.GetInstanceCount()).
			Debug("total instance count is greater than expected")
	}

	// determineJobRuntimeState would handle both
	// totalInstanceCount > config.GetInstanceCount() and
	// partially created job
	jobState, transition, err := determineJobRuntimeState(
		ctx, jobRuntime, stateCounts, config, goalStateDriver, cachedJob)
	if err != nil {
		return err
	}

	if jobRuntime.GetTaskStats() != nil &&
		reflect.DeepEqual(stateCounts, jobRuntime.GetTaskStats()) &&
		jobRuntime.GetState() == jobState {
		log.WithField("job_id", id).
			WithField("task_stats", stateCounts).
			Debug("Task stats did not change, return")

		// if an update is running for this job, enqueue it to goal state
		// TODO change this to use watch functionality from the cache
		err = enqueueJobUpdate(ctx, jobID, jobRuntime, goalStateDriver)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id).
				Error("failed to fetch update info in runtime updater")
			goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		}
		return err
	}

	jobRuntimeUpdate = setStartTime(
		cachedJob,
		jobRuntime,
		stateCounts,
		jobRuntimeUpdate,
	)

	jobRuntimeUpdate.State = jobState

	jobRuntimeUpdate = setCompletionTime(
		cachedJob,
		jobState,
		jobRuntimeUpdate,
	)

	jobRuntimeUpdate.TaskStats = stateCounts

	jobRuntimeUpdate.ResourceUsage = cachedJob.GetResourceUsage()

	// add to active jobs list BEFORE writing state to job runtime table.
	// Also write to active jobs list only when the job is being transitioned
	// from a terminal to active state. For active to active transitions, we
	// can assume that the job is already in this list from the time it was
	// first created. Terminal jobs will be removed from this list and must
	// be added back when they are rerun.
	if transition == transitionTypeTerminalActive {
		if err := goalStateDriver.jobStore.AddActiveJob(
			ctx, jobID); err != nil {
			return err
		}
	}

	// Update the job runtime
	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: jobRuntimeUpdate,
	}, nil,
		cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update jobRuntime in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	// if an update is running for this job, enqueue it as well
	// TODO change this to use watch functionality from the cache
	err = enqueueJobUpdate(ctx, jobID, jobRuntime, goalStateDriver)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch update info in runtime updater")
		goalStateDriver.mtx.jobMetrics.JobRuntimeUpdateFailed.Inc(1)
		return err
	}

	// Evaluate this job immediately when
	// 1. job state is terminal and no more task updates will arrive, or
	// 2. job is partially created and need to create additional tasks
	// (we may have no additional tasks coming in when job is
	// partially created)
	if util.IsPelotonJobStateTerminal(jobRuntimeUpdate.GetState()) ||
		(cachedJob.IsPartiallyCreated(config) &&
			!updateutil.HasUpdate(jobRuntime)) {
		goalStateDriver.EnqueueJob(jobID, time.Now())
	}

	log.WithField("job_id", id).
		WithField("updated_state", jobState.String()).
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

// setStartTime adds start time to jobRuntimeUpdate, if the job
// first starts. It returns the updated jobRuntimeUpdate.
func setStartTime(
	cachedJob cached.Job,
	jobRuntime *job.RuntimeInfo,
	stateCounts map[string]uint32,
	jobRuntimeUpdate *job.RuntimeInfo) *job.RuntimeInfo {
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
	return jobRuntimeUpdate
}

// setCompletionTime adds completion time to jobRuntimeUpdate, if the job
// completes. It returns the updated jobRuntimeUpdate.
func setCompletionTime(
	cachedJob cached.Job,
	jobState job.JobState,
	jobRuntimeUpdate *job.RuntimeInfo) *job.RuntimeInfo {
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
	return jobRuntimeUpdate
}

// recalculateJobStateFromCache gets the state counts from cached tasks instead
// of materialized view. We don't do this all the time because this requires
// walking through the list of ALL tasks of the job one by one and in acquiring
// the lock for each task once when fetching the current state. It is not
// desirable to do this all the time because this has potential to slow down
// event handling for these tasks.
func recalculateJobStateFromCache(
	ctx context.Context, jobRuntime *job.RuntimeInfo, cachedJob cached.Job,
	jobState job.JobState, config jobmgrcommon.JobConfig) (job.JobState, error) {

	tasks := cachedJob.GetAllTasks()
	stateCountsFromCache := make(map[string]uint32)
	for _, task := range tasks {
		state := task.CurrentState().State.String()
		if _, ok := stateCountsFromCache[state]; ok {
			stateCountsFromCache[state]++
		} else {
			stateCountsFromCache[state] = 1
		}
	}

	// in case we have a task with state unknown, it means that the task was not
	// present in cache. In this case, return the original state
	if _, ok := stateCountsFromCache[task.TaskState_UNKNOWN.String()]; ok {
		return jobState, nil
	}

	// recalculate jobState based on the new task state count
	jobStateDeterminer := jobStateDeterminerFactory(
		jobRuntime, stateCountsFromCache, cachedJob, config)
	jobState, err := jobStateDeterminer.getState(ctx, jobRuntime)
	return jobState, err
}

// shouldRecalculateJobState is true if the job state needs to be recalculated
func shouldRecalculateJobState(
	cachedJob cached.Job, jobType job.JobType, jobState job.JobState,
	stateCounts map[string]uint32,
	forceRecalculateFromCache bool, config jobmgrcommon.JobConfig) bool {
	return jobType == job.JobType_BATCH &&
		!util.IsPelotonJobStateTerminal(jobState) &&
		isJobStateStale(cachedJob, common.StaleJobStateDurationThreshold) ||
		getTotalInstanceCount(stateCounts) > config.GetInstanceCount() &&
			forceRecalculateFromCache
}

// isJobStateStale returns true if the job is in active state for more than the
// threshold duration
func isJobStateStale(cachedJob cached.Job, threshold time.Duration) bool {
	lastTaskUpdateTime := cachedJob.GetLastTaskUpdateTime()
	durationInCurrState := int64(
		float64(time.Now().UnixNano()) - lastTaskUpdateTime)
	if durationInCurrState >= threshold.Nanoseconds() {
		return true
	}
	return false
}

// enqueueJobUpdate enqueues the update to the goal state engine if a job
// update exists in the runtime, and it is not in terminal state
func enqueueJobUpdate(
	ctx context.Context,
	jobID *peloton.JobID,
	jobRuntime *job.RuntimeInfo,
	goalStateDriver *driver) error {
	if !updateutil.HasUpdate(jobRuntime) {
		return nil
	}

	updateInfo, err := goalStateDriver.updateStore.GetUpdateProgress(
		ctx, jobRuntime.GetUpdateID())
	if err != nil {
		return err
	}

	if !cached.IsUpdateStateTerminal(updateInfo.GetState()) {
		goalStateDriver.EnqueueUpdate(jobID, jobRuntime.GetUpdateID(), time.Now())
	}
	return nil
}
