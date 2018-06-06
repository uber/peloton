package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

// JobKill will stop all tasks in the job.
func JobKill(ctx context.Context, entity goalstate.Entity) error {
	id := entity.GetID()
	jobID := &peloton.JobID{Value: id}
	goalStateDriver := entity.(*jobEntity).driver

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		return nil
	}
	tasks := cachedJob.GetAllTasks()

	// Update task runtimes in DB and cache to kill task
	updatedRuntimes := make(map[uint32]*task.RuntimeInfo)
	for instanceID, cachedTask := range tasks {
		runtime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id).
				WithField("instance_id", instanceID).
				Info("failed to fetch task runtime to kill a job")
			return err
		}

		if runtime.GetGoalState() == task.TaskState_KILLED || util.IsPelotonStateTerminal(runtime.GetState()) {
			continue
		}

		updatedRuntime := &task.RuntimeInfo{
			GoalState: task.TaskState_KILLED,
			Message:   "Task stop API request",
			Reason:    "",
		}
		updatedRuntimes[instanceID] = updatedRuntime
	}

	config, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("Failed to get job config")
		return err
	}

	err = cachedJob.UpdateTasks(ctx, updatedRuntimes, cached.UpdateCacheAndDB)

	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update task runtimes to kill a job")
		return err
	}

	// Schedule all tasks in goal state engine
	for instanceID := range updatedRuntimes {
		goalStateDriver.EnqueueTask(jobID, instanceID, time.Now())
	}

	// Only enqueue the job into goal state if any of the
	// tasks need to be killed.
	if len(updatedRuntimes) > 0 {
		EnqueueJobWithDefaultDelay(jobID, goalStateDriver, cachedJob)
	}

	// Get job runtime and update job state to killing
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime during job kill")
		return err
	}
	jobState := job.JobState_KILLING

	// If not all instances have been created, and all created instances are already killed,
	// then directly update the job state to KILLED.
	if len(updatedRuntimes) == 0 && jobRuntime.GetState() == job.JobState_INITIALIZED && cachedJob.IsPartiallyCreated(config) {
		jobState = job.JobState_KILLED
		for _, cachedTask := range tasks {
			runtime, err := cachedTask.GetRunTime(ctx)
			if err != nil || !util.IsPelotonStateTerminal(runtime.GetState()) {
				jobState = job.JobState_KILLING
				break
			}
		}
	}

	err = cachedJob.Update(ctx, &job.JobInfo{
		Runtime: &job.RuntimeInfo{State: jobState},
	}, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to update job runtime during job kill")
		return err
	}

	log.WithField("job_id", id).
		Info("initiated kill of all tasks in the job")
	return nil
}
