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

	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch job config to kill a job")
		return err
	}

	instanceCount := jobConfig.GetInstanceCount()
	instRange := &task.InstanceRange{
		From: 0,
		To:   instanceCount,
	}
	runtimes, err := goalStateDriver.taskStore.GetTaskRuntimesForJobByRange(ctx, jobID, instRange)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to fetch task runtimes to kill a job")
		return err
	}

	// Update task runtimes in DB and cache to kill task
	updatedRuntimes := make(map[uint32]*task.RuntimeInfo)
	for instanceID, runtime := range runtimes {
		if runtime.GetGoalState() == task.TaskState_KILLED || util.IsPelotonStateTerminal(runtime.GetState()) {
			continue
		}
		runtime.GoalState = task.TaskState_KILLED
		runtime.Message = "Task stop API request"
		runtime.Reason = ""
		updatedRuntimes[instanceID] = runtime
	}

	cachedJob := goalStateDriver.jobFactory.GetJob(jobID)
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

	// Get job runtime and update job state to killing
	jobRuntime, err := goalStateDriver.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			Error("failed to get job runtime during job kill")
		return err
	}
	jobState := job.JobState_KILLING

	// If all instances have not been created, and all created instances are already killed,
	// then directly update the job state to KILLED.
	if len(updatedRuntimes) == 0 && jobRuntime.GetState() == job.JobState_INITIALIZED && cachedJob.IsPartiallyCreated() {
		jobState = job.JobState_KILLED
		for _, runtime := range runtimes {
			if !util.IsPelotonStateTerminal(runtime.GetState()) {
				jobState = job.JobState_KILLING
				break
			}
		}
	}
	jobRuntime.State = jobState

	err = goalStateDriver.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
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
