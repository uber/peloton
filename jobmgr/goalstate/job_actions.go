package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// JobEnqueue enqueues the job back into the goal state engine.
func JobEnqueue(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver
	goalStateDriver.EnqueueJob(jobEnt.id, time.Now())
	return nil
}

// JobUntrack deletes the job and tasks from the goal state engine and the cache.
func JobUntrack(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)

	if cachedJob == nil {
		return nil
	}

	jobConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		if !yarpcerrors.IsNotFound(err) {
			// if config is not found, untrack the job from cache
			return err
		}
	} else if jobConfig.GetType() == job.JobType_SERVICE {
		// service jobs are always active and never untracked
		return nil
	}

	// First clean from goal state
	taskMap := cachedJob.GetAllTasks()
	for instID := range taskMap {
		goalStateDriver.DeleteTask(jobEnt.id, instID)
	}
	goalStateDriver.DeleteJob(jobEnt.id)

	// Next clean up from the cache
	goalStateDriver.jobFactory.ClearJob(jobEnt.id)
	return nil
}

// JobStateInvalid dumps a sentry error to indicate that the
// job goal state, state combination is not valid
func JobStateInvalid(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)

	if cachedJob == nil {
		return nil
	}

	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"current_state": jobRuntime.State.String(),
		"goal_state":    jobRuntime.GoalState.String(),
		"job_id":        jobEnt.GetID(),
	}).Error("unexpected job state")
	goalStateDriver.mtx.jobMetrics.JobInvalidState.Inc(1)
	return nil
}

// JobRecover tries to recover a partially created job.
// If job is not recoverable, it would untrack the job
func JobRecover(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.AddJob(jobEnt.id)

	_, err := cachedJob.GetConfig(ctx)
	// config exists, it means the job is created, move the state to initialized
	if err == nil {
		log.WithFields(log.Fields{
			"job_id": jobEnt.GetID(),
		}).Info("job config is found and job is recoverable")

		if err := cachedJob.Update(ctx, &job.JobInfo{
			Runtime: &job.RuntimeInfo{State: job.JobState_INITIALIZED},
		}, nil, cached.UpdateCacheAndDB); err != nil {
			return err
		}
		goalStateDriver.EnqueueJob(jobEnt.id, time.Now())
		return nil
	}

	// config is not created, job cannot be recovered.
	if yarpcerrors.IsNotFound(err) {
		log.WithFields(log.Fields{
			"job_id": jobEnt.GetID(),
		}).Info("job is not recoverable due to missing config")
		return JobUntrack(ctx, entity)
	}

	return err
}
