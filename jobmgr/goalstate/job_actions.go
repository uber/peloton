package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/common/goalstate"
)

// JobEnqueue enqueues the job back into the goal state engine.
func JobEnqueue(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := entity.(*jobEntity).driver
	goalStateDriver.EnqueueJob(jobEnt.id, time.Now())
	return nil
}

// JobClearRuntime clears the job runtime from cache.
func JobClearRuntime(ctx context.Context, entity goalstate.Entity) error {
	jobEnt := entity.(*jobEntity)
	goalStateDriver := jobEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(jobEnt.id)
	if cachedJob == nil {
		return nil
	}
	cachedJob.ClearRuntime()
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
