package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/common/goalstate"
	log "github.com/sirupsen/logrus"
)

// TaskReloadRuntime reloads task runtime into cache.
func TaskReloadRuntime(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.AddTask(taskEnt.instanceID)

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)
	if err != nil {
		return err
	}
	cachedTask.ReplaceRuntime(runtime, false)

	// This function is called when the runtime in cache is nil.
	// The task needs to re-enqueued into the goal state engine
	// so that it the corresponding action can be executed.
	goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	return nil
}

// TaskStateInvalid dumps a sentry error to indicate that the
// task goal state, state combination is not valid
func TaskStateInvalid(_ context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		return nil
	}
	log.WithFields(log.Fields{
		"current_state": cachedTask.CurrentState().State.String(),
		"goal_state":    cachedTask.GoalState().State.String(),
		"job_id":        taskEnt.jobID.Value,
		"instance_id":   cachedTask.ID(),
	}).Error("unexpected task state")
	goalStateDriver.mtx.taskMetrics.TaskInvalidState.Inc(1)
	return nil
}
