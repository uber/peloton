package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	log "github.com/sirupsen/logrus"
)

// TaskKilled handles killed tasks. The kill attempts in the cache needs to
// be reset to zero after task is successfully killed.
func TaskKilled(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
		}).Error("task is nil in cache with valid job")
		return nil
	}
	cachedTask.ClearKillAttempts()
	return nil
}

// TaskFailed handles initialized tasks with failed goal state.
// This is not a valid condition and this is implemented to recover
// tasks stuck in this state due to a previous job manager bug.
func TaskFailed(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", taskEnt.jobID).
			WithField("instance_id", taskEnt.instanceID).
			Error("failed to get task runtime during task fail action")
		return err
	}

	runtime.State = task.TaskState_FAILED
	err = cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: runtime}, cached.UpdateCacheAndDB)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	}
	return err
}

// TaskReloadRuntime reloads task runtime into cache.
func TaskReloadRuntime(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
		}).Error("task is nil in cache with valid job")
		return nil
	}

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)
	if err != nil {
		return err
	}
	cachedTask.UpdateRuntime(runtime)

	// This function is called when the runtime in cache is nil.
	// The task needs to re-enqueued into the goal state engine
	// so that it the corresponding action can be executed.
	goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
	return nil
}

// TaskPreempt implemets task preemption.
func TaskPreempt(ctx context.Context, entity goalstate.Entity) error {
	action, err := getPostPreemptAction(ctx, entity)
	if err != nil {
		log.WithError(err).
			Error("unable to get post preemption action")
		return err
	}
	if action != nil {
		action(ctx, entity)
	}
	return nil
}

// returns the action to be performed after preemption based on the task
// preemption policy
func getPostPreemptAction(ctx context.Context, entity goalstate.Entity) (goalstate.Action, error) {
	// Here we check what the task preemption policy is,
	// if killOnPreempt is set to true then we don't reschedule the task
	// after it is preempted
	var action goalstate.Action

	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver

	taskGoalState := taskEnt.GetGoalState().(cached.TaskStateVector)

	pp, err := getTaskPreemptionPolicy(ctx, taskEnt.jobID, taskEnt.instanceID,
		taskGoalState.ConfigVersion, goalStateDriver)
	if err != nil {
		log.WithError(err).
			Error("unable to get task preemption policy")
		return action, err
	}
	if pp != nil && pp.GetKillOnPreempt() {
		// We are done , we don't want to reschedule it
		return nil, nil
	}
	return TaskInitialize, nil
}

// getTaskPreemptionPolicy returns the restart policy of the task,
// used when the task is preempted
func getTaskPreemptionPolicy(ctx context.Context, jobID *peloton.JobID,
	instanceID uint32, configVersion uint64,
	goalStateDriver *driver) (*task.PreemptionPolicy,
	error) {
	config, err := goalStateDriver.taskStore.GetTaskConfig(ctx, jobID, instanceID,
		int64(configVersion))
	if err != nil {
		return nil, err
	}
	return config.GetPreemptionPolicy(), nil
}
