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

	runtime := &task.RuntimeInfo{
		State: task.TaskState_FAILED,
	}
	err := cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{taskEnt.instanceID: runtime}, cached.UpdateCacheAndDB)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		goalStateDriver.EnqueueJob(taskEnt.jobID, time.Now().Add(
			goalStateDriver.GetJobRuntimeDuration(cachedJob.GetJobType())))
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
	cachedTask := cachedJob.AddTask(taskEnt.instanceID)

	runtime, err := goalStateDriver.taskStore.GetTaskRuntime(ctx, taskEnt.jobID, taskEnt.instanceID)
	if err != nil {
		return err
	}
	cachedTask.UpdateRuntime(ctx, runtime, cached.UpdateCacheOnly)

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

// returns the action to be performed after preemption based on the task
// preemption policy
func getPostPreemptAction(ctx context.Context, entity goalstate.Entity) (goalstate.Action, error) {
	// Here we check what the task preemption policy is,
	// if killOnPreempt is set to true then we don't reschedule the task
	// after it is preempted
	var action goalstate.Action

	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver

	taskState := taskEnt.GetState().(cached.TaskStateVector)

	pp, err := getTaskPreemptionPolicy(ctx, taskEnt.jobID, taskEnt.instanceID,
		taskState.ConfigVersion, goalStateDriver)
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
	config, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		jobID,
		instanceID,
		configVersion)
	if err != nil {
		return nil, err
	}
	return config.GetPreemptionPolicy(), nil
}
