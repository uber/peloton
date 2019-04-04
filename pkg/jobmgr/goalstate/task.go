// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"

	log "github.com/sirupsen/logrus"
)

// _defaultTaskActionTimeout is the context timeout for all task actions.
const (
	_defaultTaskActionTimeout = 5 * time.Second
)

// TaskAction is a string for task actions.
type TaskAction string

const (
	// NoTaskAction implies do not take any action
	NoTaskAction TaskAction = "noop"
	// StartAction starts a task by sending it to resource manager
	StartAction TaskAction = "start_task"
	// StopAction kills the task
	StopAction TaskAction = "stop_task"
	// ExecutorShutdownAction shuts down executor directly after StopAction timeout
	ExecutorShutdownAction TaskAction = "executor_shutdown"
	// InitializeAction re-initializes the task and regenerates the mesos task id
	InitializeAction TaskAction = "initialize_task"
	// ReloadTaskRuntime reload the task runtime into cache
	ReloadTaskRuntime TaskAction = "reload_runtime"
	// LaunchRetryAction is run after task launch to either send to resource manager
	// that task has been successfully launched or re-initialize the task after lauch timeout
	LaunchRetryAction TaskAction = "launch_retry"
	// FailRetryAction retries a failed task
	FailRetryAction TaskAction = "fail_retry"
	// TerminatedRetryAction helps restart terminated tasks with throttling as well as
	// fail the task update if the task does not come up for max instance retries.
	TerminatedRetryAction TaskAction = "terminated_retry"
	// DeleteAction deletes the task from cache and its runtime from the DB
	DeleteAction TaskAction = "delete_task"
	// TaskStateInvalidAction is executed when a task enters
	// invalid current state and goal state combination, and it logs a sentry error
	TaskStateInvalidAction TaskAction = "state_invalid"
)

// _taskActionsMaps maps the task action string to task action function
var (
	_taskActionsMaps = map[TaskAction]goalstate.ActionExecute{
		NoTaskAction:           nil,
		StartAction:            TaskStart,
		StopAction:             TaskStop,
		InitializeAction:       TaskInitialize,
		ReloadTaskRuntime:      TaskReloadRuntime,
		LaunchRetryAction:      TaskLaunchRetry,
		TerminatedRetryAction:  TaskTerminatedRetry,
		FailRetryAction:        TaskFailRetry,
		ExecutorShutdownAction: TaskExecutorShutdown,
		DeleteAction:           TaskDelete,
		TaskStateInvalidAction: TaskStateInvalid,
	}
)

var (
	// _isoVersionsTaskRules maps current states to action, given a goal state:
	// goal-state -> current-state -> action.
	// It assumes task's runtime and goal are at the same version
	_isoVersionsTaskRules = map[task.TaskState]map[task.TaskState]TaskAction{
		task.TaskState_UNKNOWN: {
			// This reloads the task runtime from DB if the task runtime in cache is nil
			task.TaskState_UNKNOWN: ReloadTaskRuntime,
		},
		task.TaskState_RUNNING: {
			task.TaskState_INITIALIZED: StartAction,
			task.TaskState_LAUNCHED:    LaunchRetryAction,
			task.TaskState_STARTING:    LaunchRetryAction,
			task.TaskState_SUCCEEDED:   TerminatedRetryAction,
			task.TaskState_FAILED:      TerminatedRetryAction,
			task.TaskState_KILLED:      TerminatedRetryAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
			task.TaskState_LOST:        TerminatedRetryAction,
		},
		task.TaskState_SUCCEEDED: {
			task.TaskState_INITIALIZED: StartAction,
			task.TaskState_LAUNCHED:    LaunchRetryAction,
			task.TaskState_STARTING:    LaunchRetryAction,
			task.TaskState_FAILED:      FailRetryAction,
			task.TaskState_KILLED:      FailRetryAction,
			task.TaskState_LOST:        FailRetryAction,
		},
		task.TaskState_KILLED: {
			task.TaskState_INITIALIZED: StopAction,
			task.TaskState_PENDING:     StopAction,
			task.TaskState_LAUNCHED:    StopAction,
			task.TaskState_STARTING:    StopAction,
			task.TaskState_RUNNING:     StopAction,
			task.TaskState_LOST:        NoTaskAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
		},
		task.TaskState_DELETED: {
			task.TaskState_INITIALIZED: StopAction,
			task.TaskState_PENDING:     StopAction,
			task.TaskState_LAUNCHED:    StopAction,
			task.TaskState_STARTING:    StopAction,
			task.TaskState_RUNNING:     StopAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
			task.TaskState_LOST:        DeleteAction,
			task.TaskState_SUCCEEDED:   DeleteAction,
			task.TaskState_FAILED:      DeleteAction,
			task.TaskState_KILLED:      DeleteAction,
		},
		task.TaskState_FAILED: {
			// FAILED is not a valid task goal state.
			task.TaskState_INITIALIZED: TaskStateInvalidAction,
			task.TaskState_PENDING:     TaskStateInvalidAction,
			task.TaskState_LAUNCHED:    TaskStateInvalidAction,
			task.TaskState_STARTING:    TaskStateInvalidAction,
			task.TaskState_RUNNING:     TaskStateInvalidAction,
			task.TaskState_SUCCEEDED:   TaskStateInvalidAction,
			task.TaskState_FAILED:      TaskStateInvalidAction,
			task.TaskState_LOST:        TaskStateInvalidAction,
			task.TaskState_KILLING:     TaskStateInvalidAction,
			task.TaskState_KILLED:      TaskStateInvalidAction,
		},
		task.TaskState_PREEMPTING: {
			// PREEMPTING is used only for batch jobs which need to be killed
			// on preemption.
			// TODO @avyas: remove once we have the longer term fix for this.
			task.TaskState_INITIALIZED: StopAction,
			task.TaskState_PENDING:     StopAction,
			task.TaskState_LAUNCHED:    StopAction,
			task.TaskState_STARTING:    StopAction,
			task.TaskState_RUNNING:     StopAction,
			task.TaskState_LOST:        NoTaskAction,
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
		},
	}
)

// NewTaskEntity implements the goal state Entity interface for tasks.
func NewTaskEntity(jobID *peloton.JobID, instanceID uint32, driver *driver) goalstate.Entity {
	return &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     driver,
	}
}

type taskEntity struct {
	jobID      *peloton.JobID // job identifier
	instanceID uint32         // instance identifier
	driver     *driver        // the goal state driver
}

func (t *taskEntity) GetID() string {
	// return task identifier
	taskID := fmt.Sprintf("%s-%d", t.jobID.GetValue(), t.instanceID)
	return taskID
}

func (t *taskEntity) GetState() interface{} {
	cachedJob := t.driver.jobFactory.AddJob(t.jobID)
	cachedTask := cachedJob.GetTask(t.instanceID)
	if cachedTask == nil {
		// return UNKNOWN to reload the task runtime into cache
		return cached.TaskStateVector{
			State: task.TaskState_UNKNOWN,
		}
	}
	return cachedTask.CurrentState()
}

func (t *taskEntity) GetGoalState() interface{} {
	cachedJob := t.driver.jobFactory.AddJob(t.jobID)
	cachedTask := cachedJob.GetTask(t.instanceID)
	if cachedTask == nil {
		// return UNKNOWN to reload the task runtime into cache
		return cached.TaskStateVector{
			State: task.TaskState_UNKNOWN,
		}
	}
	return cachedTask.GoalState()
}

func (t *taskEntity) GetActionList(
	state interface{},
	goalState interface{}) (
	context.Context,
	context.CancelFunc,
	[]goalstate.Action) {
	var actions []goalstate.Action

	taskState := state.(cached.TaskStateVector)
	taskGoalState := goalState.(cached.TaskStateVector)

	ctx, cancel := context.WithTimeout(context.Background(), _defaultTaskActionTimeout)

	if taskState.State == task.TaskState_UNKNOWN || taskGoalState.State == task.TaskState_UNKNOWN {
		// no runtime in cache, reload the task runtime
		actions = append(actions, goalstate.Action{
			Name:    string(ReloadTaskRuntime),
			Execute: TaskReloadRuntime,
		})
		return ctx, cancel, actions
	}

	actionStr := t.suggestTaskAction(taskState, taskGoalState)
	action := _taskActionsMaps[actionStr]

	log.WithField("job_id", t.jobID.GetValue()).
		WithField("instance_id", t.instanceID).
		WithField("current_state", taskState.State.String()).
		WithField("goal_state", taskGoalState.State.String()).
		WithField("current_config_version", taskState.ConfigVersion).
		WithField("desired_config_version", taskGoalState.ConfigVersion).
		WithField("current_pod_run_id", taskState.MesosTaskID.GetValue()).
		WithField("desired_pod_run_id", taskGoalState.MesosTaskID.GetValue()).
		WithField("task_action", actionStr).
		Info("running task action")

	if action != nil {
		actions = append(actions, goalstate.Action{
			Name:    string(actionStr),
			Execute: action,
		})
	}

	return ctx, cancel, actions
}

// suggestTaskAction provides the task action for a given state and goal state
func (t *taskEntity) suggestTaskAction(
	currentState cached.TaskStateVector,
	goalState cached.TaskStateVector) TaskAction {
	if requireUpdate(currentState, goalState) ||
		requireRestart(currentState, goalState) {
		switch {
		case util.IsPelotonStateTerminal(currentState.State):
			return InitializeAction

		default:
			return StopAction
		}
	}

	// At this point the task has the correct version.
	// Find action to reach goal state from current state.
	if tr, ok := _isoVersionsTaskRules[goalState.State]; ok {
		if a, ok := tr[currentState.State]; ok {
			return a
		}
	}

	return NoTaskAction
}

// check if the current configuration version of a task is the same
// as the desired configuration version. If it is not, then update
// workflow for the task needs to be triggered. The update workflow
// needs to be run for tasks which do not have non-terminal
// goal states to avoid trying to update a batch task or a task
// which is going to be killed anyways.
func requireUpdate(currentState cached.TaskStateVector,
	goalState cached.TaskStateVector) bool {
	return currentState.ConfigVersion != goalState.ConfigVersion &&
		!util.IsPelotonStateTerminal(goalState.State)
}

// Then check if current state and goal state runID are different.
// if goalState runID is zero, it means it is an old task without
// expected runID set.
// TODO: remove goalState.MesosTaskID check after all tasks
// have desired mesos task id
func requireRestart(currentState cached.TaskStateVector,
	goalState cached.TaskStateVector) bool {
	return currentState.MesosTaskID.GetValue() !=
		goalState.MesosTaskID.GetValue() &&
		goalState.MesosTaskID.GetValue() != "" &&
		goalState.State != task.TaskState_KILLED &&
		goalState.State != task.TaskState_DELETED
}
