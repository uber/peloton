package goalstate

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/util"

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
	// PreemptAction preempts the task
	PreemptAction TaskAction = "preempt_action"
	// InitializeAction re-initializes the task and regenerates the mesos task id
	InitializeAction TaskAction = "initialize_task"
	// ReloadTaskRuntime reload the task runtime into cache
	ReloadTaskRuntime TaskAction = "reload_runtime"
	// LaunchRetryAction is run after task launch to either send to resource manager
	// that task has been successfully launched or re-initialize the task after lauch timeout
	LaunchRetryAction TaskAction = "launch_retry"
	// FailRetryAction retries a failed task
	FailRetryAction TaskAction = "fail_retry"
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
		PreemptAction:          TaskPreempt,
		InitializeAction:       TaskInitialize,
		ReloadTaskRuntime:      TaskReloadRuntime,
		LaunchRetryAction:      TaskLaunchRetry,
		FailRetryAction:        TaskFailRetry,
		ExecutorShutdownAction: TaskExecutorShutdown,
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
			task.TaskState_SUCCEEDED:   InitializeAction,
			task.TaskState_FAILED:      InitializeAction,
			task.TaskState_KILLED:      InitializeAction,
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     TaskStateInvalidAction,
			task.TaskState_LOST:        InitializeAction,
		},
		task.TaskState_SUCCEEDED: {
			task.TaskState_INITIALIZED: StartAction,
			task.TaskState_LAUNCHED:    LaunchRetryAction,
			task.TaskState_STARTING:    LaunchRetryAction,
			task.TaskState_FAILED:      FailRetryAction,
			task.TaskState_KILLED:      FailRetryAction,
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     TaskStateInvalidAction,
			task.TaskState_LOST:        InitializeAction,
		},
		task.TaskState_KILLED: {
			task.TaskState_INITIALIZED: StopAction,
			task.TaskState_PENDING:     StopAction,
			task.TaskState_LAUNCHED:    StopAction,
			task.TaskState_STARTING:    StopAction,
			task.TaskState_RUNNING:     StopAction,
			task.TaskState_LOST:        NoTaskAction,
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
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
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     TaskStateInvalidAction,
			task.TaskState_KILLED:      TaskStateInvalidAction,
		},
		task.TaskState_PREEMPTING: {
			task.TaskState_INITIALIZED: StopAction,
			task.TaskState_PENDING:     StopAction,
			task.TaskState_LAUNCHED:    StopAction,
			task.TaskState_STARTING:    StopAction,
			task.TaskState_RUNNING:     StopAction,
			task.TaskState_LOST:        PreemptAction,
			task.TaskState_PREEMPTING:  TaskStateInvalidAction,
			task.TaskState_KILLING:     ExecutorShutdownAction,
			task.TaskState_KILLED:      PreemptAction,
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
func (t *taskEntity) suggestTaskAction(currentState cached.TaskStateVector, goalState cached.TaskStateVector) TaskAction {
	// First test if the task is at the goal version. If not, we'll have to
	// trigger a stop and wait until the task is in a terminal state.
	if currentState.ConfigVersion != goalState.ConfigVersion {
		switch {
		case currentState.ConfigVersion == cached.UnknownVersion,
			goalState.ConfigVersion == cached.UnknownVersion:
			// Ignore versions if version is unknown.

		case util.IsPelotonStateTerminal(currentState.State):
			// TODO restart the task
			return NoTaskAction

		default:
			return StopAction
		}
	}

	// At this point the job has the correct version.
	// Find action to reach goal state from current state.
	if tr, ok := _isoVersionsTaskRules[goalState.State]; ok {
		if a, ok := tr[currentState.State]; ok {
			return a
		}
	}

	return NoTaskAction
}
