package tracked

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// UnknownVersion is used by the goalstate engine, when either the current
	// or desired config version is unknown.
	UnknownVersion = math.MaxUint64
)

// Task tracked by the system, serving as a best effort view of what's stored
// in the database.
type Task interface {
	// ID of the task.
	ID() uint32

	// Job the task belongs to.
	Job() Job

	// CurrentState of the task.
	CurrentState() State

	// GoalState of the task.
	GoalState() State

	// LastAction performed by the task, as well as when it was performed.
	LastAction() (TaskAction, time.Time)

	// RunAction on the task. Returns a bool representing if the task should
	// be rescheduled by the goalstate engine and an error representing the
	// result of the action.
	RunAction(ctx context.Context, action TaskAction) (bool, error)

	// GetRunTime returns the task run time
	GetRunTime() *pb_task.RuntimeInfo
}

// State of a job. This can encapsulate either the actual state or the goal
// state.
type State struct {
	State         pb_task.TaskState
	ConfigVersion uint64
}

// TaskAction that can be given to the Task.RunAction method.
type TaskAction string

// Actions available to be performed on the task.
const (
	NoAction             TaskAction = "no_action"
	UntrackAction        TaskAction = "untrack"
	StartAction          TaskAction = "start_task"
	StopAction           TaskAction = "stop_task"
	PreemptAction        TaskAction = "preempt_action"
	InitializeAction     TaskAction = "initialize_task"
	UseGoalVersionAction TaskAction = "use_goal_state"
)

func newTask(job *job, id uint32) *task {
	task := &task{
		queueItemMixin: newQueueItemMixing(),
		job:            job,
		id:             id,
	}

	return task
}

// task is the wrapper around task info for state machine
type task struct {
	sync.RWMutex
	queueItemMixin

	job *job
	id  uint32

	runtime *pb_task.RuntimeInfo

	// goalState along with the time the goal state was updated.
	stateTime     time.Time
	goalStateTime time.Time

	// lastState set, the resulting action and when that action was last tried.
	lastAction     TaskAction
	lastActionTime time.Time
}

func (t *task) ID() uint32 {
	return t.id
}

func (t *task) Job() Job {
	return t.job
}

func (t *task) CurrentState() State {
	t.RLock()
	defer t.RUnlock()

	return State{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
	}
}

func (t *task) GoalState() State {
	t.RLock()
	defer t.RUnlock()

	return State{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
	}
}

func (t *task) LastAction() (TaskAction, time.Time) {
	t.RLock()
	defer t.RUnlock()

	return t.lastAction, t.lastActionTime
}

func (t *task) RunAction(ctx context.Context, action TaskAction) (bool, error) {
	defer t.job.m.mtx.scope.Tagged(map[string]string{"action": string(action)}).Timer("run_duration").Start().Stop()

	// TODO: Move to Manager, such that the following holds:
	// Take job lock only while we evaluate action. That ensure we have a
	// consistent view across the entire job, while we decide if we can apply
	// the action.

	t.Lock()
	t.lastAction = action
	t.lastActionTime = time.Now()
	t.Unlock()

	log.WithField("action", action).
		WithField("current_state", t.CurrentState().State.String()).
		WithField("current_config", t.CurrentState().ConfigVersion).
		WithField("goal_state", t.GoalState().State.String()).
		WithField("goal_version", t.GoalState().ConfigVersion).
		WithField("job_id", t.job.id.GetValue()).
		WithField("instance_id", t.id).
		Info("running action for task")

	var err error
	reschedule := true

	switch action {
	case NoAction:
		reschedule = false

	case UntrackAction:
		reschedule = false
		t.job.m.clearTask(t)

	case StartAction:
		err = t.start(ctx)

	case StopAction:
		err = t.stop(ctx)

	case InitializeAction:
		err = t.initialize(ctx)

	case PreemptAction:
		action, err := t.getPostPreemptAction(ctx)
		if err != nil {
			err = errors.Wrapf(err, "unable to get post preemption action")
			break
		}
		log.WithField("job_id", t.job.id.GetValue()).
			WithField("instance_id", t.id).
			WithField("action", action).
			Info("running preemption action")
		return t.RunAction(ctx, action)

	default:
		err = fmt.Errorf("no command configured for running task action `%v`", action)
	}

	return reschedule, err
}

// returns the action to be performed after preemption based on the task
// preemption policy
func (t *task) getPostPreemptAction(ctx context.Context) (TaskAction, error) {
	// Here we check what the task preemption policy is,
	// if killOnPreempt is set to true then we don't reschedule the task
	// after it is preempted
	var action TaskAction
	pp, err := t.getTaskPreemptionPolicy(ctx, t.job.id, t.id,
		t.GoalState().ConfigVersion)
	if err != nil {
		err = errors.Wrapf(err, "unable to get task preemption policy")
		return action, err
	}
	if pp != nil && pp.GetKillOnPreempt() {
		// We are done , we don't want to reschedule it
		return UntrackAction, nil
	}
	return InitializeAction, nil
}

// getTaskPreemptionPolicy returns the restart policy of the task,
// used when the task is preempted
func (t *task) getTaskPreemptionPolicy(ctx context.Context, jobID *peloton.JobID,
	instanceID uint32, configVersion uint64) (*pb_task.PreemptionPolicy,
	error) {
	config, err := t.job.m.taskStore.GetTaskConfig(ctx, jobID, instanceID,
		int64(configVersion))
	if err != nil {
		return nil, err
	}
	return config.GetPreemptionPolicy(), nil
}

func (t *task) updateRuntime(runtime *pb_task.RuntimeInfo) {
	t.Lock()
	defer t.Unlock()

	// TODO: Reject update if older than current revision.
	t.runtime = runtime

	now := time.Now()
	t.goalStateTime = now
	t.stateTime = now
}

// GetRunTime returns task run time
func (t *task) GetRunTime() *pb_task.RuntimeInfo {
	t.RLock()
	defer t.RUnlock()
	return t.runtime
}
