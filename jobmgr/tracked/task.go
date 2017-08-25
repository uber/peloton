package tracked

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
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

	// RunAction on the task.
	RunAction(ctx context.Context, action TaskAction) error
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
	StopAction           TaskAction = "stop_task"
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

func (t *task) RunAction(ctx context.Context, action TaskAction) error {
	t.Lock()
	t.lastAction = action
	t.lastActionTime = time.Now()
	t.Unlock()

	switch action {
	case NoAction:
		return nil

	case StopAction:
		return t.stop(ctx)

	default:
		return fmt.Errorf("no command configured for running task action `%v`", action)
	}
}

func (t *task) stop(ctx context.Context) error {
	t.Lock()
	runtime := t.runtime
	t.Unlock()

	if runtime == nil {
		return fmt.Errorf("tracked task has no runtime info assigned")
	}

	// TODO: Add when we can detect initialized.
	if false {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", t.job.ID(), t.ID()),
		}
		killReq := &resmgrsvc.KillTasksRequest{
			Tasks: []*peloton.TaskID{taskID},
		}

		// Calling resmgr Kill API
		killRes, err := t.job.m.resmgrClient.KillTasks(ctx, killReq)
		if err != nil {
			log.WithError(err).Error("Error in killing from resmgr")
		}

		if killRes.Error != nil {
			log.WithError(errors.New(killRes.GetError().Message)).
				Error("Error in killing from resmgr")
		}
	}

	req := &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{runtime.GetMesosTaskId()},
	}
	res, err := t.job.m.hostmgrClient.KillTasks(ctx, req)
	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.KillFailure != nil:
			return fmt.Errorf(e.KillFailure.Message)
		case e.InvalidTaskIDs != nil:
			return fmt.Errorf(e.InvalidTaskIDs.Message)
		default:
			return fmt.Errorf(e.String())
		}
	}

	return nil
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
