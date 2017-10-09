package task

import (
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/eventstream"
	state "code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// RunTimeStats is the container for run time stats of the res mgr task
type RunTimeStats struct {
	StartTime time.Time
}

// RMTask is the wrapper around resmgr.task for state machine
type RMTask struct {
	task                *resmgr.Task
	stateMachine        state.StateMachine
	respool             respool.ResPool
	statusUpdateHandler *eventstream.Handler
	runTimeStats        *RunTimeStats
	config              *Config
}

// CreateRMTask creates the RM task from resmgr.task
func CreateRMTask(
	t *resmgr.Task,
	handler *eventstream.Handler,
	respool respool.ResPool,
	config *Config) (*RMTask, error) {
	r := RMTask{
		task:                t,
		statusUpdateHandler: handler,
		respool:             respool,
		config:              config,
		runTimeStats: &RunTimeStats{
			StartTime: time.Time{},
		},
	}
	var err error
	r.stateMachine, err = r.createStateMachine()
	return &r, err
}

// createStateMachine creates the state machine
func (rmTask *RMTask) createStateMachine() (state.StateMachine, error) {

	stateMachine, err :=
		state.NewBuilder().
			WithName(rmTask.task.Id.Value).
			WithCurrentState(state.State(task.TaskState_INITIALIZED.String())).
			WithTransitionCallback(rmTask.transitionCallBack).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_INITIALIZED.String()),
					To: []state.State{
						state.State(task.TaskState_PENDING.String()),
						// We need this transition when we want to place
						// running/launched task back to resmgr
						// as running/launching
						state.State(task.TaskState_RUNNING.String()),
						state.State(task.TaskState_LAUNCHING.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_PENDING.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
						state.State(task.TaskState_KILLED.String()),
						// It may happen that placement engine returns
						// just after resmgr recovery and task is still
						// in pending
						state.State(task.TaskState_PLACED.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_READY.String()),
					To: []state.State{
						state.State(task.TaskState_PLACING.String()),
						// It may happen that placement engine returns
						// just after resmgr timeout and task is still
						// in ready
						state.State(task.TaskState_PLACED.String()),
						state.State(task.TaskState_KILLED.String()),
						// This transition we need, to put back ready
						// state to pending state for in transitions
						// tasks which could not reach to ready queue
						state.State(task.TaskState_PENDING.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_PLACING.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
						state.State(task.TaskState_PLACED.String()),
						state.State(task.TaskState_KILLED.String()),
						// This transition is required when the task is
						// preempted while its being placed by the Placement
						// engine. If preempted it'll go back to PENDING
						// state and relinquish its resource allocation from
						// the resource pool.
						state.State(task.TaskState_PENDING.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_PLACED.String()),
					To: []state.State{
						state.State(task.TaskState_LAUNCHING.String()),
						state.State(task.TaskState_KILLED.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_LAUNCHING.String()),
					To: []state.State{
						state.State(task.TaskState_RUNNING.String()),
						state.State(task.TaskState_READY.String()),
						state.State(task.TaskState_KILLED.String()),
						state.State(task.TaskState_LAUNCHED.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_RUNNING.String()),
					To: []state.State{
						state.State(task.TaskState_SUCCEEDED.String()),
						state.State(task.TaskState_LOST.String()),
						state.State(task.TaskState_PREEMPTING.String()),
						state.State(task.TaskState_KILLING.String()),
						state.State(task.TaskState_FAILED.String()),
						state.State(task.TaskState_KILLED.String()),
						state.State(task.TaskState_READY.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_FAILED.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_KILLED.String()),
					To: []state.State{
						state.State(task.TaskState_PENDING.String()),
					},
					Callback: nil,
				}).
			AddTimeoutRule(
				&state.TimeoutRule{
					From:     state.State(task.TaskState_PLACING.String()),
					To:       state.State(task.TaskState_READY.String()),
					Timeout:  rmTask.config.PlacingTimeout,
					Callback: rmTask.placingToReadyCallBack,
				}).
			AddTimeoutRule(
				&state.TimeoutRule{
					From:     state.State(task.TaskState_LAUNCHING.String()),
					To:       state.State(task.TaskState_READY.String()),
					Timeout:  rmTask.config.LaunchingTimeout,
					Callback: rmTask.placingToReadyCallBack,
				}).
			Build()
	if err != nil {
		log.WithField("task", rmTask.task.GetTaskId().Value).Error(err)
		return nil, err
	}
	return stateMachine, nil
}

// TransitTo transitions to the target state
func (rmTask *RMTask) TransitTo(stateTo string, args ...interface{}) error {
	err := rmTask.stateMachine.TransitTo(state.State(stateTo), args)
	if err == nil {
		GetTracker().UpdateCounters(rmTask.GetCurrentState().String(), stateTo)
	}
	return err
}

// transitionCallBack is the global callback for the resource manager task
func (rmTask *RMTask) transitionCallBack(t *state.Transition) error {
	// Sending State change event to Ready
	rmTask.updateStatus(string(t.To))
	return nil
}

// placingToReadyCallBack is the callback for the resource manager task
// which moving after timeout from placing state to ready state
func (rmTask *RMTask) placingToReadyCallBack(t *state.Transition) error {
	pTaskID := &peloton.TaskID{Value: t.StateMachine.GetName()}
	task := GetTracker().GetTask(pTaskID)
	if task == nil {
		return errors.Errorf("Task is not present in statemachine "+
			"tracker %s ", t.StateMachine.GetName())
	}
	var tasks []*resmgr.Task
	gang := &resmgrsvc.Gang{
		Tasks: append(tasks, task.task),
	}
	err := GetScheduler().EnqueueGang(gang)
	if err != nil {
		log.WithField("Gang", gang).Error("Could not enqueue " +
			"gang to ready after timeout")
		return err
	}
	log.WithField("Gang", gang).Debug("Enqueue again due to timeout")
	return nil
}

// updateStatus creates and send the task event to event stream
func (rmTask *RMTask) updateStatus(status string) {
	// TODO : Commenting it for now to not publish yet
	// Until we have Solution for event race
	// T936171

	//t := time.Now()
	//// Create Peloton task event
	//taskEvent := &task.TaskEvent{
	//	Source:    task.TaskEvent_SOURCE_RESMGR,
	//	State:     task.TaskState(task.TaskState_value[status]),
	//	TaskId:    rmTask.task.Id,
	//	Timestamp: t.Format(time.RFC3339),
	//}
	//
	//event := &pb_eventstream.Event{
	//	PelotonTaskEvent: taskEvent,
	//	Type:             pb_eventstream.Event_PELOTON_TASK_EVENT,
	//}
	//
	//err := rmTask.statusUpdateHandler.AddEvent(event)
	//if err != nil {
	//	log.WithError(err).WithField("Event", event).
	//		Error("Cannot add status update")
	//}
}

// Task returns the task of the RMTask.
func (rmTask *RMTask) Task() *resmgr.Task {
	return rmTask.task
}

// GetCurrentState returns the current state
func (rmTask *RMTask) GetCurrentState() task.TaskState {
	return task.TaskState(
		task.TaskState_value[string(
			rmTask.stateMachine.GetCurrentState())])
}

// Respool returns the respool of the RMTask.
func (rmTask *RMTask) Respool() respool.ResPool {
	return rmTask.respool
}

// RunTimeStats returns the runtime stats of the RMTask
func (rmTask *RMTask) RunTimeStats() *RunTimeStats {
	return rmTask.runTimeStats
}

// UpdateStartTime updates the start time of the RMTask
func (rmTask *RMTask) UpdateStartTime(startTime time.Time) {
	rmTask.runTimeStats.StartTime = startTime
}
