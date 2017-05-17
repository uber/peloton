package task

import (
	"time"

	log "github.com/Sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/common/eventstream"
	state "code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"
)

// RMTask is the wrapper around resmgr.task for state machine
type RMTask struct {
	task                *resmgr.Task
	stateMachine        state.StateMachine
	respool             respool.ResPool
	statusUpdateHandler *eventstream.Handler
}

// CreateRMTask creates the RM task from resmgr.task
func CreateRMTask(
	t *resmgr.Task,
	handler *eventstream.Handler,
	respool respool.ResPool) (*RMTask, error) {
	r := RMTask{
		task:                t,
		statusUpdateHandler: handler,
		respool:             respool,
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
					From:     state.State(task.TaskState_INITIALIZED.String()),
					To:       []state.State{state.State(task.TaskState_PENDING.String())},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_PENDING.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
						state.State(task.TaskState_KILLED.String()),
					},
					Callback: nil,
				}).
			AddRule(
				&state.Rule{
					From: state.State(task.TaskState_READY.String()),
					To: []state.State{
						state.State(task.TaskState_PLACING.String()),
						state.State(task.TaskState_KILLED.String()),
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
			Build()
	if err != nil {
		log.WithField("task", rmTask.task.GetTaskId().Value).Error(err)
		return nil, err
	}
	return stateMachine, nil
}

// TransitTo transitions to the target state
func (rmTask *RMTask) TransitTo(stateTo string, args ...interface{}) error {
	return rmTask.stateMachine.TransitTo(state.State(stateTo), args)
}

// transitionCallBack is the global callback for the resource manager task
func (rmTask *RMTask) transitionCallBack(t *state.Transition) error {
	// Sending State change event to Ready
	rmTask.updateStatus(string(t.To))
	return nil
}

// updateStatus creates and send the task event to event stream
func (rmTask *RMTask) updateStatus(status string) {
	t := time.Now()
	// Create Peloton task event
	taskEvent := &task.TaskEvent{
		Source:    task.TaskEvent_SOURCE_RESMGR,
		State:     task.TaskState(task.TaskState_value[status]),
		TaskId:    rmTask.task.Id,
		Timestamp: t.Format(time.RFC3339),
	}

	event := &pb_eventstream.Event{
		PelotonTaskEvent: taskEvent,
		Type:             pb_eventstream.Event_PELOTON_TASK_EVENT,
	}

	err := rmTask.statusUpdateHandler.AddEvent(event)
	if err != nil {
		log.WithError(err).WithField("Event", event).
			Error("Cannot add status update")
	}
}
