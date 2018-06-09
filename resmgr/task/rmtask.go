package task

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/eventstream"
	state "code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// RunTimeStats is the container for run time stats of the res mgr task
type RunTimeStats struct {
	StartTime time.Time
}

// RMTask is the wrapper around resmgr.task for state machine
type RMTask struct {
	sync.Mutex // Mutex for synchronization

	task                *resmgr.Task         // resmgr task
	stateMachine        state.StateMachine   // state machine for the task
	respool             respool.ResPool      // ResPool in which this tasks belongs to
	statusUpdateHandler *eventstream.Handler // Event handler for updates
	runTimeStats        *RunTimeStats        // run time stats for resmgr task
	config              *Config              // resmgr config object
	policy              Policy               // placement retry backoff policy
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
	if err != nil {
		return nil, err
	}

	// As this is when task is being created , retry should be 0
	r.Task().PlacementRetryCount = 0
	// Placement timeout should be equal to placing timeout by default
	r.Task().PlacementTimeoutSeconds = config.PlacingTimeout.Seconds()
	// Checking if placement backoff is enabled
	if !config.EnablePlacementBackoff {
		return &r, nil
	}

	// Creating the backoff policy specified in config
	// and will be used for further backoff calculations.
	r.policy, err = GetFactory().CreateBackOffPolicy(config)
	if err != nil {
		return nil, err
	}
	return &r, nil
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
						state.State(task.TaskState_LAUNCHED.String()),
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
					From: state.State(task.TaskState_LAUNCHED.String()),
					To: []state.State{
						state.State(task.TaskState_RUNNING.String()),
						// The launch of the task may time out in job manager,
						// which will then regenerate the mesos task id, and then
						// enqueue the task again into resource manager. Since, the
						// task has already passed admission control, it will be
						// moved to the READY state.
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
					From: state.State(task.TaskState_PLACING.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
						state.State(task.TaskState_PENDING.String()),
					},
					Timeout:     rmTask.config.PlacingTimeout,
					Callback:    rmTask.timeoutCallbackFromPlacing,
					PreCallback: rmTask.preTimeoutCallback,
				}).
			AddTimeoutRule(
				&state.TimeoutRule{
					From: state.State(task.TaskState_LAUNCHING.String()),
					To: []state.State{
						state.State(task.TaskState_READY.String()),
					},
					Timeout:  rmTask.config.LaunchingTimeout,
					Callback: rmTask.timeoutCallbackFromLaunching,
				}).
			Build()
	if err != nil {
		log.WithField("task", rmTask.task.GetTaskId().Value).Error(err)
		return nil, err
	}
	return stateMachine, nil
}

// TransitTo transitions to the target state
func (rmTask *RMTask) TransitTo(stateTo string, options ...state.Option) error {
	err := rmTask.stateMachine.TransitTo(state.State(stateTo), options...)
	if err == nil {
		GetTracker().UpdateCounters(rmTask.GetCurrentState(),
			task.TaskState(task.TaskState_value[stateTo]))
	}
	return err
}

// transitionCallBack is the global callback for the resource manager task
func (rmTask *RMTask) transitionCallBack(t *state.Transition) error {
	// Sending State change event to Ready
	rmTask.updateStatus(string(t.To))
	return nil
}

// timeoutCallback is the callback for the resource manager task
// which moving after timeout from placing/launching state to ready state
func (rmTask *RMTask) timeoutCallbackFromPlacing(t *state.Transition) error {
	pTaskID := &peloton.TaskID{Value: t.StateMachine.GetName()}
	rmtask := GetTracker().GetTask(pTaskID)
	if rmtask == nil {
		return errors.Errorf("Task is not present in statemachine "+
			"tracker %s ", t.StateMachine.GetName())
	}

	if t.To == state.State(task.TaskState_PENDING.String()) {
		log.WithFields(log.Fields{
			"task_id":    pTaskID.Value,
			"from_state": t.From,
			"to_state":   t.To,
		}).Info("task is pushed back to pending queue")
		// we need to push it if pending
		err := rmtask.PushTaskForReadmission()
		if err != nil {
			return err
		}
		return nil
	}
	log.WithFields(log.Fields{
		"task_id":    pTaskID.Value,
		"from_state": t.From,
		"to_state":   t.To,
	}).Info("task is pushed back to ready queue")
	err := rmtask.PushTaskForPlacementAgain()
	if err != nil {
		return err
	}
	log.WithField("Task", rmtask).Debug("Enqueue again due to timeout")
	return nil
}

func (rmTask *RMTask) timeoutCallbackFromLaunching(t *state.Transition) error {
	pTaskID := &peloton.TaskID{Value: t.StateMachine.GetName()}
	rmtask := GetTracker().GetTask(pTaskID)
	if rmtask == nil {
		return errors.Errorf("Task is not present in statemachine "+
			"tracker %s ", t.StateMachine.GetName())
	}

	err := rmtask.PushTaskForPlacementAgain()
	if err != nil {
		return err
	}
	log.WithField("Task", rmtask).Debug("Enqueue again due to timeout")
	return nil
}

func (rmTask *RMTask) preTimeoutCallback(t *state.Transition) error {
	pTaskID := &peloton.TaskID{Value: t.StateMachine.GetName()}
	rmtask := GetTracker().GetTask(pTaskID)
	if rmtask == nil {
		return errors.Errorf("Task is not present in statemachine "+
			"tracker %s ", t.StateMachine.GetName())
	}

	if rmtask.IsFailedEnoughPlacement() {
		t.To = state.State(task.TaskState_PENDING.String())
		return nil
	}
	t.To = state.State(task.TaskState_READY.String())
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

// StateMachine returns the state machine of the RMTask.
func (rmTask *RMTask) StateMachine() state.StateMachine {
	return rmTask.stateMachine
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

// AddBackoff adds the backoff to the RMtask based on backoff policy
func (rmTask *RMTask) AddBackoff() error {
	rmTask.Lock()
	defer rmTask.Unlock()
	// Check if policy is nil then we should return back
	if rmTask.policy == nil {
		return errors.Errorf("backoff policy is disabled %s", rmTask.Task().Id.GetValue())
	}
	// Adding the placement timeout values based on policy
	rmTask.Task().PlacementTimeoutSeconds = rmTask.config.PlacingTimeout.Seconds() +
		rmTask.policy.GetNextBackoffDuration(rmTask.Task(), rmTask.config)
	// Adding the placement retry count based on backoff policy
	rmTask.Task().PlacementRetryCount++

	// If there is no Timeout rule for PLACING state we should error out
	if _, ok := rmTask.stateMachine.GetTimeOutRules()[state.State(task.TaskState_PLACING.String())]; !ok {
		return errors.Errorf("could not add backoff to task %s", rmTask.Task().Id.GetValue())
	}
	// Updating the timeout rule by that next timer will start with the new time out value.
	rule := rmTask.stateMachine.GetTimeOutRules()[state.State(task.TaskState_PLACING.String())]
	rule.Timeout = time.Duration(rmTask.Task().PlacementTimeoutSeconds) * time.Second
	log.WithFields(log.Fields{
		"task_id":           rmTask.Task().Id.Value,
		"retry_count":       rmTask.Task().PlacementRetryCount,
		"placement_timeout": rmTask.Task().PlacementTimeoutSeconds,
	}).Info("Adding backoff to task")
	return nil
}

// IsFailedEnoughPlacement returns true if one placement cylce is completed
// otherwise false
func (rmTask *RMTask) IsFailedEnoughPlacement() bool {
	rmTask.Lock()
	defer rmTask.Unlock()
	// Checking if placement backoff is enabled
	if !rmTask.config.EnablePlacementBackoff {
		return false
	}
	return rmTask.policy.IsCycleCompleted(rmTask.Task(), rmTask.config)
}

// PushTaskForReadmission pushes the task for readmission to pending queue
func (rmTask *RMTask) PushTaskForReadmission() error {
	var tasks []*resmgr.Task
	gang := &resmgrsvc.Gang{
		Tasks: append(tasks, rmTask.task),
	}
	// pushing it to pending queue
	err := rmTask.Respool().EnqueueGang(gang)
	if err != nil {
		log.WithError(err).WithField("task", rmTask.task.Id.Value).Info("could not be enqueued")
		return err
	}
	err = rmTask.Respool().SubtractFromAllocation(scalar.GetGangAllocation(gang))
	if err != nil {
		log.WithError(err).WithField(
			"Gang", gang).
			Error("Not able to remove allocation " +
				"from respool")
		return err
	}
	err = rmTask.Respool().AddToDemand(scalar.GetGangResources(gang))
	if err != nil {
		log.WithError(err).WithField(
			"Gang", gang).
			Error("Not able to add to demand " +
				"for respool")
		return err
	}
	return nil
}

// PushTaskForPlacementAgain pushes the task to ready queue as the placement cycle is not
// completed for this task.
func (rmTask *RMTask) PushTaskForPlacementAgain() error {
	var tasks []*resmgr.Task
	gang := &resmgrsvc.Gang{
		Tasks: append(tasks, rmTask.task),
	}
	err := GetScheduler().EnqueueGang(gang)
	if err != nil {
		log.WithField("Gang", gang).Error("Could not enqueue " +
			"gang to ready after timeout")
		return err
	}
	return nil
}
