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

package task

import (
	"strings"
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/eventstream"
	state "github.com/uber/peloton/pkg/common/statemachine"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

var (
	errTaskNotPresent           = errors.New("task is not present in the tracker")
	errUnplacedTaskInWrongState = errors.New("unplaced task should be in state placing")
	errTaskNotInCorrectState    = errors.New("task is not present in correct state")
	errTaskNotTransitioned      = errors.New("task is not transitioned to state")
)

var (
	reasonPlacementFailed = "Reached placement failure backoff threshold"
	reasonPlacementRetry  = "Previous placement failed"
)

// RunTimeStats is the container for run time stats of the resmgr task
type RunTimeStats struct {
	StartTime time.Time
}

// RMTaskState represents the state of the rm task
type RMTaskState struct {
	// The state the task is in
	State task.TaskState
	// The reason for being in the state
	Reason string
	// Last time the state was updated
	LastUpdateTime time.Time
}

// RMTask is the wrapper around resmgr.task for state machine
type RMTask struct {
	mu sync.Mutex // Mutex for synchronization

	task         *resmgr.Task       // resmgr task
	stateMachine state.StateMachine // state machine for the task

	respool             respool.ResPool      // ResPool in which this tasks belongs to
	statusUpdateHandler *eventstream.Handler // Event handler for updates

	config *Config // resmgr config object
	policy Policy  // placement retry backoff policy

	runTimeStats *RunTimeStats // run time stats for resmgr task

	// observes the state transitions of the rm task
	transitionObserver TransitionObserver
}

// CreateRMTask creates the RM task from resmgr.task
func CreateRMTask(
	scope tally.Scope,
	t *resmgr.Task,
	handler *eventstream.Handler,
	respool respool.ResPool,
	taskConfig *Config) (*RMTask, error) {

	r := &RMTask{
		task:                t,
		statusUpdateHandler: handler,
		respool:             respool,
		config:              taskConfig,
		runTimeStats: &RunTimeStats{
			StartTime: time.Time{},
		},
		transitionObserver: NewTransitionObserver(
			taskConfig.EnableSLATracking,
			scope,
			respool.GetPath(),
		),
	}

	err := r.initStateMachine()
	if err != nil {
		return nil, err
	}

	// Placement timeout should be equal to placing timeout by default
	r.Task().PlacementTimeoutSeconds = taskConfig.PlacingTimeout.Seconds()
	// Checking if placement backoff is enabled
	if !taskConfig.EnablePlacementBackoff {
		return r, nil
	}

	// Creating the backoff policy specified in taskConfig
	// and will be used for further backoff calculations.
	r.policy, err = GetFactory().CreateBackOffPolicy(taskConfig)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// initStateMachine initializes the resource manager task state machine
func (rmTask *RMTask) initStateMachine() error {

	stateMachine, err :=
		state.NewBuilder().
			WithName(rmTask.Task().GetTaskId().GetValue()).
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
						state.State(task.TaskState_RESERVED.String()),
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
					From: state.State(task.TaskState_RESERVED.String()),
					To: []state.State{
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
			AddTimeoutRule(
				&state.TimeoutRule{
					From: state.State(task.TaskState_RESERVED.String()),
					To: []state.State{
						state.State(task.TaskState_PENDING.String()),
					},
					Timeout:  rmTask.config.ReservingTimeout,
					Callback: rmTask.timeoutCallbackFromReserving,
				}).
			AddTimeoutRule(
				// If the task is dropped by jobmgr then the rmtask should move
				// back to RUNNING state (on timeout) so that it is enqueued in the
				// preemption queue in the next preemption cycle.
				&state.TimeoutRule{
					From: state.State(task.TaskState_PREEMPTING.String()),
					To: []state.State{
						state.State(task.TaskState_RUNNING.String()),
					},
					Timeout:  rmTask.config.PreemptingTimeout,
					Callback: nil,
				}).
			Build()
	if err != nil {
		return err
	}

	rmTask.stateMachine = stateMachine
	return nil
}

// TransitFromTo transits a task from a source to target state.
// If the from state doesn't match the current state and error is returned.
func (rmTask *RMTask) TransitFromTo(
	stateFrom string,
	stateTo string,
	options ...state.Option) error {

	rmTask.mu.Lock()
	defer rmTask.mu.Unlock()

	if rmTask.getCurrentState().State.String() != stateFrom {
		return errTaskNotInCorrectState
	}

	if err := rmTask.TransitTo(
		stateTo,
		options...); err != nil {
		return errTaskNotTransitioned
	}

	return nil
}

// TransitTo transitions to the target state
func (rmTask *RMTask) TransitTo(stateTo string, options ...state.Option) error {
	fromState := rmTask.getCurrentState().State

	err := rmTask.stateMachine.TransitTo(state.State(stateTo), options...)
	if err != nil {
		if err == state.ErrNoOpTransition {
			// the task is already in the desired state
			return nil
		}
		return errors.Wrap(err, "failed to transition rmtask")
	}

	GetTracker().UpdateMetrics(
		fromState,
		task.TaskState(task.TaskState_value[stateTo]),
		scalar.ConvertToResmgrResource(rmTask.task.GetResource()),
	)
	return nil
}

// Terminate the rm task
func (rmTask *RMTask) Terminate() {
	rmTask.mu.Lock()
	defer rmTask.mu.Unlock()
	rmTask.stateMachine.Terminate()
}

// Task returns the task of the RMTask.
func (rmTask *RMTask) Task() *resmgr.Task {
	return rmTask.task
}

// GetCurrentState returns the current state
func (rmTask *RMTask) GetCurrentState() RMTaskState {
	rmTask.mu.Lock()
	defer rmTask.mu.Unlock()
	return rmTask.getCurrentState()
}

func (rmTask *RMTask) getCurrentState() RMTaskState {
	return RMTaskState{
		State: task.TaskState(
			task.TaskState_value[string(
				rmTask.stateMachine.GetCurrentState())]),
		Reason:         rmTask.stateMachine.GetReason(),
		LastUpdateTime: rmTask.stateMachine.GetLastUpdateTime(),
	}
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
	rmTask.mu.Lock()
	defer rmTask.mu.Unlock()

	// Check if policy is nil then we should return back
	if rmTask.policy == nil {
		return errors.Errorf("backoff policy is disabled %s", rmTask.Task().Id.GetValue())
	}

	// Adding the placement timeout values based on policy
	rmTask.Task().PlacementTimeoutSeconds = rmTask.config.PlacingTimeout.Seconds() +
		rmTask.policy.GetNextBackoffDuration(rmTask.Task(), rmTask.config)

	// Adding the placement attempt/retry count based on backoff policy
	if rmTask.Task().PlacementAttemptCount < rmTask.config.PlacementAttemptsPerCycle {
		rmTask.Task().PlacementAttemptCount++
	} else {
		rmTask.Task().PlacementAttemptCount = 1
		rmTask.Task().PlacementRetryCount++
	}

	// If there is no Timeout rule for PLACING state we should error out
	rule, ok := rmTask.stateMachine.GetTimeOutRules()[state.State(task.TaskState_PLACING.String())]
	if !ok {
		return errors.Errorf("could not add backoff to task %s", rmTask.Task().Id.GetValue())
	}

	// Updating the timeout rule by that next timer will start with the new time out value.
	rule.Timeout = time.Duration(rmTask.Task().PlacementTimeoutSeconds) * time.Second
	log.WithFields(log.Fields{
		"task_id":           rmTask.Task().Id.Value,
		"retry_count":       rmTask.Task().PlacementRetryCount,
		"attempt_count":     rmTask.Task().PlacementAttemptCount,
		"placement_timeout": rmTask.Task().PlacementTimeoutSeconds,
	}).Info("Adding backoff to task")
	return nil
}

// RequeueUnPlaced Requeues the task which couldn't be placed.
func (rmTask *RMTask) RequeueUnPlaced(reason string) error {
	rmTask.mu.Lock()
	defer rmTask.mu.Unlock()

	cState := rmTask.getCurrentState().State

	// If the task is in READY/PENDING state we don't need to do anything.
	// This can happen if the state machine recovered from PLACING state
	if cState == task.TaskState_READY || cState == task.TaskState_PENDING {
		return nil
	}

	// If task is not in PLACING state, it should return error
	if cState != task.TaskState_PLACING {
		return errUnplacedTaskInWrongState
	}

	// If task is in PLACING state we need to determine which STATE it will
	// transition to based on retry attempts

	if rmTask.hasFinishedPlacementCycle() {
		// If this task is been failed enough times
		// put this task to PENDING queue.
		return rmTask.requeueToPendingQueue(reason)
	}

	// requeue to ready queue
	return rmTask.requeueToReadyQueue(reason)
}

// requeques a placing task to ready queue
// NB: Acquire lock on rm task before calling
func (rmTask *RMTask) requeueToReadyQueue(reason string) error {
	// move from PLACING to READY with the reason
	if err := rmTask.TransitTo(task.TaskState_READY.String(),
		state.WithReason(strings.Join(
			[]string{
				reasonPlacementRetry, reason,
			}, ":"))); err != nil {
		return err
	}

	// Enqueue back to ready Queue
	if err := rmTask.pushTaskForPlacementAgain(); err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"task_id":    rmTask.Task().Id.Value,
		"from_state": task.TaskState_PLACING.String(),
		"to_state":   task.TaskState_READY.String(),
	}).Info("Task moved back to ready queue")
	return nil
}

// requeques a placing task to pending queue
// NB: Acquire lock on rm task before calling
func (rmTask *RMTask) requeueToPendingQueue(reason string) error {
	// Transitioning task state to PENDING with the reason
	if err := rmTask.TransitTo(
		task.TaskState_PENDING.String(),
		state.WithReason(strings.Join(
			[]string{
				reasonPlacementFailed, reason,
			}, ":"))); err != nil {
		return err
	}
	// Pushing task to PENDING queue
	if err := rmTask.pushTaskForReadmission(); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"task_id":    rmTask.Task().Id.Value,
		"from_state": task.TaskState_PLACING.String(),
		"to_state":   task.TaskState_PENDING.String(),
	}).Info("Task is pushed back to pending queue from placement engine requeue")
	return nil
}

// hasFinishedPlacementCycle returns true if one placement cycle is completed
// otherwise false
// NB: Acquire lock before calling
func (rmTask *RMTask) hasFinishedPlacementCycle() bool {
	// Checking if placement backoff is enabled
	if !rmTask.config.EnablePlacementBackoff {
		return false
	}
	return rmTask.policy.IsCycleCompleted(rmTask.Task(), rmTask.config)
}

// Returns true if all the placement cycles are completed
// otherwise false. The task is ready for host reservation when
// all the placement cycles have been exhausted
// NB: Acquire lock before calling
func (rmTask *RMTask) hasFinishedAllPlacementCycles() bool {
	// Checking if placement backoff is enabled
	if !rmTask.config.EnablePlacementBackoff {
		return false
	}
	return rmTask.policy.allCyclesCompleted(rmTask.Task(), rmTask.config)
}

// pushTaskForReadmission pushes the pending task for readmission to pending
// queue
// NB: Acquire lock on rm task before calling
func (rmTask *RMTask) pushTaskForReadmission() error {
	var tasks []*resmgr.Task
	gang := &resmgrsvc.Gang{
		Tasks: append(tasks, rmTask.task),
	}

	// push to pending queue and add demand
	if err := rmTask.Respool().EnqueueGang(gang); err != nil {
		return errors.Wrapf(err, "failed to enqueue gang")
	}

	// remove allocation
	if err := rmTask.Respool().SubtractFromAllocation(
		scalar.GetGangAllocation(gang)); err != nil {
		return errors.Wrapf(err, "failed to remove allocation from respool")
	}

	return nil
}

// pushTaskForPlacementAgain pushes the task to ready queue as the
// placement cycle is not completed for this task or task is ready
// for host reservation
// NB: Acquire lock on rm task before calling
func (rmTask *RMTask) pushTaskForPlacementAgain() error {
	var tasks []*resmgr.Task
	gang := &resmgrsvc.Gang{
		Tasks: append(tasks, rmTask.task),
	}

	rmTask.task.Hostname = ""
	err := GetScheduler().EnqueueGang(gang)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue gang")
	}

	return nil
}

// transitionCallBack is the global callback for the resource manager task
func (rmTask *RMTask) transitionCallBack(t *state.Transition) error {
	if rmTask == nil {
		return errTaskNotPresent
	}

	tState := task.TaskState(task.TaskState_value[string(t.To)])

	rmTask.transitionObserver.Observe(
		rmTask.Task().GetTaskId().GetValue(),
		tState)

	// we only care about running state here
	if tState == task.TaskState_RUNNING {
		// update the start time
		rmTask.UpdateStartTime(time.Now().UTC())
	}
	return nil
}

// timeoutCallback is the callback for the resource manager task
// which moving after timeout from placing/launching state to ready state
func (rmTask *RMTask) timeoutCallbackFromPlacing(t *state.Transition) error {
	if rmTask == nil {
		return errTaskNotPresent
	}

	if t.To == state.State(task.TaskState_PENDING.String()) {
		log.WithFields(log.Fields{
			"task_id":    rmTask.Task().GetTaskId().Value,
			"from_state": t.From,
			"to_state":   t.To,
		}).Info("Task is pushed back to pending queue")
		// we need to push it if pending
		err := rmTask.pushTaskForReadmission()
		if err != nil {
			return err
		}
		return nil
	}

	// the task is ready for host reservation
	if rmTask.task.ReadyForHostReservation {
		log.WithFields(log.Fields{
			"task_id":    rmTask.Task().GetTaskId().GetValue(),
			"from_state": t.From,
			"to_state":   t.To,
		}).Info("Task is pushed back to ready queue for host reservation")

		// We need to push it to ready queue to get host reserved in placement
		err := rmTask.pushTaskForPlacementAgain()
		if err != nil {
			return err
		}
		return nil
	}

	log.WithFields(log.Fields{
		"task_id":    rmTask.Task().GetTaskId().GetValue(),
		"from_state": t.From,
		"to_state":   t.To,
	}).Info("Task is pushed back to ready queue")

	err := rmTask.pushTaskForPlacementAgain()
	if err != nil {
		return err
	}

	log.WithField("task_id", rmTask.Task().GetTaskId().GetValue()).
		Debug("Enqueue again due to timeout")
	return nil
}

func (rmTask *RMTask) resetHostReservation() {
	rmTask.task.PlacementRetryCount = 0
	rmTask.task.PlacementAttemptCount = 0
	rmTask.task.ReadyForHostReservation = false
}

// timeoutCallback is the callback for the resource manager task
// which moving after timeout from reserved state to pending state
func (rmTask *RMTask) timeoutCallbackFromReserving(t *state.Transition) error {
	if rmTask == nil {
		return errTaskNotPresent
	}

	rmTask.resetHostReservation()
	err := rmTask.pushTaskForReadmission()
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"task_id":    rmTask.Task().GetTaskId().Value,
		"from_state": t.From,
		"to_state":   t.To,
	}).Info("Task is pushed back to pending queue")

	return nil
}

func (rmTask *RMTask) timeoutCallbackFromLaunching(t *state.Transition) error {
	if rmTask == nil {
		return errTaskNotPresent
	}

	err := rmTask.pushTaskForPlacementAgain()
	if err != nil {
		return err
	}

	log.WithField("task_id", rmTask.Task().GetTaskId().GetValue()).
		Debug("Enqueue again due to timeout")
	return nil
}

func (rmTask *RMTask) preTimeoutCallback(t *state.Transition) error {
	if rmTask == nil {
		return errTaskNotPresent
	}

	if rmTask.config.EnableHostReservation && rmTask.hasFinishedAllPlacementCycles() {
		rmTask.task.ReadyForHostReservation = true
		t.To = state.State(task.TaskState_READY.String())
		return nil
	}

	if rmTask.hasFinishedPlacementCycle() {
		t.To = state.State(task.TaskState_PENDING.String())
		return nil
	}
	t.To = state.State(task.TaskState_READY.String())
	return nil
}

// TODO : Commenting it for now to not publish yet, Until we have solution for
// event race : T936171
// updateStatus creates and send the task event to event stream
//func (rmTask *RMTask) updateStatus(status string) {
//
//
//
//
//	//t := time.Now()
//	//// Create Peloton task event
//	//taskEvent := &task.TaskEvent{
//	//	Source:    task.TaskEvent_SOURCE_RESMGR,
//	//	State:     task.TaskState(task.TaskState_value[status]),
//	//	TaskId:    rmTask.task.Id,
//	//	Timestamp: t.Format(time.RFC3339),
//	//}
//	//
//	//event := &pb_eventstream.Event{
//	//	PelotonTaskEvent: taskEvent,
//	//	Type:             pb_eventstream.Event_PELOTON_TASK_EVENT,
//	//}
//	//
//	//err := rmTask.statusUpdateHandler.AddEvent(event)
//	//if err != nil {
//	//	log.WithError(err).WithField("Event", event).
//	//		Error("Cannot add status update")
//	//}
//}
