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

package statemachine

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	createStateReasonString = "task created"
)

var (
	// ErrNoOpTransition is returned when the transition is a no-op
	ErrNoOpTransition = errors.Errorf("no-op transition")
)

// Rule is struct to define the transition rules
// Rule is from one source state to multiple destination states
// This can define callback function from 1:1 basis from src->dest state
type Rule struct {
	// from is the source state
	From State
	// to is the destination state
	To []State
	// callback is transition function which defines 1:1 mapping
	// of callbacks
	Callback func(*Transition) error
}

// TimeoutRule is a struct to define the state transition which is
// triggered by the time duration. This is kind of timeout where
// state will automatically move to "to" state after the timeout
type TimeoutRule struct {
	// from is the source state
	From State
	// to is the list of destination states
	// actual destination state has to be determine
	// in precall back
	To []State
	// timeout for transition to "to" state
	Timeout time.Duration
	// callback is transition function which defines 1:1 mapping
	// of callbacks
	Callback func(*Transition) error
	// PreCallback is needed to determine about which state timeout
	// going to transition. If precallback is not present
	// Timeout should transition to first TO state.
	PreCallback func(*Transition) error
}

// Callback is the type for callback function
type Callback func(*Transition) error

// Option is the option passed to TransitTo to trigger state machine transition
type Option func(*statemachine)

// WithReason is an Option and is used to provide the state transition reason
func WithReason(reason string) Option {
	return func(sm *statemachine) {
		sm.reason = reason
	}
}

// WithInfo is an option passed in TransitTo to have store any meta infor about
// state machine
func WithInfo(key string, value string) Option {
	return func(sm *statemachine) {
		sm.metaInfo[key] = value
	}
}

// StateMachine is the interface wrapping around the statemachine Object
// Using to not expose full object
type StateMachine interface {

	// TransitTo function transits to desired state
	TransitTo(to State, options ...Option) error

	// GetCurrentState returns the current state of State Machine
	GetCurrentState() State

	// GetReason returns the reason for the last state transition
	GetReason() string

	// GetName returns the Name of the StateMachine object
	GetName() string

	// GetStateTimer returns the statetimer object
	GetStateTimer() StateTimer

	// GetLastUpdatedTime returns the last update time of the state machine
	GetLastUpdateTime() time.Time

	// Terminates the state machine
	Terminate()

	// GetMetaInfo returns the map of meta info about state machine
	GetMetaInfo() map[string]string

	// GetTimeOutRules returns the timeout rules defined for state machine
	// Its a map of [from state] to timeout rule
	GetTimeOutRules() map[State]*TimeoutRule
}

// statemachine is state machine, State Machine is responsible for moving states
// from source to destination and callback from source to destination
type statemachine struct {
	// To synchronize state machine operations
	sync.RWMutex

	// name of the object with which state machine is associated with.
	// This will be used by the clients to determine the call back for
	// the object on which callback is called
	name string

	// current is the current state of the object
	current State

	// map of rules to define the StateMachine Transitions
	// rules are defined as srcState-> [] destStates
	rules map[State]*Rule

	// global transition callback which applies to all state transitions
	// for an example want to notify state transitions
	transitionCallback func(*Transition) error

	// lastUpdatedTime records the time when last state is transitioned
	lastUpdatedTime time.Time

	// timeoutrules are the rules from transitioning from state which
	// can be timed out
	timeoutRules map[State]*TimeoutRule

	// timer is the object for statetimer
	timer StateTimer

	// reason records the reason for a state transition
	reason string

	// metaInfo will keep the metaInfo for the statemachine
	// it's a key value pair.
	metaInfo map[string]string
}

// NewStateMachine it will create the new state machine
// which clients can use to do tansitions on the object
func NewStateMachine(
	name string,
	current State,
	rules map[State]*Rule,
	timeoutRules map[State]*TimeoutRule,
	trasitionCallback Callback,
) (StateMachine, error) {

	sm := &statemachine{
		name:               name,
		current:            current,
		rules:              make(map[State]*Rule),
		timeoutRules:       timeoutRules,
		transitionCallback: trasitionCallback,
		lastUpdatedTime:    time.Now(),
		reason:             createStateReasonString,
		metaInfo:           make(map[string]string),
	}

	sm.timer = NewTimer(sm)

	err := sm.addRules(rules)
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *statemachine) GetStateTimer() StateTimer {
	return sm.timer
}

// addRules add the rules which defines the transitions
func (sm *statemachine) addRules(rules map[State]*Rule) error {
	for _, r := range rules {
		err := sm.validateRule(r)
		if err != nil {
			return err
		}
	}
	sm.rules = rules
	return nil
}

// validateRule validates the transitions
// All previous rules will be replaced by subsequent rules
func (sm *statemachine) validateRule(rule *Rule) error {
	sources := make(map[State]bool)
	for _, s := range rule.To {
		if _, ok := sources[s]; ok {
			return fmt.Errorf("invalid rule to be applied,"+
				" duplicate source %s", s)
		}
		sources[s] = true
	}
	return nil
}

// TransitTo is the function which clients will call to transition from one state to other
// this also calls the callbacks after the valid transition is done
func (sm *statemachine) TransitTo(to State, options ...Option) error {
	// Locking the statemachine to synchronize state changes
	sm.Lock()
	defer sm.Unlock()

	// checking if transition is allowed
	err := sm.isValidTransition(to)
	if err != nil {
		return err
	}

	//  Creating Transition to pass to callbacks
	t := &Transition{
		StateMachine: sm,
		From:         sm.current,
		To:           to,
		Params:       nil,
	}

	// Storing values for reverseTransition
	curState := sm.current

	// Try to stop state recovery if its transitioning
	// from timeout state
	if _, ok := sm.timeoutRules[curState]; ok {
		log.WithFields(log.Fields{
			"task_id":           sm.name,
			"rule_from ":        curState,
			"rule_to":           to,
			"meta_info_noindex": sm.GetMetaInfo(),
			"reason":            sm.reason,
		}).Info("Stopping state timeout recovery")
		sm.timer.Stop()
	}

	// Doing actual transition
	sm.current = to
	sm.lastUpdatedTime = time.Now()
	// Update options
	for _, option := range options {
		option(sm)
	}

	// invoking callback function
	if sm.rules[curState].Callback != nil {
		err = sm.rules[curState].Callback(t)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id":           sm.GetName(),
				"current_state ":    curState,
				"to_state":          to,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Error("callback failed for task")
			return err
		}
	}

	// Run the transition callback
	if sm.transitionCallback != nil {
		err = sm.transitionCallback(t)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id":           sm.GetName(),
				"current_state ":    curState,
				"to_state":          to,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Error("transition callback failed for task")
			return err
		}
	}
	// Checking if this STATE is timeout state
	if rule, ok := sm.timeoutRules[to]; ok {
		log.WithFields(log.Fields{
			"task_id":           sm.name,
			"rule_from":         curState,
			"rule_to":           to,
			"meta_info_noindex": sm.GetMetaInfo(),
			"reason":            sm.reason,
		}).Info("Task transitioned to timeout state")
		if rule.Timeout != 0 {
			log.WithFields(log.Fields{
				"task_id":   sm.name,
				"rule_from": curState,
				"rule_to":   to,
				"timeout":   rule.Timeout.String(),
				"reason":    sm.reason,
			}).Info("Starting timer to recover state if needed")
			err := sm.timer.Start(rule.Timeout)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"task_id": sm.name,
					"state":   to,
				}).Error("timer could not be started retrying ...")
				sm.timer.Stop()
				err = sm.timer.Start(rule.Timeout)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"task_id": sm.name,
						"state":   to,
					}).Error("timer could not be started, returning")
					return err
				}
			}
		}
	}
	log.WithFields(log.Fields{
		"task_id":           sm.name,
		"from_state ":       curState,
		"to_state":          to,
		"meta_info_noindex": sm.GetMetaInfo(),
		"reason":            sm.reason,
	}).Info("Task transitioned successfully")
	return nil
}

// isValidTransition checks if the transition is allowed
// form source state to destination state
func (sm *statemachine) isValidTransition(to State) error {
	// Checking if the current state is same as destination
	// Then no need to transition and return error
	if sm.current == to {
		return ErrNoOpTransition
	}

	if val, ok := sm.rules[sm.current]; ok {
		if val.From != sm.current {
			return errors.Errorf("invalid transition for %s "+
				"[from %s to %s]", sm.name, sm.current, to)
		}
		for _, dest := range val.To {
			if dest == to {
				return nil
			}
		}
	}
	return errors.Errorf("invalid transition for %s [from %s to %s]",
		sm.name, sm.current, to)
}

// GetCurrentState returns the current state of the state machine
func (sm *statemachine) GetCurrentState() State {
	sm.RLock()
	defer sm.RUnlock()
	return sm.current
}

func (sm *statemachine) GetReason() string {
	sm.RLock()
	defer sm.RUnlock()
	return sm.reason
}

func (sm *statemachine) GetLastUpdateTime() time.Time {
	sm.RLock()
	defer sm.RUnlock()
	return sm.lastUpdatedTime
}

// GetName returns the name of the state machine object
func (sm *statemachine) GetName() string {
	return sm.name
}

// Terminate terminates the state machine
func (sm *statemachine) Terminate() {
	sm.Lock()
	defer sm.Unlock()

	// check if current state is a timeout state
	if rule, ok := sm.timeoutRules[sm.current]; ok {
		if rule.Timeout != 0 {
			sm.timer.Stop()
		}
	}
}

// GetTimeOutRules returns the timeout rules defined for state machine
// Its a map of [from state] to timeout rule
func (sm *statemachine) GetTimeOutRules() map[State]*TimeoutRule {
	return sm.timeoutRules
}

// rollbackState recovers the state.
// Note: the caller should acquire the lock on the state machine before calling.
func (sm *statemachine) rollbackState() error {
	if sm.timeoutRules == nil {
		return nil
	}

	if _, ok := sm.timeoutRules[sm.current]; !ok {
		return nil
	}

	rule := sm.timeoutRules[sm.current]

	if time.Now().Sub(sm.lastUpdatedTime) <= rule.Timeout {
		return nil
	}

	// Creating Transition to pass to callbacks, we need to pass this
	// Transition object to Precallback and precall back has to fill
	// the transition object with the desired "TO" state
	t := &Transition{
		StateMachine: sm,
		From:         sm.current,
		To:           "",
		Params:       nil,
	}

	// Checking if precall back is nil then get the first TO state
	if rule.PreCallback == nil {
		t.To = rule.To[0]
	} else {
		log.WithFields(log.Fields{
			"task_id":           t.StateMachine.GetName(),
			"rule_from":         sm.current,
			"rule_to":           "",
			"meta_info_noindex": sm.GetMetaInfo(),
			"reason":            sm.reason,
		}).Info("Calling PreCallback")

		// Call the precall back function, which should fil the t.To state
		err := rule.PreCallback(t)

		if err != nil {
			log.WithFields(log.Fields{
				"task_id":           sm.name,
				"rule_from":         t.From,
				"rule_to":           t.To,
				"current_state":     sm.current,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Info("pre call back returned error")
			return err
		}

		if t.To == "" {
			log.WithFields(log.Fields{
				"task_id":           sm.name,
				"rule_from":         t.From,
				"rule_to":           t.To,
				"current_state":     sm.current,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Info("pre call back did not return to_state")
			return errors.New("pre call back failed")
		}
	}

	log.WithFields(log.Fields{
		"task_id":           t.StateMachine.GetName(),
		"rule_from":         rule.From,
		"rule_to":           t.To,
		"current_state":     sm.current,
		"meta_info_noindex": sm.GetMetaInfo(),
		"reason":            sm.reason,
	}).Info("Transitioning from Timeout")

	// Doing actual transition
	sm.reason = fmt.Sprintf("rollback from state %s to state %s due to timeout", sm.current, t.To)
	sm.current = t.To
	sm.lastUpdatedTime = time.Now()

	// invoking callback function
	if rule.Callback != nil {
		err := rule.Callback(t)
		if err != nil {
			log.WithFields(log.Fields{
				"task_id":           sm.name,
				"rule_from":         rule.From,
				"rule_to":           rule.To,
				"current_state":     sm.current,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Error("Error in call back")
			return err
		}
	}

	// Callback the transition callback
	if sm.transitionCallback != nil {
		err := sm.transitionCallback(t)
		if err != nil {
			log.WithFields(log.Fields{
				"task_id":           t.StateMachine.GetName(),
				"rule_from":         rule.From,
				"rule_to":           rule.To,
				"current_state":     sm.current,
				"meta_info_noindex": sm.GetMetaInfo(),
				"reason":            sm.reason,
			}).Error("Error in transition callback")
			return err
		}
	}
	return nil
}

// GetMetaInfo returns the meta info for the statemachine
func (sm *statemachine) GetMetaInfo() map[string]string {
	return sm.metaInfo
}
