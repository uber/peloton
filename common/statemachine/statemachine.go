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
	// to is the destination state
	To State
	// timeout for transition to "to" state
	Timeout time.Duration
	// callback is transition function which defines 1:1 mapping
	// of callbacks
	Callback func(*Transition) error
}

// Callback is the type for callback function
type Callback func(*Transition) error

// StateMachine is the interface wrapping around the statemachine Object
// Using to not expose full object
type StateMachine interface {

	// TransitTo function transits to desired state
	TransitTo(to State, reason string, args ...interface{}) error

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
		if val, ok := sources[s]; ok {
			log.WithField("Source ", val).Error("Already exists, duplicate entry")
			return errors.New("invalid rule to be applied, duplicate sources")
		}
	}
	return nil
}

// TransitTo is the function which clients will call to transition from one state to other
// this also calls the callbacks after the valid transition is done
func (sm *statemachine) TransitTo(to State, reason string, args ...interface{}) error {
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
		Params:       args,
	}

	// Storing values for reverseTransition
	curState := sm.current

	// Try to stop state recovery if its transitioning
	// from timeout state
	if _, ok := sm.timeoutRules[curState]; ok {
		log.WithFields(log.Fields{
			"task_id": sm.name,
			"from ":   curState,
			"to":      to,
		}).Info("Stopping state timeout recovery")
		err = sm.timer.Stop()
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id": sm.name,
				"from ":   curState,
				"to":      to,
			}).Error("failed to stop state timeout recovery")
			return err
		}
	}

	// Doing actual transition
	sm.current = to
	sm.lastUpdatedTime = time.Now()
	sm.reason = reason

	// invoking callback function
	if sm.rules[curState].Callback != nil {
		err = sm.rules[curState].Callback(t)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id":        sm.GetName(),
				"current_state ": curState,
				"to_state":       to,
			}).Error("callback failed for task")

			return err
		}
	}

	// Run the transition callback
	if sm.transitionCallback != nil {
		err = sm.transitionCallback(t)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_id":        sm.GetName(),
				"current_state ": curState,
				"to_state":       to,
			}).Error("transition callback failed for task")
			return err
		}
	}
	// Checking if this STATE is timeout state
	if rule, ok := sm.timeoutRules[to]; ok {
		log.WithFields(log.Fields{
			"task_id": sm.name,
			"from":    curState,
			"to":      to,
		}).Info("Task transitioned to timeout state")
		if rule.Timeout != 0 {
			err := sm.timer.Start(rule.Timeout)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"task_id": sm.name,
					"state":   to,
				}).Error("timer could not be started retrying ...")
				err = sm.timer.Stop()
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"task_id": sm.name,
						"state":   to,
					}).Error("timer could not be stoped")
				}
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
	return nil
}

// isValidTransition checks if the transition is allowed
// form source state to destination state
func (sm *statemachine) isValidTransition(to State) error {
	// Checking if the current state is same as destination
	// Then no need to transition and return error
	if sm.current == to {
		return errors.Errorf("already reached to state %s no need to "+
			"transition", to)
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

// rollbackState recovers the state.
func (sm *statemachine) rollbackState() error {
	sm.Lock()
	defer sm.Unlock()

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

	//  Creating Transition to pass to callbacks
	t := &Transition{
		StateMachine: sm,
		From:         sm.current,
		To:           rule.To,
		Params:       nil,
	}

	log.WithFields(log.Fields{
		"task_id":       t.StateMachine.GetName(),
		"rule_from":     rule.From,
		"rule_to":       rule.To,
		"current_state": sm.current,
	}).Debug("Transitioning from Timeout")

	// Doing actual transition
	sm.current = rule.To
	sm.lastUpdatedTime = time.Now()
	sm.reason = fmt.Sprintf("rollback from state %s to state %s", sm.current, rule.To)

	// invoking callback function
	if rule.Callback != nil {
		err := rule.Callback(t)
		if err != nil {
			log.WithFields(log.Fields{
				"task_id":       sm.name,
				"rule_from":     rule.From,
				"rule_to":       rule.To,
				"current_state": sm.current,
			}).Error("Error in call back")
			return err
		}
	}

	// Callback the transition callback
	if sm.transitionCallback != nil {
		err := sm.transitionCallback(t)
		if err != nil {
			log.WithFields(log.Fields{
				"task_id":       t.StateMachine.GetName(),
				"rule_from":     rule.From,
				"rule_to":       rule.To,
				"current_state": sm.current,
			}).Error("Error in transition callback")
			return err
		}
	}
	return nil
}
