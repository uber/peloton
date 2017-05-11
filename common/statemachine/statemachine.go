package statemachine

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/uber-go/atomic"
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

// Callback is the type for callback function
type Callback func(*Transition) error

// StateMachine is the interface wrapping around the statemachine Object
// Using to not expose full object
type StateMachine interface {

	// TransitTo function transits to desired state
	TransitTo(to State, args ...interface{}) error

	// GetCurrentState returns the current state of State Machine
	GetCurrentState() State

	// GetName returns the Name of the StateMachine object
	GetName() string
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

	// This atomic boolean helps to identify if previous transition is
	// complete or still not done
	inTransition atomic.Bool
}

// NewStateMachine it will create the new state machine
// which clients can use to do tansitions on the object
func NewStateMachine(
	name string,
	current State,
	rules map[State]*Rule,
	trasitionCallback Callback,
) (StateMachine, error) {
	sm := &statemachine{
		name:               name,
		current:            current,
		rules:              make(map[State]*Rule),
		transitionCallback: trasitionCallback,
	}

	err := sm.addRules(rules)
	if err != nil {
		return nil, err
	}
	return sm, nil
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
func (sm *statemachine) TransitTo(to State, args ...interface{}) error {
	// Locking the statemachine to synchronize state changes
	sm.Lock()

	// Checking is previous transitions are complete
	inTransition := sm.inTransition.Load()
	if inTransition {
		sm.Unlock()
		return errors.Errorf("transition to  %s not able to "+
			"transition, previous transition is not "+
			"finished yet", fmt.Sprint(to))
	}

	// checking if transition is allowed
	err := sm.isValidTransition(to)
	if err != nil {
		sm.Unlock()
		return err
	}

	//  Creating Transition to pass to callbacks
	t := &Transition{
		StateMachine: sm,
		From:         sm.current,
		To:           to,
		Params:       args,
	}

	// Changing intransition value by that we block rest
	// of the transitions
	sm.inTransition.Swap(true)

	// Doing actual transition
	curState := sm.current
	sm.current = to

	// Unlocking the state machine to call callback functions
	sm.Unlock()

	// invoking callback function
	if sm.rules[curState].Callback != nil {
		err = sm.rules[curState].Callback(t)
		if err != nil {
			log.Error("error in callback ")
			return err
		}
	}

	// Callback the transititon callback
	if sm.transitionCallback != nil {
		err = sm.transitionCallback(t)
		if err != nil {
			log.Error("Error in transition callback ")
			return err
		}
	}

	// Making transition done
	sm.inTransition.Swap(false)

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

// GetName returns the name of the state machine object
func (sm *statemachine) GetName() string {
	return sm.name
}
