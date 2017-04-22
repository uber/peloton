package statemachine

// State defines the state
type State string

// Transition defines the transition for the callback
// which will be passed to the call back function
// during the transition
type Transition struct {
	// StateMachine object
	StateMachine StateMachine

	// From which state transition is happening
	From State

	// TO which state transition is happening
	To State

	// Arguments passed during the transition
	// which will be passed to callback function
	Params []interface{}
}
