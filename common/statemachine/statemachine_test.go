package statemachine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type StateMachineTask struct {
	state State
}

type StateMachineTestSuite struct {
	suite.Suite
	task         *StateMachineTask
	stateMachine StateMachine
}

func (suite *StateMachineTestSuite) SetupTest() {
	suite.task = new(StateMachineTask)
	suite.task.state = "initialized"
	var err error
	suite.stateMachine, err = NewBuilder().
		WithName("task1").
		WithCurrentState(suite.task.state).
		WithTransitionCallback(nil).
		AddRule(
			&Rule{
				from: "initialized",
				to:   []State{"running", "killed"},
				callback: func(t *Transition) error {
					switch t.To {
					case "running":
						return suite.callbackRunning(t)
					case "killed":
						return suite.callbackKilledFromInit(t)
					default:
						return nil
					}
				},
			}).
		AddRule(
			&Rule{
				from: "running",
				to:   []State{"killed", "succeeded", "running"},
				callback: func(t *Transition) error {
					switch t.To {
					case "killed":
						return suite.callbackKilledfromrunning(t)
					case "succeeded":
						return suite.callbacksucceededfromrunning(t)
					default:
						return nil
					}
				},
			}).
		Build()
	suite.NoError(err)
}

func TestPelotonStateMachine(t *testing.T) {
	suite.Run(t, new(StateMachineTestSuite))
}

func (suite *StateMachineTestSuite) callbackRunning(t *Transition) error {
	suite.task.state = t.To
	return nil
}

func (suite *StateMachineTestSuite) callbackKilledFromInit(t *Transition) error {
	suite.task.state = t.To
	return nil
}

func (suite *StateMachineTestSuite) callbackKilledfromrunning(t *Transition) error {
	suite.task.state = t.To
	return nil
}

func (suite *StateMachineTestSuite) callbacksucceededfromrunning(t *Transition) error {
	suite.task.state = t.To
	err := suite.stateMachine.TransitTo("killed")
	return err
}

func (suite *StateMachineTestSuite) TestCallbacksRunning() {
	err := suite.stateMachine.TransitTo("running")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "running")
	err = suite.stateMachine.TransitTo("killed")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "killed")
}

func (suite *StateMachineTestSuite) TestCallbacksKilled() {
	err := suite.stateMachine.TransitTo("killed")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "killed")
}

func (suite *StateMachineTestSuite) TestInvalidTransition() {
	err := suite.stateMachine.TransitTo("killed")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "killed")
	err = suite.stateMachine.TransitTo("initialized")
	suite.Error(err)
}

func (suite *StateMachineTestSuite) TestTransitionWithInTransition() {
	err := suite.stateMachine.TransitTo("running")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "running")
	// Previous transition is not finished yet, should have error
	err = suite.stateMachine.TransitTo("succeeded")
	suite.Error(err)
}

func (suite *StateMachineTestSuite) TestTransitionSameState() {
	err := suite.stateMachine.TransitTo("running")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "running")
	// Transition to same state
	err = suite.stateMachine.TransitTo("running")
	suite.Error(err)
}
