package statemachine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type StateTimerTestSuite struct {
	suite.Suite

	task         *StateMachineTask
	stateMachine *statemachine
	statetimer   *statetimer
}

func (suite *StateTimerTestSuite) SetupTest() {
	suite.task = new(StateMachineTask)
	suite.task.state = "initialized"
	suite.stateMachine = &statemachine{
		name:               "task-1",
		current:            suite.task.state,
		rules:              make(map[State]*Rule),
		timeoutRules:       make(map[State]*TimeoutRule),
		transitionCallback: nil,
		lastUpdatedTime:    time.Now(),
	}
	suite.statetimer = &statetimer{
		statemachine: suite.stateMachine,
		stopChan:     make(chan struct{}, 1),
	}
}

func TestPelotonStateTimer(t *testing.T) {
	suite.Run(t, new(StateTimerTestSuite))
}

func (suite *StateTimerTestSuite) TestTimer() {
	suite.statetimer.Start(4 * time.Second)
	suite.EqualValues(suite.statetimer.runningState, runningStateRunning)
	suite.statetimer.Stop()
	suite.EqualValues(suite.statetimer.runningState, runningStateNotStarted)
	suite.statetimer.Stop()
	suite.EqualValues(suite.statetimer.runningState, runningStateNotStarted)
}
