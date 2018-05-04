package statemachine

import (
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
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
		WithTransitionCallback(suite.TransitionCallBack).
		AddRule(
			&Rule{
				From: "initialized",
				To:   []State{"running", "killed", "placing", "launching"},
				Callback: func(t *Transition) error {
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
				From: "running",
				To:   []State{"killed", "succeeded", "running", "placing"},
				Callback: func(t *Transition) error {
					switch t.To {
					case "killed":
						return suite.callbackKilledfromrunning(t)
					case "succeeded":
						return suite.callbacksucceededfromrunning(t)
					case "placing":
						return nil
					default:
						return nil
					}
				},
			}).
		AddRule(
			&Rule{
				From: "placing",
				To:   []State{"succeeded"},
				Callback: func(t *Transition) error {
					switch t.To {
					case "succeeded":
						return nil
					default:
						return nil
					}
				},
			}).
		AddTimeoutRule(
			&TimeoutRule{
				From:        "killed",
				To:          []State{"running"},
				Timeout:     2 * time.Second,
				Callback:    suite.callbackTimeout,
				PreCallback: suite.preCallbackTimeout,
			},
		).
		AddTimeoutRule(
			&TimeoutRule{
				From:    "placing",
				To:      []State{"killed"},
				Timeout: 2 * time.Second,
			},
		).
		AddTimeoutRule(
			&TimeoutRule{
				From:        "launching",
				To:          []State{"ready"},
				Timeout:     2 * time.Second,
				PreCallback: suite.preCallbackTimeoutError,
			},
		).
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
	return errors.New("Error")
}

func (suite *StateMachineTestSuite) TransitionCallBack(t *Transition) error {
	return nil
}

func (suite *StateMachineTestSuite) callbackTimeout(t *Transition) error {
	return nil
}

func (suite *StateMachineTestSuite) preCallbackTimeout(t *Transition) error {
	t.To = "running"
	return nil
}

func (suite *StateMachineTestSuite) preCallbackTimeoutError(t *Transition) error {
	// This will return To state empty which is error condition,
	// So transition should not happen
	t.To = ""
	return nil
}

func (suite *StateMachineTestSuite) TestCallbacksRunning() {
	mesosTaskID := suite.stateMachine.GetName() + "mesosid"
	err := suite.stateMachine.TransitTo("running", WithReason("move to running"),
		WithInfo("mesos_task_id", mesosTaskID))
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "running")
	suite.Equal(suite.stateMachine.GetReason(), "move to running")
	suite.Equal(suite.stateMachine.GetMetaInfo()["mesos_task_id"], mesosTaskID)
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
	err = suite.stateMachine.TransitTo("initialized", WithReason(""))
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

func (suite *StateMachineTestSuite) TestTimeOut() {
	err := suite.stateMachine.TransitTo("killed")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "killed")
	time.Sleep(3 * time.Second)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "running")
	suite.Equal(fmt.Sprint(suite.stateMachine.GetReason()),
		"rollback from state killed to state running due to timeout")
}

func (suite *StateMachineTestSuite) TestPreCallBackNil() {
	err := suite.stateMachine.TransitTo("placing")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "placing")
	// As timeout is 2 seconds , we need to wait for 3 seconds to actual timeout happen
	time.Sleep(3 * time.Second)
	// Precall back succeeds. timeout should transit state machine to "killed"
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "killed")
	suite.Equal(fmt.Sprint(suite.stateMachine.GetReason()),
		"rollback from state placing to state killed due to timeout")
}

func (suite *StateMachineTestSuite) TestPreCallBackError() {
	err := suite.stateMachine.TransitTo("launching")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "launching")
	// As timeout is 2 seconds , we need to wait for 3 seconds to actual timeout happen
	time.Sleep(3 * time.Second)
	// As there is error in callback , transition should not happen
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "launching")
}

func (suite *StateMachineTestSuite) TestCancelTimeOutTransition() {
	err := suite.stateMachine.TransitTo("placing")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "placing")
	err = suite.stateMachine.TransitTo("succeeded")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "succeeded")
}

func (suite *StateMachineTestSuite) TestTerminateStateMachine() {
	err := suite.stateMachine.TransitTo("killed")
	suite.NoError(err)
	suite.Equal(fmt.Sprint(suite.task.state), "killed")
	suite.stateMachine.Terminate()
	time.Sleep(3 * time.Second)
	suite.Equal(fmt.Sprint(suite.stateMachine.GetCurrentState()), "killed")
}

func (suite *StateMachineTestSuite) TestMetaInfo() {
	err := suite.stateMachine.TransitTo("placing", WithInfo("key1", "value1"))
	suite.NoError(err)
	suite.Equal(suite.stateMachine.GetMetaInfo()["key1"], "value1")
}
