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
