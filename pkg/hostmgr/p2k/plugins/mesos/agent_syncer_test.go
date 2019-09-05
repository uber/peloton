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

package mesos

import (
	"testing"
	"time"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	mpbmocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type AgentSyncerTestSuite struct {
	suite.Suite

	ctrl           *gomock.Controller
	operatorClient *mpbmocks.MockMasterOperatorClient
	agentSyncer    *agentSyncer
}

func (suite *AgentSyncerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.operatorClient = mpbmocks.NewMockMasterOperatorClient(suite.ctrl)

	suite.agentSyncer = newAgentSyncer(
		suite.operatorClient,
		10*time.Second,
	)
}

func (suite *AgentSyncerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestStartStop test normal start stop
func (suite *AgentSyncerTestSuite) TestStartStop() {
	// Agents method should be called at least once upon start
	suite.operatorClient.
		EXPECT().
		Agents().
		Return(nil, nil).
		MinTimes(1)

	suite.agentSyncer.Start()
	suite.agentSyncer.Stop()
}

// TestMultipleStart tests multiple consecutive starts,
// the later one should be noop
func (suite *AgentSyncerTestSuite) TestMultipleStart() {
	// set an arbitrary large interval, so it would be called
	// at most once during unit test
	suite.agentSyncer.refreshInterval = 100 * time.Hour
	suite.operatorClient.
		EXPECT().
		Agents().
		Return(nil, nil).
		Times(1)

	suite.agentSyncer.Start()
	// second call to Start should be a noop
	suite.agentSyncer.Start()
	suite.agentSyncer.Stop()
}

// TestStartForExtendedPeriodOfTime tests run is called
// continuously after Start
func (suite *AgentSyncerTestSuite) TestStartForExtendedPeriodOfTime() {
	// set a small refresh interval, so runOnce would be called
	// multiple times
	suite.agentSyncer.refreshInterval = 1000 * time.Microsecond

	// Agents method should be called at least 500 times within
	// 1s sleep
	suite.operatorClient.
		EXPECT().
		Agents().
		Return(nil, nil).
		MinTimes(500)

	suite.agentSyncer.Start()
	time.Sleep(1 * time.Second)
	suite.agentSyncer.Stop()
}

// TestRunOnce tests agent info is sent to channel
func (suite *AgentSyncerTestSuite) TestRunOnce() {
	hostname := "hostname1"

	agent := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{
			{
				AgentInfo: &mesosv1.AgentInfo{
					Hostname: &hostname,
				},
			},
		},
	}

	suite.operatorClient.EXPECT().
		Agents().
		Return(agent, nil)

	// start lifecycle because runOnce uses
	// lf.StopCh(), which is initialized when Start()
	// is called
	suite.agentSyncer.lf.Start()
	suite.agentSyncer.runOnce()

	agents := <-suite.agentSyncer.AgentCh()

	suite.Len(agents, 1)
}

func TestAgentSyncerTestSuite(t *testing.T) {
	suite.Run(t, new(AgentSyncerTestSuite))
}
