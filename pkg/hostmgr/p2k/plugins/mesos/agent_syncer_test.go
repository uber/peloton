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
	"github.com/uber/peloton/pkg/common/util"
	mpbmocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type AgentSyncerTestSuite struct {
	suite.Suite

	ctrl           *gomock.Controller
	hostEventCh    chan *scalar.HostEvent
	operatorClient *mpbmocks.MockMasterOperatorClient
	agentSyncer    *agentSyncer
}

func (suite *AgentSyncerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.hostEventCh = make(chan *scalar.HostEvent, 1000)
	suite.operatorClient = mpbmocks.NewMockMasterOperatorClient(suite.ctrl)

	suite.agentSyncer = newAgentSyncer(
		suite.operatorClient,
		suite.hostEventCh,
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
	cpu := 4.0
	mem := 1024.0
	disk := 2048.0
	gpu := 1.0
	hostname := "hostname1"

	agent := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{
			{
				AgentInfo: &mesosv1.AgentInfo{
					Hostname: &hostname,
				},
				TotalResources: []*mesosv1.Resource{
					util.NewMesosResourceBuilder().
						WithName("cpus").
						WithValue(cpu).
						Build(),
					util.NewMesosResourceBuilder().
						WithName("mem").
						WithValue(mem).
						Build(),
					util.NewMesosResourceBuilder().
						WithName("disk").
						WithValue(disk).
						Build(),
					util.NewMesosResourceBuilder().
						WithName("gpus").
						WithValue(gpu).
						Build(),
				},
			},
		},
	}

	suite.operatorClient.EXPECT().
		Agents().
		Return(agent, nil)

	suite.agentSyncer.runOnce()

	hostEvent := <-suite.hostEventCh

	suite.Equal(hostEvent.GetEventType(), scalar.UpdateAgent)
	suite.Equal(hostEvent.GetHostInfo().GetCapacity().GetCpu(), cpu)
	suite.Equal(hostEvent.GetHostInfo().GetCapacity().GetMemMb(), mem)
	suite.Equal(hostEvent.GetHostInfo().GetCapacity().GetDiskMb(), disk)
	suite.Equal(hostEvent.GetHostInfo().GetCapacity().GetGpu(), gpu)
}

func TestAgentSyncerTestSuite(t *testing.T) {
	suite.Run(t, new(AgentSyncerTestSuite))
}
