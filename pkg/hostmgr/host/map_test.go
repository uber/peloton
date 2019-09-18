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

package host

import (
	"errors"
	"fmt"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

var (
	_defaultResourceValue = 1
)

type hostMapTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	testScope       tally.TestScope
	operatorClient  *mock_mpb.MockMasterOperatorClient
	mockHostInfoOps *orm_mocks.MockHostInfoOps
}

func (suite *hostMapTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.operatorClient = mock_mpb.NewMockMasterOperatorClient(suite.ctrl)
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.ctrl)
}

func makeAgentsResponse(numAgents int) *mesosmaster.Response_GetAgents {
	response := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{},
	}
	for i := 0; i < numAgents; i++ {
		resVal := float64(_defaultResourceValue)
		tmpID := fmt.Sprintf("id-%d", i)
		resources := []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName(common.MesosCPU).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosDisk).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosGPU).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosCPU).
				WithValue(resVal).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(resVal).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build(),
		}

		for _, r := range resources {
			r.Reservations = []*mesos.Resource_ReservationInfo{
				{
					Role: &[]string{"peloton"}[0],
				},
			}
		}

		pid := fmt.Sprintf("slave(%d)@%d.%d.%d.%d:123", i, i, i, i, i)
		getAgent := &mesosmaster.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Hostname:  &tmpID,
				Resources: resources,
			},
			TotalResources: resources,
			Pid:            &pid,
		}
		response.Agents = append(response.Agents, getAgent)
	}

	return response
}

func (suite *hostMapTestSuite) setupMocks(response *mesosmaster.Response_GetAgents) {
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)

	suite.operatorClient.EXPECT().Agents().Return(response, nil)
	suite.operatorClient.EXPECT().GetMaintenanceStatus().Return(nil, nil)
	for _, a := range response.GetAgents() {
		ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(a.GetPid())
		suite.NoError(err)

		suite.mockHostInfoOps.EXPECT().Create(
			gomock.Any(),
			a.GetAgentInfo().GetHostname(),
			ip,
			pbhost.HostState_HOST_STATE_UP,
			pbhost.HostState_HOST_STATE_UP,
			map[string]string{},
			"",
			"",
		).Return(nil)
	}

}

func (suite *hostMapTestSuite) TestRefresh() {
	defer suite.ctrl.Finish()

	loader := &Loader{
		OperatorClient:     suite.operatorClient,
		Scope:              suite.testScope,
		SlackResourceTypes: []string{common.MesosCPU},
		HostInfoOps:        suite.mockHostInfoOps,
	}

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().
			Return(nil, errors.New("unable to get agents")),
	)
	loader.Load(nil)

	numAgents := 2000
	response := makeAgentsResponse(numAgents)

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.operatorClient.EXPECT().Agents().Return(response, nil)
	suite.operatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesosmaster.Response_GetMaintenanceStatus{
			Status: &mesosmaintenance.ClusterStatus{
				DrainingMachines: []*mesosmaintenance.ClusterStatus_DrainingMachine{
					{
						Id: &mesos.MachineID{
							Hostname: response.
								Agents[len(response.GetAgents())-1].
								AgentInfo.Hostname,
						},
					},
				},
			},
		}, nil)

	for _, a := range response.GetAgents() {
		ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(a.GetPid())
		suite.NoError(err)

		suite.mockHostInfoOps.EXPECT().Create(
			gomock.Any(),
			a.GetAgentInfo().GetHostname(),
			ip,
			pbhost.HostState_HOST_STATE_UP,
			pbhost.HostState_HOST_STATE_UP,
			map[string]string{},
			"",
			"",
		).Return(nil)
	}
	loader.Load(nil)

	numRegisteredAgents := numAgents - 1
	m := GetAgentMap()
	suite.Len(m.RegisteredAgents, numRegisteredAgents)
	suite.Len(m.HostCapacities, numRegisteredAgents)

	id1 := "id-1"
	a1 := GetAgentInfo(id1)
	suite.NotEmpty(a1.GetResources())
	id2 := "id-20000"
	a2 := GetAgentInfo(id2)
	suite.Nil(a2)

	// Check per host capacities.
	resVal := float64(_defaultResourceValue)
	expectedPhysical := scalar.Resources{
		CPU:  resVal,
		Mem:  resVal,
		Disk: resVal,
		GPU:  resVal,
	}
	expectedSlack := scalar.Resources{
		CPU: resVal,
	}
	for hostname, capacity := range m.HostCapacities {
		suite.Equal(expectedPhysical, capacity.Physical, hostname)
		suite.Equal(expectedSlack, capacity.Slack, hostname)
	}

	// Check cluster capacity and metrics.
	gauges := suite.testScope.Snapshot().Gauges()
	suite.Contains(gauges, "registered_hosts+")
	suite.Equal(float64(numRegisteredAgents), gauges["registered_hosts+"].Value())
	suite.Contains(gauges, "cpus+")
	suite.Equal(float64(numRegisteredAgents*_defaultResourceValue), gauges["cpus+"].Value())
	suite.Contains(gauges, "cpus_revocable+")
	suite.Equal(float64(numRegisteredAgents*_defaultResourceValue), gauges["cpus_revocable+"].Value())
	suite.Contains(gauges, "mem+")
	suite.Equal(float64(numRegisteredAgents*_defaultResourceValue), gauges["mem+"].Value())
	suite.Contains(gauges, "disk+")
	suite.Equal(float64(numRegisteredAgents*_defaultResourceValue), gauges["disk+"].Value())
	suite.Contains(gauges, "gpus+")
	suite.Equal(float64(numRegisteredAgents*_defaultResourceValue), gauges["gpus+"].Value())
}

// TestIsRegisteredAsPelotonAgent tests IsRegisteredAsPelotonAgent
func (suite *hostMapTestSuite) TestIsRegisteredAsPelotonAgent() {
	loader := &Loader{
		OperatorClient: suite.operatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}

	// Mock 1 host `id-0` as an non-peloton agent
	agentsResponse := makeAgentsResponse(2)
	for _, r := range agentsResponse.Agents[0].GetAgentInfo().GetResources() {
		r.Reservations[0].Role = &[]string{"*"}[0]
	}
	suite.setupMocks(agentsResponse)

	loader.Load(nil)
	suite.False(IsRegisteredAsPelotonAgent("id-0", "peloton"))
	suite.True(IsRegisteredAsPelotonAgent("id-1", "peloton"))
}

func (suite *hostMapTestSuite) TestIsHostRegistered() {
	loader := &Loader{
		OperatorClient: suite.operatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := makeAgentsResponse(1)

	var hostInfos []*pbhost.HostInfo
	for _, a := range agentsResponse.GetAgents() {
		hostInfos = append(hostInfos, &pbhost.HostInfo{
			Hostname: a.GetAgentInfo().GetHostname(),
		})
	}
	suite.setupMocks(agentsResponse)
	loader.Load(nil)
	suite.True(IsHostRegistered("id-0"))
	suite.False(IsHostRegistered("id-1"))
}

func (suite *hostMapTestSuite) TestBuildHostInfoForRegisteredAgents() {
	loader := &Loader{
		OperatorClient: suite.operatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := makeAgentsResponse(2)
	suite.setupMocks(agentsResponse)
	loader.Load(nil)

	hostInfoMap := make(map[string]*pbhost.HostInfo)
	hostInfoMap["id-0"] = &pbhost.HostInfo{
		Hostname: "id-0",
		Ip:       "0.0.0.0",
		State:    pbhost.HostState_HOST_STATE_UP,
	}
	hostInfoMap["id-1"] = &pbhost.HostInfo{
		Hostname: "id-1",
		Ip:       "1.1.1.1",
		State:    pbhost.HostState_HOST_STATE_UP,
	}

	hostInfosBuilt, err := BuildHostInfoForRegisteredAgents()
	suite.NoError(err)
	suite.Equal(hostInfoMap, hostInfosBuilt)
}

func (suite *hostMapTestSuite) TestGetUpHostIP() {
	loader := &Loader{
		OperatorClient: suite.operatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := makeAgentsResponse(2)
	suite.setupMocks(agentsResponse)
	loader.Load(nil)

	IP, err := GetUpHostIP("id-0")
	suite.NoError(err)
	suite.Equal("0.0.0.0", IP)

	IP, err = GetUpHostIP("id-1")
	suite.NoError(err)
	suite.Equal("1.1.1.1", IP)

	IP, err = GetUpHostIP("id-2")
	suite.Equal(errors.New("unknown host id-2"), err)
	suite.Equal("", IP)
}

func TestHostMapTestSuite(t *testing.T) {
	suite.Run(t, new(hostMapTestSuite))
}
