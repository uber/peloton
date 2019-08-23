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
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	host "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	hm "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

var (
	_defaultResourceValue = 1
)

type hostMapTestSuite struct {
	suite.Suite

	ctrl           *gomock.Controller
	testScope      tally.TestScope
	operatorClient *mock_mpb.MockMasterOperatorClient
}

func (suite *hostMapTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.operatorClient = mock_mpb.NewMockMasterOperatorClient(suite.ctrl)
}

func makeAgentsResponse(numAgents int) *mesos_master.Response_GetAgents {
	response := &mesos_master.Response_GetAgents{
		Agents: []*mesos_master.Response_GetAgents_Agent{},
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
					Role: &[]string{pelotonAgentRole}[0],
				},
			}
		}

		getAgent := &mesos_master.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Hostname:  &tmpID,
				Resources: resources,
			},
			TotalResources: resources,
		}
		response.Agents = append(response.Agents, getAgent)
	}

	return response
}

func (suite *hostMapTestSuite) TestRefresh() {
	defer suite.ctrl.Finish()

	mockMaintenanceMap := hm.NewMockMaintenanceHostInfoMap(suite.ctrl)
	loader := &Loader{
		OperatorClient:         suite.operatorClient,
		Scope:                  suite.testScope,
		SlackResourceTypes:     []string{common.MesosCPU},
		MaintenanceHostInfoMap: mockMaintenanceMap,
	}

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().
			Return(nil, errors.New("unable to get agents")),
	)
	loader.Load(nil)

	numAgents := 2000
	response := makeAgentsResponse(numAgents)

	mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(gomock.Any()).
		Return([]*host.HostInfo{}).Times(len(response.GetAgents()) - 1)

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),

		mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos([]string{*response.Agents[numAgents-1].AgentInfo.Hostname}).
			Return([]*host.HostInfo{
				{
					Hostname: *response.Agents[numAgents-1].AgentInfo.Hostname,
					State:    host.HostState_HOST_STATE_DRAINING,
				},
			}),
	)
	loader.Load(nil)

	numRegisteredAgents := numAgents - 1
	m := GetAgentMap()
	suite.Len(m.RegisteredAgents, numRegisteredAgents)

	id1 := "id-1"
	a1 := GetAgentInfo(id1)
	suite.NotEmpty(a1.GetResources())
	id2 := "id-20000"
	a2 := GetAgentInfo(id2)
	suite.Nil(a2)

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

func (suite *hostMapTestSuite) TestMaintenanceHostInfoMap() {
	maintenanceHostInfoMap := NewMaintenanceHostInfoMap(tally.NoopScope)
	suite.NotNil(maintenanceHostInfoMap)

	drainingHostInfos := []*host.HostInfo{
		{
			Hostname: "host1",
			Ip:       "0.0.0.0",
			State:    host.HostState_HOST_STATE_DRAINING,
		},
	}

	downHostInfos := []*host.HostInfo{
		{
			Hostname: "host2",
			Ip:       "0.0.0.1",
			State:    host.HostState_HOST_STATE_DOWN,
		},
	}

	var (
		drainingHosts []string
		downHosts     []string
	)

	for _, hostInfo := range drainingHostInfos {
		drainingHosts = append(drainingHosts, hostInfo.GetHostname())
	}
	for _, hostInfo := range downHostInfos {
		downHosts = append(downHosts, hostInfo.GetHostname())
	}
	suite.Nil(maintenanceHostInfoMap.GetDrainingHostInfos([]string{}))
	for _, drainingHostInfo := range drainingHostInfos {
		maintenanceHostInfoMap.AddHostInfo(drainingHostInfo)
	}
	suite.NotEmpty(maintenanceHostInfoMap.GetDrainingHostInfos(drainingHosts))

	suite.Nil(maintenanceHostInfoMap.GetDownHostInfos([]string{}))
	for _, downHostInfo := range downHostInfos {
		maintenanceHostInfoMap.AddHostInfo(downHostInfo)
	}
	suite.NotEmpty(maintenanceHostInfoMap.GetDownHostInfos(downHosts))

	drainingHostInfoMap := make(map[string]*host.HostInfo)
	for _, hostInfo := range maintenanceHostInfoMap.GetDrainingHostInfos([]string{}) {
		drainingHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, hostInfo := range drainingHostInfos {
		suite.NotNil(drainingHostInfoMap[hostInfo.GetHostname()])
		suite.Equal(hostInfo.GetHostname(),
			drainingHostInfoMap[hostInfo.GetHostname()].GetHostname())
		suite.Equal(hostInfo.GetIp(),
			drainingHostInfoMap[hostInfo.GetHostname()].GetIp())
		suite.Equal(host.HostState_HOST_STATE_DRAINING,
			drainingHostInfoMap[hostInfo.GetHostname()].GetState())
	}

	downHostInfoMap := make(map[string]*host.HostInfo)
	for _, hostInfo := range maintenanceHostInfoMap.GetDownHostInfos([]string{}) {
		downHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, hostInfo := range downHostInfos {
		suite.NotNil(downHostInfoMap[hostInfo.GetHostname()])
		suite.Equal(hostInfo.GetHostname(),
			downHostInfoMap[hostInfo.GetHostname()].GetHostname())
		suite.Equal(hostInfo.GetIp(),
			downHostInfoMap[hostInfo.GetHostname()].GetIp())
		suite.Equal(host.HostState_HOST_STATE_DOWN,
			downHostInfoMap[hostInfo.GetHostname()].GetState())
	}

	// Test UpdateHostState
	err := maintenanceHostInfoMap.UpdateHostState(
		drainingHosts[0],
		host.HostState_HOST_STATE_DRAINING,
		host.HostState_HOST_STATE_DOWN)
	suite.NoError(err)
	suite.Empty(
		maintenanceHostInfoMap.GetDrainingHostInfos([]string{drainingHosts[0]}))
	suite.NotEmpty(
		maintenanceHostInfoMap.GetDownHostInfos([]string{drainingHosts[0]}))

	err = maintenanceHostInfoMap.UpdateHostState(
		drainingHosts[0],
		host.HostState_HOST_STATE_DOWN,
		host.HostState_HOST_STATE_DRAINING)
	suite.NoError(err)
	suite.Empty(
		maintenanceHostInfoMap.GetDownHostInfos([]string{drainingHosts[0]}))
	suite.NotEmpty(
		maintenanceHostInfoMap.GetDrainingHostInfos([]string{drainingHosts[0]}))

	// Test UpdateHostState errors
	// Test 'invalid current state' error
	err = maintenanceHostInfoMap.UpdateHostState(
		drainingHosts[0],
		host.HostState_HOST_STATE_DRAINED,
		host.HostState_HOST_STATE_DOWN)
	suite.Error(err)
	// Test 'invalid target state' error
	err = maintenanceHostInfoMap.UpdateHostState(
		drainingHosts[0],
		host.HostState_HOST_STATE_DRAINING,
		host.HostState_HOST_STATE_UP)
	suite.Error(err)
	// Test 'host not in expected state' error
	err = maintenanceHostInfoMap.UpdateHostState(
		"invalidHost",
		host.HostState_HOST_STATE_DRAINING,
		host.HostState_HOST_STATE_DOWN)
	suite.Error(err)
	err = maintenanceHostInfoMap.UpdateHostState(
		"invalidHost",
		host.HostState_HOST_STATE_DOWN,
		host.HostState_HOST_STATE_DRAINING)
	suite.Error(err)

	// Test RemoveHostInfos
	for _, drainingHost := range drainingHosts {
		maintenanceHostInfoMap.RemoveHostInfo(drainingHost)
	}
	suite.Empty(maintenanceHostInfoMap.GetDrainingHostInfos([]string{}))
	suite.NotEmpty(maintenanceHostInfoMap.GetDownHostInfos([]string{}))

	for _, downHost := range downHosts {
		maintenanceHostInfoMap.RemoveHostInfo(downHost)
	}
	suite.Empty(maintenanceHostInfoMap.GetDrainingHostInfos([]string{}))
	suite.Empty(maintenanceHostInfoMap.GetDownHostInfos([]string{}))

	// Test ClearAndFillMap
	for _, drainingHostInfo := range drainingHostInfos {
		maintenanceHostInfoMap.AddHostInfo(drainingHostInfo)
	}
	suite.NotEmpty(maintenanceHostInfoMap.GetDrainingHostInfos(drainingHosts))

	for _, downHostInfo := range downHostInfos {
		maintenanceHostInfoMap.AddHostInfo(downHostInfo)
	}
	suite.NotEmpty(maintenanceHostInfoMap.GetDownHostInfos(downHosts))

	maintenanceHostInfoMap.ClearAndFillMap(drainingHostInfos)
	suite.NotEmpty(maintenanceHostInfoMap.GetDrainingHostInfos([]string{}))
	suite.Empty(maintenanceHostInfoMap.GetDownHostInfos([]string{}))

	maintenanceHostInfoMap.ClearAndFillMap(downHostInfos)
	suite.NotEmpty(maintenanceHostInfoMap.GetDownHostInfos([]string{}))
	suite.Empty(maintenanceHostInfoMap.GetDrainingHostInfos([]string{}))
}

func TestHostMapTestSuite(t *testing.T) {
	suite.Run(t, new(hostMapTestSuite))
}
