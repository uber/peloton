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

package hostsvc

import (
	"context"
	"fmt"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	svcpb "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"

	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/hostmgr/host"
	hm "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	ym "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	qm "github.com/uber/peloton/pkg/hostmgr/queue/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type HostSvcHandlerTestSuite struct {
	suite.Suite

	ctx                      context.Context
	upMachine                *mesos.MachineID
	upMachines               []*mesos.MachineID
	downMachine              *mesos.MachineID
	downMachines             []*mesos.MachineID
	drainingMachine          *mesos.MachineID
	drainingMachines         []*mesos.MachineID
	mockCtrl                 *gomock.Controller
	handler                  *serviceHandler
	mockMasterOperatorClient *ym.MockMasterOperatorClient
	mockMaintenanceQueue     *qm.MockMaintenanceQueue
	mockMaintenanceMap       *hm.MockMaintenanceHostInfoMap
}

func (suite *HostSvcHandlerTestSuite) SetupSuite() {
	suite.handler = &serviceHandler{
		metrics: NewMetrics(tally.NoopScope),
	}

	upHost := "host1"
	upIP := "172.17.0.5"
	suite.upMachine = &mesos.MachineID{
		Hostname: &upHost,
		Ip:       &upIP,
	}
	suite.upMachines = append(suite.upMachines, suite.upMachine)

	downHost := "host2"
	downIP := "172.17.0.6"
	suite.downMachine = &mesos.MachineID{
		Hostname: &downHost,
		Ip:       &downIP,
	}
	suite.downMachines = append(suite.downMachines, suite.downMachine)

	drainingHost := "host3"
	drainingIP := "172.17.0.7"
	suite.drainingMachine = &mesos.MachineID{
		Hostname: &drainingHost,
		Ip:       &drainingIP,
	}
	suite.drainingMachines = append(suite.drainingMachines, suite.drainingMachine)
}

func (suite *HostSvcHandlerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockMasterOperatorClient = ym.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = qm.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.mockMaintenanceMap = hm.NewMockMaintenanceHostInfoMap(suite.mockCtrl)
	suite.handler.operatorMasterClient = suite.mockMasterOperatorClient
	suite.handler.maintenanceQueue = suite.mockMaintenanceQueue
	suite.handler.maintenanceHostInfoMap = suite.mockMaintenanceMap

	response := suite.makeAgentsResponse()
	loader := &host.Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(gomock.Any()).
		Return([]*hpb.HostInfo{}).
		Times(len(suite.upMachines) + len(suite.drainingMachines))
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	loader.Load(nil)
}

func (suite *HostSvcHandlerTestSuite) makeAgentsResponse() *mesosmaster.Response_GetAgents {
	response := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{},
	}

	pidUp := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.upMachine.GetIp())
	hostnameUp := suite.upMachine.GetHostname()
	agentUp := &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostnameUp,
		},
		Pid: &pidUp,
	}
	response.Agents = append(response.Agents, agentUp)

	pidDraining := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.drainingMachine.GetIp())
	hostnameDraining := suite.drainingMachine.GetHostname()
	agentDraining := &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostnameDraining,
		},
		Pid: &pidDraining,
	}
	response.Agents = append(response.Agents, agentDraining)

	return response
}

func TestHostSvcHandler(t *testing.T) {
	suite.Run(t, new(HostSvcHandlerTestSuite))
}

func (suite *HostSvcHandlerTestSuite) TearDownTest() {
	log.Info("tearing down HostSvcHandlerTestSuite")
	suite.mockCtrl.Finish()
}

func (suite *HostSvcHandlerTestSuite) TestStartMaintenance() {
	hostname := suite.upMachine.GetHostname()
	hostInfo := &hpb.HostInfo{
		Hostname: hostname,
		Ip:       suite.upMachine.GetIp(),
		State:    hpb.HostState_HOST_STATE_DRAINING,
	}

	gomock.InOrder(
		suite.mockMasterOperatorClient.EXPECT().GetMaintenanceSchedule().
			Return(&mesosmaster.Response_GetMaintenanceSchedule{
				Schedule: &mesosmaintenance.Schedule{},
			}, nil),
		suite.mockMasterOperatorClient.EXPECT().
			UpdateMaintenanceSchedule(gomock.Any()).Return(nil),
		suite.mockMaintenanceMap.EXPECT().
			AddHostInfo(hostInfo),
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(hostname).Return(nil),
	)

	resp, err := suite.handler.StartMaintenance(
		suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		})
	suite.NoError(err)
	suite.Equal(&svcpb.StartMaintenanceResponse{
		Hostname: hostname,
	}, resp)
}

func (suite *HostSvcHandlerTestSuite) TestStartMaintenanceError() {
	hostname := suite.upMachine.GetHostname()
	hostInfo := &hpb.HostInfo{
		Hostname: hostname,
		Ip:       suite.upMachine.GetIp(),
		State:    hpb.HostState_HOST_STATE_DRAINING,
	}

	// Test error while getting maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(nil, fmt.Errorf("fake GetMaintenanceSchedule error"))
	response, err := suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		})
	suite.Error(err)
	suite.Nil(response)

	// Test error while posting maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesosmaster.Response_GetMaintenanceSchedule{
			Schedule: &mesosmaintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(fmt.Errorf("fake UpdateMaintenanceSchedule error"))
	response, err = suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		})
	suite.Error(err)
	suite.Nil(response)

	// Test error while enqueuing in maintenance queue
	gomock.InOrder(
		suite.mockMasterOperatorClient.EXPECT().
			GetMaintenanceSchedule().
			Return(&mesosmaster.Response_GetMaintenanceSchedule{
				Schedule: &mesosmaintenance.Schedule{},
			}, nil),
		suite.mockMasterOperatorClient.EXPECT().
			UpdateMaintenanceSchedule(gomock.Any()).Return(nil),
		suite.mockMaintenanceMap.EXPECT().
			AddHostInfo(hostInfo),
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(hostname).Return(fmt.Errorf("fake Enqueue error")),
	)
	response, err = suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		})
	suite.Error(err)
	suite.Nil(response)

	// Test Unknown host error
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostname: "invalid",
	})
	suite.Error(err)
	suite.Nil(response)

	// TestExtractIPFromMesosAgentPID error
	pid := "invalidPID"
	host.GetAgentMap().RegisteredAgents[hostname].Pid = &pid
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostname: hostname,
	})
	suite.Error(err)
	suite.Nil(response)

	// Test 'No registered agents' error
	loader := &host.Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: hm.NewMockMaintenanceHostInfoMap(suite.mockCtrl),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	loader.Load(nil)
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostname: hostname,
	})
	suite.Error(err)
	suite.Nil(response)
}

func (suite *HostSvcHandlerTestSuite) TestCompleteMaintenance() {
	hostname := suite.downMachine.GetHostname()
	hostInfo := &hpb.HostInfo{
		Hostname: hostname,
		Ip:       suite.downMachine.GetIp(),
		State:    hpb.HostState_HOST_STATE_DOWN,
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*hpb.HostInfo{hostInfo})
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance([]*mesos.MachineID{suite.downMachine}).Return(nil)
	suite.mockMaintenanceMap.EXPECT().
		RemoveHostInfo(hostname)

	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostname: hostname,
		})
	suite.NoError(err)
	suite.Equal(&svcpb.CompleteMaintenanceResponse{
		Hostname: hostname,
	}, resp)
}

func (suite *HostSvcHandlerTestSuite) TestCompleteMaintenanceError() {
	// Test error while stopping maintenance
	hostname := suite.downMachine.GetHostname()
	hostInfo := &hpb.HostInfo{
		Hostname: hostname,
		Ip:       suite.downMachine.GetIp(),
		State:    hpb.HostState_HOST_STATE_DOWN,
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*hpb.HostInfo{hostInfo})
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance([]*mesos.MachineID{suite.downMachine}).
		Return(fmt.Errorf("fake StopMaintenance error"))

	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostname: hostname,
		})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'Host not down' error
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*hpb.HostInfo{})
	resp, err = suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostname: "anyhostname",
		})
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *HostSvcHandlerTestSuite) TestQueryHosts() {
	var (
		hostInfos         []*hpb.HostInfo
		drainingHostInfos []*hpb.HostInfo
		downHostsInfos    []*hpb.HostInfo
	)

	for _, machine := range suite.downMachines {
		downHostsInfos = append(downHostsInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINING,
		})
		hostInfos = append(hostInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINING,
		})
	}

	for _, machine := range suite.drainingMachines {
		drainingHostInfos = append(drainingHostInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINING,
		})
		hostInfos = append(hostInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINING,
		})
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos([]string{}).
		Return(drainingHostInfos).
		AnyTimes()
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return(downHostsInfos).
		AnyTimes()
	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
			hpb.HostState_HOST_STATE_DRAINING,
			hpb.HostState_HOST_STATE_DOWN,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		len(suite.upMachines)+
			len(suite.drainingMachines)+
			len(suite.downMachines),
		len(resp.GetHostInfos()),
	)

	hostnameSet := stringset.New()
	for _, hostInfo := range resp.GetHostInfos() {
		hostnameSet.Add(hostInfo.GetHostname())
	}
	for _, upMachine := range suite.upMachines {
		suite.True(hostnameSet.Contains(upMachine.GetHostname()))
	}
	for _, drainingMachine := range suite.drainingMachines {
		suite.True(hostnameSet.Contains(drainingMachine.GetHostname()))
	}
	for _, downMachine := range suite.downMachines {
		suite.True(hostnameSet.Contains(downMachine.GetHostname()))
	}

	// Test querying only draining hosts
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_DRAINING,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(len(suite.drainingMachines), len(resp.GetHostInfos()))
	for i, drainingMachine := range suite.drainingMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), drainingMachine.GetHostname())
	}

	// Empty QueryHostsRequest should return hosts in all states
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		len(suite.upMachines)+
			len(suite.drainingMachines)+
			len(suite.downMachines),
		len(resp.GetHostInfos()),
	)

	hostnameSet.Clear()
	for _, hostInfo := range resp.GetHostInfos() {
		hostnameSet.Add(hostInfo.GetHostname())
	}
	for _, upMachine := range suite.upMachines {
		suite.True(hostnameSet.Contains(upMachine.GetHostname()))
	}
	for _, drainingMachine := range suite.drainingMachines {
		suite.True(hostnameSet.Contains(drainingMachine.GetHostname()))
	}
	for _, downMachine := range suite.downMachines {
		suite.True(hostnameSet.Contains(downMachine.GetHostname()))
	}
}

func (suite *HostSvcHandlerTestSuite) TestQueryHostsError() {
	// Test ExtractIPFromMesosAgentPID error
	hostname := "testhost"
	pid := "invalidPID"
	host.GetAgentMap().RegisteredAgents[hostname] = &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostname,
		},
		Pid: &pid,
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(gomock.Any()).
		Return([]*hpb.HostInfo{})
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos(gomock.Any()).
		Return([]*hpb.HostInfo{})

	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'No registered agents'
	loader := &host.Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}

	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	loader.Load(nil)

	gomock.InOrder(
		suite.mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}),
		suite.mockMaintenanceMap.EXPECT().
			GetDownHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}),
	)

	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
}
