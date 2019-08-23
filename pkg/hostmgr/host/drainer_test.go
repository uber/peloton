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
	"context"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	host "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/lifecycle"
	host_mocks "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	mq_mocks "github.com/uber/peloton/pkg/hostmgr/queue/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	drainerPeriod    = 100 * time.Millisecond
	pelotonAgentRole = "peloton"
)

type drainerTestSuite struct {
	suite.Suite
	ctx                      context.Context
	drainer                  *drainer
	mockCtrl                 *gomock.Controller
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockMaintenanceQueue     *mq_mocks.MockMaintenanceQueue
	mockMaintenanceMap       *host_mocks.MockMaintenanceHostInfoMap
	upMachine                *mesos.MachineID
	drainingMachines         []*mesos.MachineID
	downMachines             []*mesos.MachineID
	hostInfos                []*host.HostInfo
}

func (suite *drainerTestSuite) SetupSuite() {
	upHost := "host1"
	upIP := "172.17.0.5"
	suite.upMachine = &mesos.MachineID{
		Hostname: &upHost,
		Ip:       &upIP,
	}
	testDownMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host2",
			ip:   "172.17.0.6",
		},
	}
	for _, test := range testDownMachines {
		suite.downMachines = append(suite.downMachines, &mesos.MachineID{
			Hostname: &test.host,
			Ip:       &test.ip,
		})
	}

	testDrainingMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host3",
			ip:   "172.17.0.7",
		},
	}
	for _, test := range testDrainingMachines {
		suite.drainingMachines = append(
			suite.drainingMachines,
			&mesos.MachineID{
				Hostname: &test.host,
				Ip:       &test.ip,
			})
	}

	for _, drainingMachine := range suite.drainingMachines {
		suite.hostInfos = append(suite.hostInfos,
			&host.HostInfo{
				Hostname: drainingMachine.GetHostname(),
				Ip:       drainingMachine.GetIp(),
				State:    host.HostState_HOST_STATE_DRAINING,
			})
	}

	for _, downMachine := range suite.downMachines {
		suite.hostInfos = append(suite.hostInfos,
			&host.HostInfo{
				Hostname: downMachine.GetHostname(),
				Ip:       downMachine.GetIp(),
				State:    host.HostState_HOST_STATE_DOWN,
			})
	}
}

func (suite *drainerTestSuite) SetupTest() {
	suite.ctx = context.Background()
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = mq_mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.mockMaintenanceMap = host_mocks.NewMockMaintenanceHostInfoMap(suite.mockCtrl)

	suite.drainer = &drainer{
		drainerPeriod:          drainerPeriod,
		pelotonAgentRole:       pelotonAgentRole,
		masterOperatorClient:   suite.mockMasterOperatorClient,
		maintenanceQueue:       suite.mockMaintenanceQueue,
		lifecycle:              lifecycle.NewLifeCycle(),
		maintenanceHostInfoMap: suite.mockMaintenanceMap,
	}

	response := suite.makeAgentsResponse()
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	loader := &Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(gomock.Any()).
		Return([]*host.HostInfo{}).
		Times(1 + len(suite.drainingMachines)) // + 1 for upMachine
	loader.Load(nil)
}

func (suite *drainerTestSuite) makeAgentsResponse() *mesosmaster.Response_GetAgents {
	response := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{},
	}

	pidUp := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.upMachine.GetIp())
	hostnameUp := suite.upMachine.GetHostname()
	agentUp := &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostnameUp,
			Resources: []*mesos.Resource{
				{
					Reservations: []*mesos.Resource_ReservationInfo{
						{
							Role: &[]string{pelotonAgentRole}[0],
						},
					},
				},
			},
		},
		Pid: &pidUp,
	}
	response.Agents = append(response.Agents, agentUp)

	drainingMachine := suite.drainingMachines[0]
	pidDraining := fmt.Sprintf("slave(0)@%s:0.0.0.0", drainingMachine.GetIp())
	hostnameDraining := drainingMachine.GetHostname()
	agentDraining := &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostnameDraining,
		},
		Pid: &pidDraining,
	}
	response.Agents = append(response.Agents, agentDraining)

	return response
}

func (suite *drainerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(drainerTestSuite))
}

//TestNewDrainer test creation of new host drainer
func (suite *drainerTestSuite) TestDrainerNewDrainer() {
	drainer := NewDrainer(
		drainerPeriod,
		pelotonAgentRole,
		suite.mockMasterOperatorClient,
		suite.mockMaintenanceQueue,
		host_mocks.NewMockMaintenanceHostInfoMap(suite.mockCtrl))
	suite.NotNil(drainer)
}

// TestDrainerStartSuccess tests the success case of starting the host drainer
func (suite *drainerTestSuite) TestDrainerStartSuccess() {
	response := mesosmaster.Response_GetMaintenanceStatus{
		Status: &mesosmaintenance.ClusterStatus{
			DrainingMachines: []*mesosmaintenance.ClusterStatus_DrainingMachine{},
			DownMachines:     suite.downMachines,
		},
	}

	var drainingHostnames []string
	for _, drainingMachine := range suite.drainingMachines {
		response.Status.DrainingMachines = append(
			response.Status.DrainingMachines,
			&mesosmaintenance.ClusterStatus_DrainingMachine{
				Id: &mesos.MachineID{
					Hostname: drainingMachine.Hostname,
					Ip:       drainingMachine.Ip,
				},
			})

		drainingHostnames = append(drainingHostnames, drainingMachine.GetHostname())
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)

	suite.mockMaintenanceMap.EXPECT().
		ClearAndFillMap(suite.hostInfos).
		MinTimes(1).
		MaxTimes(2)

	for _, drainingHostname := range drainingHostnames {
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(drainingHostname).
			Return(nil).
			MinTimes(1).
			MaxTimes(2)
	}

	suite.drainer.Start()
	// Starting drainer again should be no-op
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestDrainerStartGetMaintenanceStatusFailure tests the failure case of
// starting the host drainer due to error while getting maintenance status
func (suite *drainerTestSuite) TestDrainerStartGetMaintenanceStatusFailure() {
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenanceStatus error")).
		MinTimes(1).
		MaxTimes(2)

	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestDrainerStartEnqueueFailure tests the failure case of starting the
// host drainer due to error while enqueuing hostnames into maintenance queue
func (suite *drainerTestSuite) TestDrainerStartEnqueueFailure() {
	var drainingHostnames []string
	response := mesosmaster.Response_GetMaintenanceStatus{
		Status: &mesosmaintenance.ClusterStatus{
			DrainingMachines: []*mesosmaintenance.ClusterStatus_DrainingMachine{},
			DownMachines:     suite.downMachines,
		},
	}

	for _, drainingMachine := range suite.drainingMachines {
		response.Status.DrainingMachines = append(
			response.Status.DrainingMachines,
			&mesosmaintenance.ClusterStatus_DrainingMachine{
				Id: &mesos.MachineID{
					Hostname: drainingMachine.Hostname,
					Ip:       drainingMachine.Ip,
				},
			})

		drainingHostnames = append(drainingHostnames,
			drainingMachine.GetHostname())
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)

	suite.mockMaintenanceMap.EXPECT().
		ClearAndFillMap(suite.hostInfos).
		MinTimes(1).
		MaxTimes(2)

	for _, drainingHostname := range drainingHostnames {
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(drainingHostname).
			Return(fmt.Errorf("Fake Enqueue error")).
			MinTimes(1).
			MaxTimes(2)
	}

	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestStop tests stopping the host drainer
func (suite *drainerTestSuite) TestStop() {
	suite.drainer.Stop()
	<-suite.drainer.lifecycle.StopCh()
}

func (suite *drainerTestSuite) TestStartMaintenance() {
	hostname := suite.upMachine.GetHostname()
	hostInfo := &host.HostInfo{
		Hostname: hostname,
		Ip:       suite.upMachine.GetIp(),
		State:    host.HostState_HOST_STATE_DRAINING,
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

	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.NoError(err)
}

// TestStartMaintenanceGetMaintenanceScheduleError tests the failure case of
// starting maintenance due to error while getting maintenance schedule
func (suite *drainerTestSuite) TestStartMaintenanceGetMaintenanceScheduleError() {
	hostname := suite.upMachine.GetHostname()

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(nil, fmt.Errorf("fake GetMaintenanceSchedule error"))
	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

// TestStartMaintenancePostMaintenanceScheduleError tests the failure case of
// starting maintenance due to error while posting maintenance schedule
func (suite *drainerTestSuite) TestStartMaintenancePostMaintenanceScheduleError() {
	hostname := suite.upMachine.GetHostname()

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesosmaster.Response_GetMaintenanceSchedule{
			Schedule: &mesosmaintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(fmt.Errorf("fake UpdateMaintenanceSchedule error"))
	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

// TestStartMaintenanceEnqueueError tests the failure case of starting
// maintenance due to error while enqueuing to maintenance queue
func (suite *drainerTestSuite) TestStartMaintenanceEnqueueError() {
	hostname := suite.upMachine.GetHostname()
	hostInfo := &host.HostInfo{
		Hostname: hostname,
		Ip:       suite.upMachine.GetIp(),
		State:    host.HostState_HOST_STATE_DRAINING,
	}

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
	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

// TestStartMaintenanceUnknownHost tests the failure case of starting
// maintenance on an unknown host
func (suite *drainerTestSuite) TestStartMaintenanceUnknownHost() {
	suite.Error(suite.drainer.StartMaintenance(suite.ctx, "invalid"))
}

// TestStartMaintenanceUnknownHost tests the failure case of starting
// maintenance due to error while parsing mesos agent pid
func (suite *drainerTestSuite) TestStartMaintenancePidParseError() {
	hostname := suite.upMachine.GetHostname()
	pid := "invalidPID"
	GetAgentMap().RegisteredAgents[hostname].Pid = &pid
	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

// TestStartMaintenanceNonPelotonAgentError tests the failure case of starting
// maintenance when the host is not registered as a Peloton agent
func (suite *drainerTestSuite) TestStartMaintenanceNonPelotonAgentError() {
	hostname := suite.upMachine.GetHostname()
	loader := &Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}

	suite.mockMasterOperatorClient.EXPECT().Agents().Return(&mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{
			{
				AgentInfo: &mesos.AgentInfo{
					Hostname: &hostname,
					Resources: []*mesos.Resource{
						{
							Reservations: []*mesos.Resource_ReservationInfo{
								{
									Role: &[]string{"*"}[0],
								},
							},
						},
					},
				},
			},
		},
	}, nil)

	suite.mockMaintenanceMap.EXPECT().GetDrainingHostInfos(gomock.Any()).Return(nil)
	loader.Load(nil)
	err := suite.drainer.StartMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

func (suite *drainerTestSuite) TestCompleteMaintenance() {
	downMachine := suite.downMachines[0]
	hostname := downMachine.GetHostname()
	hostInfo := &host.HostInfo{
		Hostname: hostname,
		Ip:       downMachine.GetIp(),
		State:    host.HostState_HOST_STATE_DOWN,
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*host.HostInfo{hostInfo})
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance([]*mesos.MachineID{downMachine}).Return(nil)
	suite.mockMaintenanceMap.EXPECT().
		RemoveHostInfo(hostname)

	err := suite.drainer.CompleteMaintenance(suite.ctx, hostname)
	suite.NoError(err)
}

// TestCompleteMaintenanceMesosMasterCallFail tests the failure case of
// completing maintenance on a host due to error while posting to Mesos Master
func (suite *drainerTestSuite) TestCompleteMaintenanceMesosMasterCallFail() {
	downMachine := suite.downMachines[0]

	hostname := downMachine.GetHostname()
	hostInfo := &host.HostInfo{
		Hostname: hostname,
		Ip:       downMachine.GetIp(),
		State:    host.HostState_HOST_STATE_DOWN,
	}

	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*host.HostInfo{hostInfo})
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance([]*mesos.MachineID{downMachine}).
		Return(fmt.Errorf("fake StopMaintenance error"))

	err := suite.drainer.CompleteMaintenance(suite.ctx, hostname)
	suite.Error(err)
}

// TestCompleteMaintenanceHostNotDownError tests the failure case of
// completing maintenance on a host which is not in DOWN state
func (suite *drainerTestSuite) TestCompleteMaintenanceHostNotDownError() {
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return([]*host.HostInfo{})
	err := suite.drainer.CompleteMaintenance(suite.ctx, "anyhostname")
	suite.Error(err)
}

func (suite *drainerTestSuite) TestGetDownHostInfos() {
	downHostsInfos := make([]*host.HostInfo, 0)
	for _, machine := range suite.downMachines {
		downHostsInfos = append(downHostsInfos, &host.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    host.HostState_HOST_STATE_DOWN,
		})
	}
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos([]string{}).
		Return(downHostsInfos)

	resp := suite.drainer.GetDownHostInfos([]string{})
	suite.EqualValues(downHostsInfos, resp)

	filter := []string{suite.downMachines[0].GetHostname()}
	suite.mockMaintenanceMap.EXPECT().
		GetDownHostInfos(filter).
		Return(downHostsInfos[0:1])

	resp = suite.drainer.GetDownHostInfos(filter)
	suite.EqualValues(downHostsInfos[0:1], resp)
}

func (suite *drainerTestSuite) TestGetDrainingHostInfos() {
	drainingHostsInfos := make([]*host.HostInfo, 0)
	for _, machine := range suite.drainingMachines {
		drainingHostsInfos = append(drainingHostsInfos, &host.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    host.HostState_HOST_STATE_DOWN,
		})
	}
	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos([]string{}).
		Return(drainingHostsInfos)

	resp := suite.drainer.GetDrainingHostInfos([]string{})
	suite.EqualValues(drainingHostsInfos, resp)

	filter := []string{suite.downMachines[0].GetHostname()}
	suite.mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(filter).
		Return(drainingHostsInfos[0:1])

	resp = suite.drainer.GetDrainingHostInfos(filter)
	suite.EqualValues(drainingHostsInfos[0:1], resp)
}

// TestIsPelotonAgent tests isPelotonAgent
func (suite *drainerTestSuite) TestIsPelotonAgent() {
	loader := &Loader{
		OperatorClient:         suite.mockMasterOperatorClient,
		Scope:                  tally.NoopScope,
		MaintenanceHostInfoMap: NewMaintenanceHostInfoMap(tally.NoopScope),
	}

	// Mock 1 host `id-0` as an non-peloton agent
	agentsResponse := makeAgentsResponse(2)
	for _, r := range agentsResponse.Agents[0].GetAgentInfo().GetResources() {
		r.Reservations[0].Role = &[]string{"*"}[0]
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(agentsResponse, nil)
	loader.Load(nil)
	suite.False(suite.drainer.isPelotonAgent("id-0"))
	suite.True(suite.drainer.isPelotonAgent("id-1"))
}
