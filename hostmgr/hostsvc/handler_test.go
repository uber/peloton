package hostsvc

import (
	"context"
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesosmaintenance "code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	mesosmaster "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	hpb "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"code.uber.internal/infra/peloton/common/stringset"
	"code.uber.internal/infra/peloton/hostmgr/host"
	qm "code.uber.internal/infra/peloton/hostmgr/queue/mocks"
	ym "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type HostSvcHandlerTestSuite struct {
	suite.Suite

	ctx                      context.Context
	upMachines               []*mesos.MachineID
	downMachines             []*mesos.MachineID
	drainingMachines         []*mesos.MachineID
	hostsToDown              []string
	mockCtrl                 *gomock.Controller
	handler                  *serviceHandler
	mockMasterOperatorClient *ym.MockMasterOperatorClient
	mockMaintenanceQueue     *qm.MockMaintenanceQueue
}

func (suite *HostSvcHandlerTestSuite) SetupSuite() {
	suite.handler = &serviceHandler{
		metrics: NewMetrics(tally.NoopScope),
	}
	testUpMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host1",
			ip:   "172.17.0.5",
		},
	}
	for _, test := range testUpMachines {
		suite.upMachines = append(suite.upMachines, &mesos.MachineID{
			Hostname: &test.host,
			Ip:       &test.ip,
		})
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
	for _, machine := range suite.downMachines {
		suite.hostsToDown = append(suite.hostsToDown, machine.GetHostname())
	}
}

func (suite *HostSvcHandlerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockMasterOperatorClient = ym.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = qm.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.handler.operatorMasterClient = suite.mockMasterOperatorClient
	suite.handler.maintenanceQueue = suite.mockMaintenanceQueue
	suite.handler.maintenanceHostInfoMap = host.NewMaintenanceHostInfoMap()

	response := suite.makeAgentsResponse()
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: host.NewMaintenanceHostInfoMap(),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	loader.Load(nil)
}

func (suite *HostSvcHandlerTestSuite) makeAgentsResponse() *mesosmaster.Response_GetAgents {
	response := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{},
	}
	for i, upMachine := range suite.upMachines {
		pid := fmt.Sprintf("slave(%d)@%s:0.0.0.0", i, upMachine.GetIp())
		agent := &mesosmaster.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Hostname: upMachine.Hostname,
			},
			Pid: &pid,
		}
		response.Agents = append(response.Agents, agent)
	}
	for i, drainingMachine := range suite.drainingMachines {
		pid := fmt.Sprintf("slave(%d)@%s:0.0.0.0", i, drainingMachine.GetIp())
		agent := &mesosmaster.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Hostname: drainingMachine.Hostname,
			},
			Pid: &pid,
		}
		response.Agents = append(response.Agents, agent)
	}
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
	var hosts []string
	for _, machine := range suite.upMachines {
		hosts = append(hosts, machine.GetHostname())
	}

	gomock.InOrder(
		suite.mockMasterOperatorClient.EXPECT().GetMaintenanceSchedule().
			Return(&mesosmaster.Response_GetMaintenanceSchedule{
				Schedule: &mesosmaintenance.Schedule{},
			}, nil),
		suite.mockMasterOperatorClient.EXPECT().
			UpdateMaintenanceSchedule(gomock.Any()).Return(nil),
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(hosts).Return(nil),
	)

	_, err := suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostnames: hosts,
		})
	suite.NoError(err)
}

func (suite *HostSvcHandlerTestSuite) TestStartMaintenanceError() {
	var hosts []string
	for _, machine := range suite.upMachines {
		hosts = append(hosts, machine.GetHostname())
	}
	// Test error while getting maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceSchedule().
		Return(nil, fmt.Errorf("fake GetMaintenanceSchedule error"))
	response, err := suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostnames: hosts,
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
			Hostnames: hosts,
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
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(hosts).Return(fmt.Errorf("fake Enqueue error")),
	)
	response, err = suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostnames: hosts,
		})
	suite.Error(err)
	suite.Nil(response)

	// Test Unknown host error
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostnames: []string{"invalid"},
	})
	suite.Error(err)
	suite.Nil(response)

	// TestExtractIPFromMesosAgentPID error
	pid := "invalidPID"
	host.GetAgentMap().RegisteredAgents[hosts[0]].Pid = &pid
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostnames: hosts,
	})
	suite.Error(err)
	suite.Nil(response)

	// Test 'No registered agents' error
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: host.NewMaintenanceHostInfoMap(),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	loader.Load(nil)
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostnames: hosts,
	})
	suite.Error(err)
	suite.Nil(response)
}

func (suite *HostSvcHandlerTestSuite) TestCompleteMaintenance() {
	var hostInfos []*hpb.HostInfo
	for _, machine := range suite.downMachines {
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: machine.GetHostname(),
				Ip:       machine.GetIp(),
				State:    hpb.HostState_HOST_STATE_DOWN,
			})
	}
	suite.handler.maintenanceHostInfoMap.AddHostInfos(hostInfos)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(suite.downMachines).Return(nil)
	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.NoError(err)
	suite.NotNil(resp)
}

func (suite *HostSvcHandlerTestSuite) TestCompleteMaintenanceError() {
	// Test error while stopping maintenance
	var hostInfos []*hpb.HostInfo
	for _, machine := range suite.downMachines {
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: machine.GetHostname(),
				Ip:       machine.GetIp(),
				State:    hpb.HostState_HOST_STATE_DOWN,
			})
	}
	suite.handler.maintenanceHostInfoMap.AddHostInfos(hostInfos)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(suite.downMachines).
		Return(fmt.Errorf("fake StopMaintenance error"))
	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'Host not down' error
	var hostnames []string
	for _, hostInfo := range hostInfos {
		hostnames = append(hostnames, hostInfo.GetHostname())
	}
	suite.handler.maintenanceHostInfoMap.
		RemoveHostInfos(hostnames)
	resp, err = suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *HostSvcHandlerTestSuite) TestQueryHosts() {
	var hostInfos []*hpb.HostInfo
	for _, machine := range suite.downMachines {
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: machine.GetHostname(),
				Ip:       machine.GetIp(),
				State:    hpb.HostState_HOST_STATE_DOWN,
			})
	}
	for _, machine := range suite.drainingMachines {
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: machine.GetHostname(),
				Ip:       machine.GetIp(),
				State:    hpb.HostState_HOST_STATE_DRAINING,
			})
	}

	suite.handler.maintenanceHostInfoMap.AddHostInfos(hostInfos)
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
	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'No registered agents'
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: host.NewMaintenanceHostInfoMap(),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	loader.Load(nil)
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
}
