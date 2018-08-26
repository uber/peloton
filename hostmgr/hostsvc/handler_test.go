package hostsvc

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	"code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	hm_host "code.uber.internal/infra/peloton/hostmgr/host"
	qm "code.uber.internal/infra/peloton/hostmgr/queue/mocks"
	ym "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type HostSvcHandlerTestSuite struct {
	suite.Suite

	ctx                      context.Context
	upMachines               []*mesos_v1.MachineID
	downMachines             []*mesos_v1.MachineID
	drainingMachines         []*mesos_v1.MachineID
	hostsToDown              []string
	mockCtrl                 *gomock.Controller
	handler                  *serviceHandler
	mockMasterOperatorClient *ym.MockMasterOperatorClient
	mockMaintenanceQueue     *qm.MockMaintenanceQueue
	mockDispatcher           *yarpc.Dispatcher
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
		suite.upMachines = append(suite.upMachines, &mesos_v1.MachineID{
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
		suite.downMachines = append(suite.downMachines, &mesos_v1.MachineID{
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
			&mesos_v1.MachineID{
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

	response := suite.makeAgentsResponse()
	loader := &hm_host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	loader.Load(nil)
}

func (suite *HostSvcHandlerTestSuite) makeAgentsResponse() *mesos_v1_master.Response_GetAgents {
	response := &mesos_v1_master.Response_GetAgents{
		Agents: []*mesos_v1_master.Response_GetAgents_Agent{},
	}
	for i, upMachine := range suite.upMachines {
		pid := fmt.Sprintf("slave(%d)@%s:0.0.0.0", i, upMachine.GetIp())
		agent := &mesos_v1_master.Response_GetAgents_Agent{
			AgentInfo: &mesos_v1.AgentInfo{
				Hostname: upMachine.Hostname,
			},
			Pid: &pid,
		}
		response.Agents = append(response.Agents, agent)
	}
	for i, drainingMachine := range suite.drainingMachines {
		pid := fmt.Sprintf("slave(%d)@%s:0.0.0.0", i, drainingMachine.GetIp())
		agent := &mesos_v1_master.Response_GetAgents_Agent{
			AgentInfo: &mesos_v1.AgentInfo{
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
			Return(&mesos_v1_master.Response_GetMaintenanceSchedule{
				Schedule: &mesos_v1_maintenance.Schedule{},
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
		Return(&mesos_v1_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_v1_maintenance.Schedule{},
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
			Return(&mesos_v1_master.Response_GetMaintenanceSchedule{
				Schedule: &mesos_v1_maintenance.Schedule{},
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
	hm_host.GetAgentMap().RegisteredAgents[hosts[0]].Pid = &pid
	response, err = suite.handler.StartMaintenance(suite.ctx, &svcpb.StartMaintenanceRequest{
		Hostnames: hosts,
	})
	suite.Error(err)
	suite.Nil(response)

	// Test 'No registered agents' error
	loader := &hm_host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
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
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_v1_master.Response_GetMaintenanceStatus{
			Status: &mesos_v1_maintenance.ClusterStatus{
				DownMachines: suite.downMachines,
			},
		}, nil)
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
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_v1_master.Response_GetMaintenanceStatus{
			Status: &mesos_v1_maintenance.ClusterStatus{
				DownMachines: suite.downMachines,
			},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(suite.downMachines).
		Return(fmt.Errorf("fake StopMaintenance error"))
	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.Error(err)
	suite.Nil(resp)

	// Test error while getting maintenance status
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenanceStatus error"))
	resp, err = suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'Host not down' error
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_v1_master.Response_GetMaintenanceStatus{}, nil)
	resp, err = suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostnames: suite.hostsToDown,
		})
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *HostSvcHandlerTestSuite) TestQueryHosts() {
	clusterDrainingMachine := mesos_v1_maintenance.ClusterStatus_DrainingMachine{
		Id: suite.drainingMachines[0],
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_v1_master.Response_GetMaintenanceStatus{
			Status: &mesos_v1_maintenance.ClusterStatus{
				DrainingMachines: []*mesos_v1_maintenance.
					ClusterStatus_DrainingMachine{
					&clusterDrainingMachine,
				},
				DownMachines: suite.downMachines,
			},
		}, nil).Times(2)

	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []host.HostState{
			host.HostState_HOST_STATE_UP,
			host.HostState_HOST_STATE_DRAINING,
			host.HostState_HOST_STATE_DOWN,
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

	i := 0
	for _, upMachine := range suite.upMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), upMachine.GetHostname())
		i++
	}
	for _, drainingMachine := range suite.drainingMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), drainingMachine.GetHostname())
		i++
	}
	for _, downMachine := range suite.downMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), downMachine.GetHostname())
		i++
	}
	suite.Equal(i, len(resp.HostInfos))

	// Test querying only draining hosts
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []host.HostState{
			host.HostState_HOST_STATE_DRAINING,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(len(suite.drainingMachines), len(resp.GetHostInfos()))
	for i, drainingMachine := range suite.drainingMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), drainingMachine.GetHostname())
	}
}

func (suite *HostSvcHandlerTestSuite) TestQueryHostsError() {
	// Test error while getting maintenance status
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().
		Return(nil, fmt.Errorf("fake GetMaintenanceStatus error"))
	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{})
	suite.Error(err)
	suite.Nil(resp)

	// Test ExtractIPFromMesosAgentPID error
	hostname := "testhost"
	pid := "invalidPID"
	hm_host.GetAgentMap().RegisteredAgents[hostname] = &mesos_v1_master.Response_GetAgents_Agent{
		AgentInfo: &mesos_v1.AgentInfo{
			Hostname: &hostname,
		},
		Pid: &pid,
	}
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []host.HostState{
			host.HostState_HOST_STATE_UP,
		},
	})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'No registered agents'
	loader := &hm_host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
	}
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	loader.Load(nil)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_v1_master.Response_GetMaintenanceStatus{}, nil)

	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []host.HostState{
			host.HostState_HOST_STATE_UP,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
}
