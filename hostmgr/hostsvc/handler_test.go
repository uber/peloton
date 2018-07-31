package hostsvc

import (
	"context"
	"fmt"
	"testing"

	maintenance_mocks "code.uber.internal/infra/peloton/hostmgr/queue/mocks"
	yarpc_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	"code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type HostSvcHandlerTestSuite struct {
	suite.Suite

	ctx                      context.Context
	downMachines             []*mesos_v1.MachineID
	drainingMachines         []*mesos_v1.MachineID
	mockCtrl                 *gomock.Controller
	handler                  *serviceHandler
	mockMasterOperatorClient *yarpc_mocks.MockMasterOperatorClient
	mockMaintenanceQueue     *maintenance_mocks.MockMaintenanceQueue
	mockDispatcher           *yarpc.Dispatcher
}

func (suite *HostSvcHandlerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockMasterOperatorClient = yarpc_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = maintenance_mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.handler = &serviceHandler{
		operatorMasterClient: suite.mockMasterOperatorClient,
		maintenanceQueue:     suite.mockMaintenanceQueue,
		metrics:              NewMetrics(tally.NoopScope),
	}
	testDownMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host1",
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
			host: "host1",
			ip:   "172.17.0.6",
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
}

func TestHostSvcHandler(t *testing.T) {
	suite.Run(t, new(HostSvcHandlerTestSuite))
}

func (suite *HostSvcHandlerTestSuite) TearDownTest() {
	log.Info("tearing down HostSvcHandlerTestSuite")
}

func (suite *HostSvcHandlerTestSuite) TestStartMaintenance() {
	var hostsToDown []string
	for _, machine := range suite.downMachines {
		hostsToDown = append(hostsToDown, machine.GetHostname())
	}
	gomock.InOrder(
		suite.mockMasterOperatorClient.EXPECT().GetMaintenanceSchedule().
			Return(&mesos_v1_master.Response_GetMaintenanceSchedule{
				Schedule: &mesos_v1_maintenance.Schedule{},
			}, nil),
		suite.mockMasterOperatorClient.EXPECT().
			UpdateMaintenanceSchedule(gomock.Any()).Return(nil),
		suite.mockMaintenanceQueue.EXPECT().
			Enqueue(hostsToDown).Return(nil),
	)
	_, err := suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			MachineIds: suite.downMachines,
		})
	suite.NoError(err)

	// Test error while getting maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceSchedule().
		Return(nil, fmt.Errorf("fake GetMaintenanceSchedule error"))
	response, err := suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			MachineIds: suite.downMachines,
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
			MachineIds: suite.downMachines,
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
			Enqueue(hostsToDown).Return(fmt.Errorf("fake Enqueue error")),
	)
	response, err = suite.handler.StartMaintenance(suite.ctx,
		&svcpb.StartMaintenanceRequest{
			MachineIds: suite.downMachines,
		})
	suite.Error(err)
	suite.Nil(response)
}

func (suite *HostSvcHandlerTestSuite) TestCompleteMaintenance() {
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(suite.downMachines).Return(nil)
	resp, err := suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			MachineIds: suite.downMachines,
		})
	suite.NoError(err)
	suite.NotNil(resp)

	// Test error while stopping maintenance
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(suite.downMachines).
		Return(fmt.Errorf("fake StopMaintenance error"))
	resp, err = suite.handler.CompleteMaintenance(suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			MachineIds: suite.downMachines,
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
			host.HostState_HOST_STATE_DRAINING,
			host.HostState_HOST_STATE_DOWN,
			host.HostState_HOST_STATE_UP,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		len(suite.drainingMachines)+len(suite.downMachines),
		len(resp.GetHostInfos()),
	)

	i := 0
	for _, drainingMachine := range suite.drainingMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), drainingMachine.GetHostname())
		i++
	}
	for _, downMachine := range suite.downMachines {
		suite.Equal(resp.HostInfos[i].GetHostname(), downMachine.GetHostname())
		i++
	}

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

	// Test error while getting maintenance status
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().
		Return(nil, fmt.Errorf("fake GetMaintenanceStatus error"))
	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{})
	suite.Error(err)
	suite.Nil(resp)
}
