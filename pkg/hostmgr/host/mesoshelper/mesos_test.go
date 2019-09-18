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

package mesoshelper

import (
	"errors"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/host"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type mesosHelperTestSuite struct {
	suite.Suite
	mockCtrl                 *gomock.Controller
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockHostInfoOps          *orm_mocks.MockHostInfoOps
	hostname                 string
	IP                       string
}

func (suite *mesosHelperTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.mockCtrl)
	suite.hostname = "hostname"
	suite.IP = "IP"
}

func (suite *mesosHelperTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestMesosHelper(t *testing.T) {
	suite.Run(t, new(mesosHelperTestSuite))
}

// TestAddHostToMaintenanceSchedule tests AddHostToMaintenanceSchedule
func (suite *mesosHelperTestSuite) TestAddHostToMaintenanceSchedule() {
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_maintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(nil)

	suite.NoError(AddHostToMaintenanceSchedule(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestAddHostToMaintenanceScheduleNoop tests AddHostToMaintenanceSchedule
// for a host already in DRAINING state
func (suite *mesosHelperTestSuite) TestAddHostToMaintenanceScheduleNoop() {
	// Host is already part of the maintenance schedule, noop action
	nanos := int64(time.Now().Nanosecond())
	schedule := &mesos_maintenance.Schedule{
		Windows: []*mesos_maintenance.Window{
			{
				MachineIds: []*mesos.MachineID{
					{
						Hostname: &suite.hostname,
						Ip:       &suite.IP,
					},
				},
				Unavailability: &mesos.Unavailability{
					Start: &mesos.TimeInfo{
						Nanoseconds: &nanos,
					},
				},
			},
		},
	}
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: schedule,
		}, nil)

	suite.NoError(AddHostToMaintenanceSchedule(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))

}

// TestAddHostToMaintenanceScheduleFailure tests AddHostToMaintenanceSchedule
// with failures
func (suite *mesosHelperTestSuite) TestAddHostToMaintenanceScheduleFailure() {
	// Failure to fetch maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(nil, errors.New("some error"))

	suite.Error(AddHostToMaintenanceSchedule(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))

	// Failure to post new maintenance schedule
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_maintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(AddHostToMaintenanceSchedule(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsDown tests RegisterHostAsDown
func (suite *mesosHelperTestSuite) TestRegisterHostAsDown() {
	// Host is part of the draining machines in the cluster status
	clusterStatusAsDraining := &mesos_maintenance.ClusterStatus{
		DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{
			{
				Id: &mesos.MachineID{
					Hostname: &suite.hostname,
					Ip:       &suite.IP,
				},
			},
		},
		DownMachines: []*mesos.MachineID{},
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_master.Response_GetMaintenanceStatus{
			Status: clusterStatusAsDraining,
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		StartMaintenance(gomock.Any()).
		Return(nil)

	suite.NoError(RegisterHostAsDown(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsDownNoop tests RegisterHostAsDown
// for a host already DOWN
func (suite *mesosHelperTestSuite) TestRegisterHostAsDownNoop() {
	// Host is part of the down machines in the cluster status
	clusterStatusAsDown := &mesos_maintenance.ClusterStatus{
		DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{},
		DownMachines: []*mesos.MachineID{
			{
				Hostname: &suite.hostname,
				Ip:       &suite.IP,
			},
		},
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_master.Response_GetMaintenanceStatus{
			Status: clusterStatusAsDown,
		}, nil)

	suite.NoError(RegisterHostAsDown(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsDownFailure tests RegisterHostAsDown
// with failures
func (suite *mesosHelperTestSuite) TestRegisterHostAsDownFailure() {
	// Failure to get cluster maintenance status
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, errors.New("some error"))

	suite.Error(RegisterHostAsDown(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))

	// Failure to start maintenance
	// Host is part of the draining machines in the cluster status
	clusterStatusAsDraining := &mesos_maintenance.ClusterStatus{
		DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{
			{
				Id: &mesos.MachineID{
					Hostname: &suite.hostname,
					Ip:       &suite.IP,
				},
			},
		},
		DownMachines: []*mesos.MachineID{},
	}

	gomock.InOrder(
		suite.mockMasterOperatorClient.EXPECT().
			GetMaintenanceStatus().
			Return(&mesos_master.Response_GetMaintenanceStatus{
				Status: clusterStatusAsDraining,
			}, nil),
		suite.mockMasterOperatorClient.EXPECT().
			StartMaintenance(gomock.Any()).
			Return(errors.New("some error")),
	)

	suite.Error(RegisterHostAsDown(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsUp tests RegisterHostAsUp
func (suite *mesosHelperTestSuite) TestRegisterHostAsUp() {
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(gomock.Any()).
		Return(nil)

	suite.NoError(RegisterHostAsUp(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsUpNoop tests RegisterHostAsUp
// for a host already UP
func (suite *mesosHelperTestSuite) TestRegisterHostAsUpNoop() {
	// Host already UP part of the agentMap
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := &mesos_master.Response_GetAgents{
		Agents: []*mesos_master.Response_GetAgents_Agent{
			{
				AgentInfo: &mesos.AgentInfo{
					Hostname: &suite.hostname,
				},
				Pid: &[]string{"slave1@1.2.3.4:1234"}[0],
			},
		},
	}

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(agentsResponse, nil)
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().Return(nil, nil)
	for _, a := range agentsResponse.GetAgents() {
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

	suite.NoError(RegisterHostAsUp(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestRegisterHostAsUpFailure tests RegisterHostAsUp
// with failures
func (suite *mesosHelperTestSuite) TestRegisterHostAsUpFailure() {
	// Failure to stop maintenance
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(RegisterHostAsUp(
		suite.mockMasterOperatorClient,
		suite.hostname,
		suite.IP))
}

// TestIsHostAlreadyOnMaintenanceSchedule tests IsHostAlreadyOnMaintenanceSchedule
func (suite *mesosHelperTestSuite) TestIsHostAlreadyOnMaintenanceSchedule() {
	nanos := int64(time.Now().Nanosecond())
	schedule := &mesos_maintenance.Schedule{
		Windows: []*mesos_maintenance.Window{
			{
				MachineIds: []*mesos.MachineID{
					{
						Hostname: &suite.hostname,
						Ip:       &suite.IP,
					},
				},
				Unavailability: &mesos.Unavailability{
					Start: &mesos.TimeInfo{
						Nanoseconds: &nanos,
					},
				},
			},
		},
	}
	suite.True(IsHostAlreadyOnMaintenanceSchedule(suite.hostname, schedule))
	suite.False(IsHostAlreadyOnMaintenanceSchedule("unknown", schedule))
}

// TestIsHostPartOfDownMachines tests IsHostPartOfDownMachines
func (suite *mesosHelperTestSuite) TestIsHostPartOfDownMachines() {
	downMachines := []*mesos.MachineID{
		{
			Hostname: &suite.hostname,
			Ip:       &suite.IP,
		},
	}
	suite.True(IsHostPartOfDownMachines(suite.hostname, downMachines))
	suite.False(IsHostPartOfDownMachines("unknown", downMachines))
}
