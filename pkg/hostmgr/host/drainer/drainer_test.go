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

package drainer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	goalstate_mocks "github.com/uber/peloton/pkg/hostmgr/goalstate/mocks"
	"github.com/uber/peloton/pkg/hostmgr/host"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	queuemocks "github.com/uber/peloton/pkg/hostmgr/queue/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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
	mockHostInfoOps          *orm_mocks.MockHostInfoOps
	mockGoalStateDriver      *goalstate_mocks.MockDriver
	mockTaskEvictionQueue    *queuemocks.MockTaskQueue
	upHost                   string
	upIP                     string
}

func (suite *drainerTestSuite) SetupSuite() {
	suite.upHost = "host1"
	suite.upIP = "172.17.0.5"
}

func (suite *drainerTestSuite) SetupTest() {
	suite.ctx = context.Background()
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.mockCtrl)
	suite.mockGoalStateDriver = goalstate_mocks.NewMockDriver(suite.mockCtrl)
	suite.mockTaskEvictionQueue = queuemocks.NewMockTaskQueue(suite.mockCtrl)

	suite.drainer = &drainer{
		drainerPeriod:        drainerPeriod,
		taskEvictionQueue:    suite.mockTaskEvictionQueue,
		pelotonAgentRole:     pelotonAgentRole,
		masterOperatorClient: suite.mockMasterOperatorClient,
		lifecycle:            lifecycle.NewLifeCycle(),
		goalStateDriver:      suite.mockGoalStateDriver,
		hostInfoOps:          suite.mockHostInfoOps,
	}
}

func (suite *drainerTestSuite) makeUpAgentResponse() *mesosmaster.Response_GetAgents {
	response := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{},
	}

	pidUp := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.upIP)
	agentUp := &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &suite.upHost,
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
	return response
}

func (suite *drainerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func (suite *drainerTestSuite) setupLoaderMocks(agentsResponse *mesosmaster.Response_GetAgents) {
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
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(drainerTestSuite))
}

// TestNewDrainer test creation of new host drainer
func (suite *drainerTestSuite) TestDrainerNewDrainer() {
	drainer := NewDrainer(
		drainerPeriod,
		pelotonAgentRole,
		suite.mockMasterOperatorClient,
		suite.mockGoalStateDriver,
		orm_mocks.NewMockHostInfoOps(suite.mockCtrl),
		suite.mockTaskEvictionQueue,
	)
	suite.NotNil(drainer)
}

// TestStartMaintenance tests StartMaintenance
func (suite *drainerTestSuite) TestStartMaintenance() {
	// Mock 1 host `id-0` as a peloton agent
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := suite.makeUpAgentResponse()
	suite.setupLoaderMocks(agentsResponse)
	loader.Load(nil)

	gomock.InOrder(
		suite.mockHostInfoOps.EXPECT().UpdateGoalState(
			suite.ctx,
			suite.upHost,
			pbhost.HostState_HOST_STATE_DOWN,
		).Return(nil),
		suite.mockGoalStateDriver.EXPECT().EnqueueHost(suite.upHost, gomock.Any()),
	)
	suite.NoError(suite.drainer.StartMaintenance(suite.ctx, suite.upHost))
}

// TestStartMaintenanceCassandraError tests StartMaintenance with DB error
func (suite *drainerTestSuite) TestStartMaintenanceCassandraError() {
	// Mock 1 host `id-0` as a peloton agent
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := suite.makeUpAgentResponse()
	suite.setupLoaderMocks(agentsResponse)
	loader.Load(nil)

	suite.mockHostInfoOps.EXPECT().UpdateGoalState(
		suite.ctx,
		suite.upHost,
		pbhost.HostState_HOST_STATE_DOWN,
	).Return(errors.New("some error"))

	suite.Error(suite.drainer.StartMaintenance(suite.ctx, suite.upHost))
}

// TestStartMaintenanceNonPelotonAgentError tests the failure case of starting
// maintenance when the host is not registered as a Peloton agent
func (suite *drainerTestSuite) TestStartMaintenanceNonPelotonAgentError() {
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := &mesosmaster.Response_GetAgents{
		Agents: []*mesosmaster.Response_GetAgents_Agent{
			{
				AgentInfo: &mesos.AgentInfo{
					Hostname: &suite.upHost,
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
				Pid: &[]string{"slave1@1.2.3.4:1234"}[0],
			},
		},
	}
	suite.setupLoaderMocks(agentsResponse)
	loader.Load(nil)

	err := suite.drainer.StartMaintenance(suite.ctx, suite.upHost)
	suite.Error(err)
}

// TestCompleteMaintenance tests CompleteMaintenance
func (suite *drainerTestSuite) TestCompleteMaintenance() {
	hostname := "hostname"
	hostInfo := &pbhost.HostInfo{
		Hostname:  hostname,
		Ip:        "IP",
		State:     pbhost.HostState_HOST_STATE_DOWN,
		GoalState: pbhost.HostState_HOST_STATE_DOWN,
	}

	suite.mockHostInfoOps.EXPECT().Get(
		suite.ctx,
		hostname,
	).Return(hostInfo, nil)
	suite.mockHostInfoOps.EXPECT().UpdateGoalState(
		suite.ctx,
		hostname,
		pbhost.HostState_HOST_STATE_UP,
	).Return(nil)
	suite.mockGoalStateDriver.EXPECT().EnqueueHost(hostname, gomock.Any())

	suite.NoError(suite.drainer.CompleteMaintenance(suite.ctx, hostname))
}

// TestCompleteMaintenanceCassandraNotFound tests CompleteMaintenance
// for a host not found in DB
func (suite *drainerTestSuite) TestCompleteMaintenanceCassandraNotFound() {
	hostname := "hostname"
	notFoundErr := yarpcerrors.NotFoundErrorf("not found")
	suite.mockHostInfoOps.EXPECT().Get(
		suite.ctx,
		hostname,
	).Return(nil, notFoundErr)

	suite.Equal(
		notFoundErr,
		suite.drainer.CompleteMaintenance(suite.ctx, hostname))
}

// TestCompleteMaintenanceHostNotDown tests CompleteMaintenance
// for a host not down
func (suite *drainerTestSuite) TestCompleteMaintenanceHostNotDown() {
	hostname := "hostname"
	hostInfo := &pbhost.HostInfo{
		Hostname: hostname,
		State:    pbhost.HostState_HOST_STATE_DRAINING,
	}

	suite.mockHostInfoOps.EXPECT().Get(
		suite.ctx,
		hostname,
	).Return(hostInfo, nil)

	suite.Equal(
		yarpcerrors.NotFoundErrorf("Host is not DOWN"),
		suite.drainer.CompleteMaintenance(suite.ctx, hostname))
}

// TestGetAllHostInfosPerState tests GetAllHostInfosPerState
func (suite *drainerTestSuite) TestGetAllHostInfosPerState() {
	allHostInfos := []*pbhost.HostInfo{
		{
			Hostname: "host1",
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		},
		{
			Hostname: "host2",
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		},
		{
			Hostname: "host3",
			State:    pbhost.HostState_HOST_STATE_DRAINED,
		},
		{
			Hostname: "host4",
			State:    pbhost.HostState_HOST_STATE_DOWN,
		},
		{
			Hostname: "host5",
			State:    pbhost.HostState_HOST_STATE_DOWN,
		},
	}
	suite.mockHostInfoOps.EXPECT().GetAll(suite.ctx).Return(allHostInfos, nil).Times(3)

	drainingHostInfos, err := suite.drainer.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DRAINING)
	suite.NoError(err)
	drainedHostInfos, err := suite.drainer.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DRAINED)
	suite.NoError(err)
	downHostInfos, err := suite.drainer.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DOWN)
	suite.NoError(err)

	suite.Equal(
		[]*pbhost.HostInfo{
			{
				Hostname: "host1",
				State:    pbhost.HostState_HOST_STATE_DRAINING,
			},
			{
				Hostname: "host2",
				State:    pbhost.HostState_HOST_STATE_DRAINING,
			},
		},
		drainingHostInfos)
	suite.Equal(
		[]*pbhost.HostInfo{
			{
				Hostname: "host3",
				State:    pbhost.HostState_HOST_STATE_DRAINED,
			},
		},
		drainedHostInfos)
	suite.Equal(
		[]*pbhost.HostInfo{
			{
				Hostname: "host4",
				State:    pbhost.HostState_HOST_STATE_DOWN,
			},
			{
				Hostname: "host5",
				State:    pbhost.HostState_HOST_STATE_DOWN,
			},
		},
		downHostInfos)
}

// TestReconcileMaintenanceState tests reconcileMaintenanceState
func (suite *drainerTestSuite) TestReconcileMaintenanceState() {
	hostDraining := "hostDraining"
	hostDown := "hostDown"
	IP := "IP"

	hostInfosInDB := []*pbhost.HostInfo{
		{
			// State matches between DB and Mesos Master
			Hostname: hostDraining,
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		},
		{
			// State matches between DB and Mesos Master
			Hostname: hostDown,
			State:    pbhost.HostState_HOST_STATE_DOWN,
		},
		{
			// State is DOWN in DB but UP on mesos master
			Hostname: suite.upHost,
			State:    pbhost.HostState_HOST_STATE_DOWN,
		},
	}
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NoopScope,
		HostInfoOps:    suite.mockHostInfoOps,
	}
	agentsResponse := suite.makeUpAgentResponse()
	suite.setupLoaderMocks(agentsResponse)

	loader.Load(nil)

	maintenanceStatus := mesosmaster.Response_GetMaintenanceStatus{
		Status: &mesosmaintenance.ClusterStatus{
			DrainingMachines: []*mesosmaintenance.ClusterStatus_DrainingMachine{
				{
					Id: &mesos.MachineID{
						Hostname: &hostDraining,
						Ip:       &IP,
					},
				},
			},
			DownMachines: []*mesos.MachineID{
				{
					Hostname: &hostDown,
					Ip:       &IP,
				},
			},
		},
	}
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(hostInfosInDB, nil)

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&maintenanceStatus, nil)

	// The host should be updated in DB with state UP read from Mesos Master
	// and enqueue into the goal state engine
	suite.mockHostInfoOps.EXPECT().
		UpdateState(suite.ctx, suite.upHost, pbhost.HostState_HOST_STATE_UP).Return(nil)
	suite.mockGoalStateDriver.EXPECT().
		EnqueueHost(suite.upHost, gomock.Any())

	suite.NoError(suite.drainer.reconcileMaintenanceState())
}

// TestDrainerStartSuccess tests the success case of starting the host drainer
func (suite *drainerTestSuite) TestDrainerStartSuccess() {
	suite.mockGoalStateDriver.EXPECT().
		Start()
	suite.mockGoalStateDriver.EXPECT().
		Stop()
	suite.mockHostInfoOps.EXPECT().
		GetAll(gomock.Any()).Return([]*pbhost.HostInfo{}, nil).AnyTimes()
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesosmaster.Response_GetMaintenanceStatus{}, nil).AnyTimes()

	// Starting drainer again should be no-op
	suite.drainer.Start()
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestStop tests stopping the host drainer
func (suite *drainerTestSuite) TestStop() {
	// Start to then stop
	suite.mockGoalStateDriver.EXPECT().
		Start().Times(1)
	suite.drainer.Start()

	suite.mockGoalStateDriver.EXPECT().
		Stop().Times(1)
	suite.drainer.Stop()
	<-suite.drainer.lifecycle.StopCh()
}
