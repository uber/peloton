// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostsvc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/uber/peloton/pkg/common/util"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	svcpb "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"

	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/hostmgr/host"
	dm "github.com/uber/peloton/pkg/hostmgr/host/drainer/mocks"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	hmmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover/mocks"
	hpm_mock "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"
	ym "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type hostSvcHandlerTestSuite struct {
	suite.Suite

	ctx                      context.Context
	upMachine                *mesos.MachineID
	upMachines               []*mesos.MachineID
	downMachine              *mesos.MachineID
	downMachines             []*mesos.MachineID
	drainingMachine          *mesos.MachineID
	drainingMachines         []*mesos.MachineID
	drainedMachine           *mesos.MachineID
	drainedMachines          []*mesos.MachineID
	mockCtrl                 *gomock.Controller
	handler                  *serviceHandler
	mockDrainer              *dm.MockDrainer
	mockMasterOperatorClient *ym.MockMasterOperatorClient
	mockHostPoolManager      *hpm_mock.MockHostPoolManager
	mockHostInfoOps          *orm_mocks.MockHostInfoOps
	mockHostMover            *hmmocks.MockHostMover
}

func (suite *hostSvcHandlerTestSuite) SetupSuite() {
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

	drainedHost := "host4"
	drainedIP := "172.17.0.8"
	suite.drainedMachine = &mesos.MachineID{
		Hostname: &drainedHost,
		Ip:       &drainedIP,
	}
	suite.drainedMachines = append(suite.drainedMachines, suite.drainedMachine)
}

func (suite *hostSvcHandlerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockMasterOperatorClient = ym.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockDrainer = dm.NewMockDrainer(suite.mockCtrl)
	suite.mockHostPoolManager = hpm_mock.NewMockHostPoolManager(
		suite.mockCtrl,
	)
	suite.handler.hostPoolManager = suite.mockHostPoolManager
	suite.handler.drainer = suite.mockDrainer
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.mockCtrl)
	suite.mockHostMover = hmmocks.NewMockHostMover(suite.mockCtrl)
	suite.handler.hostMover = suite.mockHostMover

	response := suite.makeAgentsResponse()
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		HostInfoOps:    suite.mockHostInfoOps,
	}
	suite.setupLoaderMocks(response)
	loader.Load(nil)
}

func (suite *hostSvcHandlerTestSuite) makeAgentsResponse() *mesosmaster.Response_GetAgents {
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

func (suite *hostSvcHandlerTestSuite) setupLoaderMocks(response *mesosmaster.Response_GetAgents) {
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().Return(nil, nil)
	for _, a := range response.GetAgents() {
		ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(a.GetPid())
		suite.NoError(err)

		suite.mockHostInfoOps.EXPECT().Create(
			gomock.Any(),
			a.GetAgentInfo().GetHostname(),
			ip,
			hpb.HostState_HOST_STATE_UP,
			hpb.HostState_HOST_STATE_UP,
			map[string]string{},
			"",
			"",
		).Return(nil)
	}
}

func TestHostSvcHandler(t *testing.T) {
	suite.Run(t, new(hostSvcHandlerTestSuite))
}

func (suite *hostSvcHandlerTestSuite) TearDownTest() {
	log.Info("tearing down hostSvcHandlerTestSuite")
	suite.mockCtrl.Finish()
}

func (suite *hostSvcHandlerTestSuite) TestStartMaintenance() {
	hostname := "host1"
	suite.mockDrainer.EXPECT().StartMaintenance(gomock.Any(), hostname).Return(nil)
	resp, err := suite.handler.StartMaintenance(
		suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		})
	suite.NoError(err)
	suite.Equal(
		&svcpb.StartMaintenanceResponse{Hostname: hostname},
		resp)

	hostname = "host2"
	suite.mockDrainer.EXPECT().StartMaintenance(gomock.Any(), hostname).
		Return(fmt.Errorf("sonmething went wrong"))
	resp, err = suite.handler.StartMaintenance(
		suite.ctx,
		&svcpb.StartMaintenanceRequest{
			Hostname: hostname,
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *hostSvcHandlerTestSuite) TestCompleteMaintenance() {
	hostname := "host1"
	suite.mockDrainer.EXPECT().CompleteMaintenance(gomock.Any(), hostname).Return(nil)
	resp, err := suite.handler.CompleteMaintenance(
		suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostname: hostname,
		})
	suite.NoError(err)
	suite.Equal(
		&svcpb.CompleteMaintenanceResponse{Hostname: hostname},
		resp)

	hostname = "host2"
	suite.mockDrainer.EXPECT().CompleteMaintenance(gomock.Any(), hostname).
		Return(fmt.Errorf("something went wrong"))
	resp, err = suite.handler.CompleteMaintenance(
		suite.ctx,
		&svcpb.CompleteMaintenanceRequest{
			Hostname: hostname,
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *hostSvcHandlerTestSuite) TestQueryHosts() {
	suite.doTestQueryHosts()
}

func (suite *hostSvcHandlerTestSuite) doTestQueryHosts() {
	var (
		hostInfos         []*hpb.HostInfo
		drainingHostInfos []*hpb.HostInfo
		drainedHostInfos  []*hpb.HostInfo
		downHostsInfos    []*hpb.HostInfo
	)
	useHostPool := suite.handler.hostPoolManager != nil

	response := suite.makeAgentsResponse()
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		HostInfoOps:    suite.mockHostInfoOps,
	}
	suite.setupLoaderMocks(response)
	loader.Load(nil)

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

	for _, machine := range suite.drainedMachines {
		drainedHostInfos = append(drainedHostInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINED,
		})
		hostInfos = append(hostInfos, &hpb.HostInfo{
			Hostname: machine.GetHostname(),
			Ip:       machine.GetIp(),
			State:    hpb.HostState_HOST_STATE_DRAINED,
		})
	}
	expectedCurrentPool, expectedDesiredPool := "", ""
	hpool := hostpool.New("pool1", tally.NoopScope)
	if useHostPool {
		expectedCurrentPool = hpool.ID()
		expectedDesiredPool = "pool-other"
	}

	suite.mockDrainer.EXPECT().
		GetAllDrainingHostInfos().
		Return(drainingHostInfos, nil).
		AnyTimes()
	suite.mockDrainer.EXPECT().
		GetAllDrainedHostInfos().
		Return(drainedHostInfos, nil).
		AnyTimes()
	suite.mockDrainer.EXPECT().
		GetAllDownHostInfos().
		Return(downHostsInfos, nil).
		AnyTimes()
	if useHostPool {
		suite.mockHostPoolManager.EXPECT().
			GetPoolByHostname(gomock.Any()).
			Return(hpool, nil).
			AnyTimes()
		suite.mockHostPoolManager.EXPECT().
			GetDesiredPool(gomock.Any()).
			Return("pool-other", nil).
			AnyTimes()
	}
	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
			hpb.HostState_HOST_STATE_DRAINING,
			hpb.HostState_HOST_STATE_DRAINED,
			hpb.HostState_HOST_STATE_DOWN,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		len(suite.upMachines)+
			len(suite.drainingMachines)+
			len(suite.drainedMachines)+
			len(suite.downMachines),
		len(resp.GetHostInfos()),
	)

	hostnameSet := stringset.New()
	for _, hostInfo := range resp.GetHostInfos() {
		hostnameSet.Add(hostInfo.GetHostname())
		suite.Equal(
			expectedCurrentPool,
			hostInfo.GetCurrentPool(),
			"host %s", hostInfo.GetHostname())
		suite.Equal(
			expectedDesiredPool,
			hostInfo.GetDesiredPool(),
			"host %s", hostInfo.GetHostname())
	}
	for _, upMachine := range suite.upMachines {
		suite.True(hostnameSet.Contains(upMachine.GetHostname()))
	}
	for _, drainingMachine := range suite.drainingMachines {
		suite.True(hostnameSet.Contains(drainingMachine.GetHostname()))
	}
	for _, drainedMachine := range suite.drainedMachines {
		suite.True(hostnameSet.Contains(drainedMachine.GetHostname()))
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
			len(suite.drainedMachines)+
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
	for _, drainedMachine := range suite.drainedMachines {
		suite.True(hostnameSet.Contains(drainedMachine.GetHostname()))
	}
	for _, downMachine := range suite.downMachines {
		suite.True(hostnameSet.Contains(downMachine.GetHostname()))
	}
}

func (suite *hostSvcHandlerTestSuite) TestQueryHostsError() {
	response := suite.makeAgentsResponse()
	loader := &host.Loader{
		OperatorClient: suite.mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		HostInfoOps:    suite.mockHostInfoOps,
	}
	suite.setupLoaderMocks(response)
	loader.Load(nil)

	// Test ExtractIPFromMesosAgentPID error
	hostname := "testhost"
	pid := "invalidPID"
	host.GetAgentMap().RegisteredAgents[hostname] = &mesosmaster.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname: &hostname,
		},
		Pid: &pid,
	}

	suite.mockDrainer.EXPECT().
		GetAllDrainingHostInfos().
		Return([]*hpb.HostInfo{}, nil).Times(2)
	suite.mockDrainer.EXPECT().
		GetAllDrainedHostInfos().
		Return([]*hpb.HostInfo{}, nil).Times(2)
	suite.mockDrainer.EXPECT().
		GetAllDownHostInfos().
		Return([]*hpb.HostInfo{}, nil).Times(2)

	resp, err := suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.Error(err)
	suite.Nil(resp)

	// Test 'No registered agents'
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.mockMasterOperatorClient.EXPECT().Agents().Return(nil, nil)
	suite.mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().Return(nil, nil)
	loader.Load(nil)

	resp, err = suite.handler.QueryHosts(suite.ctx, &svcpb.QueryHostsRequest{
		HostStates: []hpb.HostState{
			hpb.HostState_HOST_STATE_UP,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
}

func (suite *hostSvcHandlerTestSuite) TestQueryHostsHostPoolsNotEnabled() {
	suite.handler.hostPoolManager = nil
	suite.doTestQueryHosts()
}

// TestHostPoolsNotEnabled exercises the host-pools APIs when
// host-pool is disabled
func (suite *hostSvcHandlerTestSuite) TestHostPoolsNotEnabled() {
	suite.handler.hostPoolManager = nil

	_, err := suite.handler.ListHostPools(
		suite.ctx,
		&svcpb.ListHostPoolsRequest{},
	)
	suite.Error(err)

	_, err = suite.handler.CreateHostPool(
		suite.ctx,
		&svcpb.CreateHostPoolRequest{Name: "foo"},
	)
	suite.Error(err)

	_, err = suite.handler.DeleteHostPool(
		suite.ctx,
		&svcpb.DeleteHostPoolRequest{Name: "foo"},
	)
	suite.Error(err)

	_, err = suite.handler.ChangeHostPool(
		suite.ctx,
		&svcpb.ChangeHostPoolRequest{},
	)
	suite.Error(err)
	_, err = suite.handler.MoveHosts(
		suite.ctx,
		&svcpb.MoveHostsRequest{},
	)
	suite.Error(err)
}

// TestListHostPools tests ListHostPools API method
func (suite *hostSvcHandlerTestSuite) TestListHostPools() {
	scope := tally.NoopScope
	pool1 := hostpool.New("pool1", scope)
	pool1.Add("h1")
	pool1.Add("h2")

	pool2 := hostpool.New("pool2", scope)
	pool2.Add("h3")

	pools := map[string]hostpool.HostPool{
		pool1.ID(): pool1,
		pool2.ID(): pool2,
	}

	suite.mockHostPoolManager.EXPECT().Pools().Return(pools)

	resp, err := suite.handler.ListHostPools(
		suite.ctx,
		&svcpb.ListHostPoolsRequest{},
	)
	suite.NoError(err)
	suite.NotNil(resp)

	suite.Equal(2, len(resp.Pools))
	for _, p := range resp.Pools {
		if p.GetName() == "pool1" {
			suite.ElementsMatch([]string{"h1", "h2"}, p.GetHosts())
		} else if p.GetName() == "pool2" {
			suite.ElementsMatch([]string{"h3"}, p.GetHosts())
		} else {
			suite.Fail("Unknown pool %s", p.GetName())
		}
	}
}

// TestCreateHostPools tests CreateHostPools API method
func (suite *hostSvcHandlerTestSuite) TestCreateHostPool() {

	// success case
	suite.mockHostPoolManager.EXPECT().GetPool("new-pool").
		Return(nil, fmt.Errorf("not found"))
	suite.mockHostPoolManager.EXPECT().RegisterPool("new-pool")

	resp, err := suite.handler.CreateHostPool(
		suite.ctx,
		&svcpb.CreateHostPoolRequest{Name: "new-pool"},
	)
	suite.NoError(err)
	suite.NotNil(resp)

	// exisiting pool
	suite.mockHostPoolManager.EXPECT().GetPool("old-pool").
		Return(nil, nil)
	_, err = suite.handler.CreateHostPool(
		suite.ctx,
		&svcpb.CreateHostPoolRequest{Name: "old-pool"},
	)
	suite.Error(err)

	// Bad pool name
	_, err = suite.handler.CreateHostPool(
		suite.ctx,
		&svcpb.CreateHostPoolRequest{},
	)
	suite.Error(err)
}

// TestDeleteHostPools tests DeleteHostPools API method
func (suite *hostSvcHandlerTestSuite) TestDeleteHostPool() {

	// success case
	suite.mockHostPoolManager.EXPECT().GetPool("old-pool").
		Return(nil, nil)
	suite.mockHostPoolManager.EXPECT().DeregisterPool("old-pool")

	resp, err := suite.handler.DeleteHostPool(
		suite.ctx,
		&svcpb.DeleteHostPoolRequest{Name: "old-pool"},
	)
	suite.NoError(err)
	suite.NotNil(resp)

	// non-existing pool
	suite.mockHostPoolManager.EXPECT().GetPool("bad-pool").
		Return(nil, fmt.Errorf("not found"))
	_, err = suite.handler.DeleteHostPool(
		suite.ctx,
		&svcpb.DeleteHostPoolRequest{Name: "bad-pool"},
	)
	suite.Error(err)

	// delete default pool
	_, err = suite.handler.DeleteHostPool(
		suite.ctx,
		&svcpb.DeleteHostPoolRequest{Name: "default"},
	)
	suite.Error(err)
}

// TestChangeHostPools tests ChangeHostPools API method
func (suite *hostSvcHandlerTestSuite) TestChangeHostPool() {
	// success case
	suite.mockHostPoolManager.EXPECT().
		ChangeHostPool("h1", "p1", "p2").
		Return(nil)

	resp, err := suite.handler.ChangeHostPool(
		suite.ctx,
		&svcpb.ChangeHostPoolRequest{
			Hostname:        "h1",
			SourcePool:      "p1",
			DestinationPool: "p2",
		},
	)
	suite.NoError(err)
	suite.NotNil(resp)

	// bad request
	suite.mockHostPoolManager.EXPECT().
		ChangeHostPool("h1-does-not-exist", "p1", "p2").
		Return(fmt.Errorf("not found"))
	_, err = suite.handler.ChangeHostPool(
		suite.ctx,
		&svcpb.ChangeHostPoolRequest{
			Hostname:        "h1-does-not-exist",
			SourcePool:      "p1",
			DestinationPool: "p2",
		},
	)
	suite.Error(err)
}

// TestMoveHosts tests MoveHosts API method
func (suite *hostSvcHandlerTestSuite) TestMoveHosts() {
	// success case
	suite.mockHostMover.EXPECT().MoveHosts(gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := suite.handler.MoveHosts(
		suite.ctx,
		&svcpb.MoveHostsRequest{
			SourcePool:           "p1",
			SrcPoolDesiredHosts:  1,
			DestinationPool:      "p2",
			DestPoolDesiredHosts: 1,
		},
	)
	suite.NoError(err)
	suite.NotNil(resp)
}

func (suite *hostSvcHandlerTestSuite) TestMoveHostError() {
	/// bad request
	suite.mockHostMover.EXPECT().MoveHosts(gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("error"))

	_, err := suite.handler.MoveHosts(
		suite.ctx,
		&svcpb.MoveHostsRequest{
			SourcePool:           "p1",
			SrcPoolDesiredHosts:  1,
			DestinationPool:      "p2",
			DestPoolDesiredHosts: 1,
		},
	)
	suite.Error(err)
}
