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

package manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	mesospb "github.com/uber/peloton/.gen/mesos/v1"
	masterpb "github.com/uber/peloton/.gen/mesos/v1/master"
	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/host"
	hostmgr_host_mocks "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	_testPoolIDTemplate    = "pool%d"
	_testReconcileInterval = time.Second
)

// HostPoolManagerTestSuite is test suite for host pool manager.
type HostPoolManagerTestSuite struct {
	suite.Suite

	upMachines         []*mesospb.MachineID
	drainingMachines   []*mesospb.MachineID
	manager            HostPoolManager
	eventStreamHandler *eventstream.Handler
}

// SetupTest is setup function for this suite.
func (suite *HostPoolManagerTestSuite) SetupSuite() {
	upHost := "host1"
	upIP := "172.0.0.1"
	upMachine := &mesospb.MachineID{
		Hostname: &upHost,
		Ip:       &upIP,
	}
	suite.upMachines = []*mesospb.MachineID{upMachine}

	drainingHost := "host2"
	drainingIP := "172.0.0.2"
	drainingMachine := &mesospb.MachineID{
		Hostname: &drainingHost,
		Ip:       &drainingIP,
	}
	suite.drainingMachines = []*mesospb.MachineID{drainingMachine}
}

// SetupTest is setup function for each test in this suite.
func (suite *HostPoolManagerTestSuite) SetupTest() {
	testScope := tally.NewTestScope("", map[string]string{})
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		10,
		[]string{"client1"},
		nil,
		testScope)
	suite.manager = New(
		_testReconcileInterval,
		suite.eventStreamHandler,
		tally.NoopScope,
	)
}

// TestHostPoolManagerTestSuite runs HostPoolManagerTestSuite.
func TestHostPoolManagerTestSuite(t *testing.T) {
	suite.Run(t, new(HostPoolManagerTestSuite))
}

// TestGetPoolByHostname tests getting the host pool a given host belong to.
func (suite *HostPoolManagerTestSuite) TestGetPoolByHostname() {
	testHostname := "host1"
	expectedPoolID := "pool1"

	testCases := map[string]struct {
		poolIndex      map[string][]string
		hostToPoolMap  map[string]string
		expectedErrMsg string
	}{
		"no-error": {
			poolIndex: map[string][]string{
				expectedPoolID: {testHostname, "host2"},
			},
			hostToPoolMap: map[string]string{
				testHostname: expectedPoolID,
				"host2":      "pool2",
			},
		},
		"host-not-found": {
			poolIndex:     map[string][]string{},
			hostToPoolMap: map[string]string{},
			expectedErrMsg: fmt.Sprintf(
				"host %s not found", testHostname),
		},
		"host-pool-not-found": {
			poolIndex: map[string][]string{
				"pool2": {testHostname},
			},
			hostToPoolMap: map[string]string{
				testHostname: expectedPoolID,
			},
			expectedErrMsg: fmt.Sprintf(
				"host pool %s not found", expectedPoolID),
		},
	}

	for tcName, tc := range testCases {
		manager := setupTestManager(
			tc.poolIndex,
			tc.hostToPoolMap,
			suite.eventStreamHandler,
		)

		_, err := manager.GetPoolByHostname(testHostname)
		if tc.expectedErrMsg != "" {
			suite.EqualError(
				err,
				tc.expectedErrMsg,
				"test case %s",
				tcName,
			)
		} else {
			suite.NoErrorf(err, "test case %s", tcName)
		}
	}
}

// TestRegisterPool tests creating a host pool with given pool ID in parallel.
// It pre-registers 10 test host pools to the host manager and tests with
// 20 more host pool registration attempts, 10 of them were pre-registered already.
func (suite *HostPoolManagerTestSuite) TestRegisterPool() {
	// Pre-register 10 test host pools to host pool manager.
	numPools := 10
	preRegisterTestPools(suite.manager, numPools)

	nClients := 20
	wg := sync.WaitGroup{}
	wg.Add(nClients)

	for i := 0; i < nClients; i++ {
		go func(i int) {
			poolID := fmt.Sprintf(_testPoolIDTemplate, i)
			suite.manager.RegisterPool(poolID)
			_, err := suite.manager.GetPool(poolID)
			suite.NoError(err)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

// TestDeregisterPool tests deleting a host pool with given pool ID in parallel.
// It pre-registers 10 test host pools to the host manager and tests with
// another 20 host pool deletion attempts, 10 of them don't exist.
func (suite *HostPoolManagerTestSuite) TestDeregisterPool() {
	// Pre-register 10 test host pools to host pool manager.
	numPools := 10
	preRegisterTestPools(suite.manager, numPools)

	nClients := 20
	wg := sync.WaitGroup{}
	wg.Add(nClients)

	for i := 0; i < nClients; i++ {
		go func(i int) {
			poolID := fmt.Sprintf(_testPoolIDTemplate, i)
			notFoundErrMsg := fmt.Sprintf(
				"host pool %s not found", poolID)
			err := suite.manager.DeregisterPool(poolID)
			suite.NoError(err)
			_, err = suite.manager.GetPool(poolID)
			suite.EqualError(err, notFoundErrMsg)
			wg.Done()
		}(i)
	}

	wg.Wait()

	// Test deleting default host pool.
	err := suite.manager.DeregisterPool(common.DefaultHostPoolID)
	suite.EqualError(err, "code:invalid-argument "+
		"message:can't delete default host pool")
}

// TestDeregisterPoolWithHosts tests removing a pool that has hosts associated
func (suite *HostPoolManagerTestSuite) TestDeregisterPoolWithHosts() {
	hostToPoolMap := map[string]string{
		"host0": "pool0",
		"host1": "pool0",
		"host2": "pool0",
	}
	poolIndex := map[string][]string{
		"pool0": {"host0", "host1", "host2"},
	}
	manager := setupTestManager(
		poolIndex,
		hostToPoolMap,
		suite.eventStreamHandler)
	manager.RegisterPool("default")

	manager.DeregisterPool("pool0")

	for h := range hostToPoolMap {
		p, err := manager.GetPoolByHostname(h)
		suite.NoError(err)
		suite.Equal("default", p.ID())
	}
	defpool, err := manager.GetPool("default")
	suite.NoError(err)
	defpoolHosts := defpool.Hosts()
	suite.Contains(defpoolHosts, "host0")
	suite.Contains(defpoolHosts, "host1")
	suite.Contains(defpoolHosts, "host2")

	events, err := suite.eventStreamHandler.GetEvents()
	suite.NoError(err)
	suite.Equal(3, len(events))
}

// TestGetHostPoolLabelValuesGetHostPoolLabelValues tests creating a LabelValues
// for host pool of a host.
func (suite *HostPoolManagerTestSuite) TestGetHostPoolLabelValues() {
	testCases := map[string]struct {
		poolIndex      map[string][]string
		hostToPoolMap  map[string]string
		hostname       string
		expectedRes    constraints.LabelValues
		expectedErrMsg string
	}{
		"success": {
			poolIndex: map[string][]string{
				"pool1": {"host1"},
			},
			hostToPoolMap: map[string]string{
				"host1": "pool1",
			},
			hostname: "host1",
			expectedRes: constraints.LabelValues{
				common.HostPoolKey: map[string]uint32{"pool1": 1},
			},
		},
		"host-not-found": {
			poolIndex:     map[string][]string{},
			hostToPoolMap: map[string]string{},
			hostname:      "host1",
			expectedErrMsg: "error when getting host pool of " +
				"host host1: host host1 not found",
		},
	}

	for tcName, tc := range testCases {
		manager := setupTestManager(
			tc.poolIndex,
			tc.hostToPoolMap,
			suite.eventStreamHandler,
		)
		res, err := GetHostPoolLabelValues(manager, tc.hostname)
		if tc.expectedErrMsg != "" {
			suite.EqualError(err, tc.expectedErrMsg, "test case: %s", tcName)
		} else {
			suite.NoError(err, "test case: %s", tcName)
			suite.EqualValues(tc.expectedRes, res)
		}
	}
}

// TestReconcile tests Start, Stop and reconcile host pool manager.
func (suite *HostPoolManagerTestSuite) TestReconcile() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockMasterOperatorClient := mpb_mocks.NewMockMasterOperatorClient(ctrl)
	mockMaintenanceMap := hostmgr_host_mocks.NewMockMaintenanceHostInfoMap(ctrl)

	response := suite.makeAgentsResponse()
	mockMaintenanceMap.EXPECT().
		GetDrainingHostInfos(gomock.Any()).
		Return([]*pb_host.HostInfo{}).
		Times(len(suite.upMachines) + len(suite.drainingMachines))
	mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)

	loader := &host.Loader{
		OperatorClient:         mockMasterOperatorClient,
		Scope:                  tally.NewTestScope("", map[string]string{}),
		MaintenanceHostInfoMap: mockMaintenanceMap,
	}
	loader.Load(nil)

	testCases := map[string]struct {
		poolIndex     map[string][]string
		hostToPoolMap map[string]string
		expectedCache map[string]string
	}{
		"default-host-pool-not-exist": {
			poolIndex:     map[string][]string{},
			hostToPoolMap: map[string]string{},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"host-pool-cache-in-sync": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {"host1", "host2"},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"more-hosts-in-pool-index": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {"host1", "host2"},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"more-hosts-in-host-to-pool-map": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {"host1"},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"more-hosts-in-agent-map": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {"host1"},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"more-hosts-in-host-pool-cache": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {
					"host1",
					"host2",
					"host3",
				},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
				"host3": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
		"host-in-multiple-host-pools": {
			poolIndex: map[string][]string{
				common.DefaultHostPoolID: {"host1", "host2"},
				"pool1":                  {"host1"},
			},
			hostToPoolMap: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
			expectedCache: map[string]string{
				"host1": common.DefaultHostPoolID,
				"host2": common.DefaultHostPoolID,
			},
		},
	}

	for tcName, tc := range testCases {
		manager := setupTestManager(
			tc.poolIndex,
			tc.hostToPoolMap,
			suite.eventStreamHandler,
		)

		manager.Start()

		time.Sleep(2 * _testReconcileInterval)

		cached := make(map[string]string)
		poolIndex := manager.Pools()
		for poolID, pool := range poolIndex {
			for hostname := range pool.Hosts() {
				p, err := manager.GetPoolByHostname(hostname)
				suite.NoError(err, "test case: %s", tcName)
				suite.Equal(poolID, p.ID(), "test case: %s", tcName)
				cached[hostname] = poolID
			}
		}
		suite.EqualValues(tc.expectedCache, cached, "test case: %s", tcName)

		manager.Stop()
	}
}

// TestChangeHostPool tests various cases of changing pool for a host.
func (suite *HostPoolManagerTestSuite) TestChangeHostPool() {

	hostToPoolMap := map[string]string{
		"host0": "pool0",
		"host1": "pool1",
		"host2": "pool2",
		"host3": "pool3",
		"host4": "pool0",
		"host5": "pool1",
	}
	poolIndex := map[string][]string{
		"pool0": {"host0", "host4"},
		"pool1": {"host1", "host5"},
		"pool2": {"host2"},
		"pool3": {"host3"},
	}

	testCases := map[string]struct {
		pools                       map[string][]string
		isErr                       bool
		srcPoolID, destPoolID, host string
	}{
		"success": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool3",
			host:       "host2",
		},
		"no-op": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool2",
			host:       "host2",
		},
		"host-not-found": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool3",
			host:       "host20",
			isErr:      true,
		},
		"source-host-pool-invalid": {
			pools:      poolIndex,
			srcPoolID:  "pool20",
			destPoolID: "pool3",
			host:       "host2",
			isErr:      true,
		},
		"dest-host-pool-invalid": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool30",
			host:       "host2",
			isErr:      true,
		},
		"source-host-pool-not-found": {
			pools:      make(map[string][]string),
			srcPoolID:  "pool2",
			destPoolID: "pool3",
			host:       "host2",
			isErr:      true,
		},
	}

	for tcName, tc := range testCases {
		manager := setupTestManager(
			tc.pools,
			hostToPoolMap,
			suite.eventStreamHandler)

		err := manager.ChangeHostPool(tc.host, tc.srcPoolID, tc.destPoolID)
		if tc.isErr {
			suite.Error(err, tcName)
		} else {
			suite.NoError(err, tcName)

			p, err := manager.GetPoolByHostname(tc.host)
			suite.NoError(err, tcName)
			suite.Equal(tc.destPoolID, p.ID())

			if tc.destPoolID != tc.srcPoolID {
				destPool, err := manager.GetPool(tc.destPoolID)
				suite.NoError(err, tcName)
				suite.Contains(destPool.Hosts(), tc.host, tcName)

				srcPool, err := manager.GetPool(tc.srcPoolID)
				suite.NoError(err, tcName)
				suite.NotContains(srcPool.Hosts(), tc.host, tcName)

				// check event
				events, err := suite.eventStreamHandler.GetEvents()
				suite.NoError(err, tcName)
				suite.Equal(1, len(events))
				suite.Equal(
					pb_eventstream.Event_HOST_EVENT,
					events[0].GetType())
				suite.Equal(tc.host, events[0].GetHostEvent().GetHostname())
				suite.Equal(
					pb_host.HostEvent_TYPE_HOST_POOL,
					events[0].GetHostEvent().GetType())
				suite.Equal(
					tc.destPoolID,
					events[0].GetHostEvent().GetHostPoolEvent().GetPool())
			}
		}
	}
}

// makeAgentsResponse makes a fake GetAgents response from Mesos master.
func (suite *HostPoolManagerTestSuite) makeAgentsResponse() *masterpb.Response_GetAgents {
	response := &masterpb.Response_GetAgents{
		Agents: []*masterpb.Response_GetAgents_Agent{},
	}
	pidUp := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.upMachines[0].GetIp())
	hostnameUp := suite.upMachines[0].GetHostname()
	agentUp := &masterpb.Response_GetAgents_Agent{
		AgentInfo: &mesospb.AgentInfo{
			Hostname: &hostnameUp,
		},
		Pid: &pidUp,
	}
	response.Agents = append(response.Agents, agentUp)

	pidDraining := fmt.Sprintf(
		"slave(0)@%s:0.0.0.0", suite.drainingMachines[0].GetIp())
	hostnameDraining := suite.drainingMachines[0].GetHostname()
	agentDraining := &masterpb.Response_GetAgents_Agent{
		AgentInfo: &mesospb.AgentInfo{
			Hostname: &hostnameDraining,
		},
		Pid: &pidDraining,
	}
	response.Agents = append(response.Agents, agentDraining)

	return response
}

// setupTestManager set up test host manager by constructing
// a new host pool manager with given pool index and host index.
func setupTestManager(
	poolIndex map[string][]string,
	hostToPoolMap map[string]string,
	eventStreamHandler *eventstream.Handler,
) HostPoolManager {
	scope := tally.NoopScope
	manager := &hostPoolManager{
		reconcileInternal:  _testReconcileInterval,
		eventStreamHandler: eventStreamHandler,
		poolIndex:          map[string]hostpool.HostPool{},
		hostToPoolMap:      map[string]string{},
		lifecycle:          lifecycle.NewLifeCycle(),
		parentScope:        scope,
		metrics:            NewMetrics(scope),
	}

	for poolID, hosts := range poolIndex {
		manager.RegisterPool(poolID)
		pool, _ := manager.GetPool(poolID)
		for _, host := range hosts {
			pool.Add(host)
		}
	}

	for host, poolID := range hostToPoolMap {
		manager.hostToPoolMap[host] = poolID
	}

	return manager
}

// preRegisterTestPools creates given number of test host pools to given
// host pool manager with ID following the pattern as 'poolX'.
func preRegisterTestPools(manager HostPoolManager, numPools int) {
	for i := 0; i < numPools; i++ {
		manager.RegisterPool(fmt.Sprintf(_testPoolIDTemplate, i))
	}
}
