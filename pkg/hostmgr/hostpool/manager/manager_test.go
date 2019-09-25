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

	"github.com/uber/peloton/pkg/common/util"

	pbmesos "github.com/uber/peloton/.gen/mesos/v1"
	pbmesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	mpbmocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	_testPoolIDTemplate    = "pool%d"
	_testReconcileInterval = 50 * time.Millisecond
)

// HostPoolManagerTestSuite is test suite for host pool manager.
type HostPoolManagerTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	upMachines         []*pbmesos.MachineID
	drainingMachines   []*pbmesos.MachineID
	manager            HostPoolManager
	eventStreamHandler *eventstream.Handler
	hostCapacity       *host.ResourceCapacity
	mockHostInfoOps    *orm_mocks.MockHostInfoOps
}

// SetupTest is setup function for this suite.
func (suite *HostPoolManagerTestSuite) SetupSuite() {
	upHost := "host1"
	upIP := "172.0.0.1"
	upMachine := &pbmesos.MachineID{
		Hostname: &upHost,
		Ip:       &upIP,
	}
	suite.upMachines = []*pbmesos.MachineID{upMachine}

	drainingHost := "host2"
	drainingIP := "172.0.0.2"
	drainingMachine := &pbmesos.MachineID{
		Hostname: &drainingHost,
		Ip:       &drainingIP,
	}
	suite.drainingMachines = []*pbmesos.MachineID{drainingMachine}
	suite.hostCapacity = &host.ResourceCapacity{
		Physical: scalar.Resources{
			CPU:  float64(2.0),
			Mem:  float64(4.0),
			Disk: float64(10.0),
			GPU:  float64(1.0),
		},
		Slack: scalar.Resources{
			CPU: float64(1.5),
		},
	}
}

// SetupTest is setup function for each test in this suite.
func (suite *HostPoolManagerTestSuite) SetupTest() {
	testScope := tally.NewTestScope("", map[string]string{})

	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		10,
		[]string{"client1"},
		nil,
		testScope)

	suite.ctrl = gomock.NewController(suite.T())
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.ctrl)

	suite.manager = New(
		_testReconcileInterval,
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
		tally.NoopScope,
	)
}

func (suite *HostPoolManagerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
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
		suite.manager = setupTestManager(
			tc.poolIndex,
			tc.hostToPoolMap,
			suite.eventStreamHandler,
			suite.mockHostInfoOps,
		)

		_, err := suite.manager.GetPoolByHostname(testHostname)
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
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	loader := suite.setupAgentMapLoader(ctrl)
	loader.Load(nil)
	host.GetAgentMap().HostCapacities = map[string]*host.ResourceCapacity{
		"host0": suite.hostCapacity,
		"host1": suite.hostCapacity,
		"host2": suite.hostCapacity,
	}

	hostToPoolMap := map[string]string{
		"host0": "pool0",
		"host1": "pool0",
		"host2": "pool0",
	}
	poolIndex := map[string][]string{
		"pool0": {"host0", "host1", "host2"},
	}
	suite.manager = setupTestManager(
		poolIndex,
		hostToPoolMap,
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
	)
	suite.manager.RegisterPool("default")

	for _, h := range poolIndex["pool0"] {
		suite.mockHostInfoOps.EXPECT().UpdatePool(
			gomock.Any(),
			h,
			common.DefaultHostPoolID,
			common.DefaultHostPoolID,
		).Return(nil)
	}

	err := suite.manager.DeregisterPool("pool0")
	suite.NoError(err)

	for h := range hostToPoolMap {
		p, err := suite.manager.GetPoolByHostname(h)
		suite.NoError(err)
		suite.Equal("default", p.ID())
	}
	defpool, err := suite.manager.GetPool("default")
	suite.NoError(err)
	defpoolHosts := defpool.Hosts()
	suite.Contains(defpoolHosts, "host0")
	suite.Contains(defpoolHosts, "host1")
	suite.Contains(defpoolHosts, "host2")

	events, err := suite.eventStreamHandler.GetEvents()
	suite.NoError(err)
	suite.Equal(3, len(events))

	suite.checkPoolCapacity(
		suite.manager.(*hostPoolManager),
		"TestDeregisterPoolWithHosts")
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
		suite.manager = setupTestManager(
			tc.poolIndex,
			tc.hostToPoolMap,
			suite.eventStreamHandler,
			suite.mockHostInfoOps,
		)
		res, err := GetHostPoolLabelValues(suite.manager, tc.hostname)
		if tc.expectedErrMsg != "" {
			suite.EqualError(err, tc.expectedErrMsg, "test case: %s", tcName)
		} else {
			suite.NoError(err, "test case: %s", tcName)
			suite.EqualValues(tc.expectedRes, res)
		}
	}
}

// TestStartStop tests Start, Stop and recovery
func (suite *HostPoolManagerTestSuite) TestStartStopRecover() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	loader := suite.setupAgentMapLoader(ctrl)
	loader.Load(nil)
	host.GetAgentMap().HostCapacities = map[string]*host.ResourceCapacity{
		"host0": suite.hostCapacity,
		"host1": suite.hostCapacity,
		"host2": suite.hostCapacity,
		"host3": suite.hostCapacity,
	}

	defaultHostInfo := []*pbhost.HostInfo{
		{
			Hostname:    "host0",
			CurrentPool: "pool1",
		},
		{
			Hostname:    "host1",
			CurrentPool: "pool1",
		},
		{
			Hostname:    "host2",
			CurrentPool: "pool2",
		},
		{
			Hostname: "host3",
		},
	}
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).
		Return(defaultHostInfo, nil)
	suite.mockHostInfoOps.EXPECT().UpdatePool(
		gomock.Any(),
		"host3",
		common.DefaultHostPoolID,
		common.DefaultHostPoolID).
		Return(nil)

	manager := setupTestManager(
		make(map[string][]string),
		make(map[string]string),
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
	)
	// Disable reconciliation by setting the interval to a large value
	mgr := manager.(*hostPoolManager)
	mgr.reconcileInternal = 5 * time.Minute

	// Stop before Start should go through fine.
	manager.Stop()

	suite.NoError(manager.Start())
	resultPoolIndex := manager.Pools()
	for poolID, pool := range resultPoolIndex {
		if poolID == common.DefaultHostPoolID {
			suite.Equal(1, len(pool.Hosts()))
			suite.Contains(pool.Hosts(), "host3")
		} else if poolID == "pool1" {
			suite.Equal(2, len(pool.Hosts()))
			suite.Contains(pool.Hosts(), "host0")
			suite.Contains(pool.Hosts(), "host1")
		} else if poolID == "pool2" {
			suite.Equal(1, len(pool.Hosts()))
			suite.Contains(pool.Hosts(), "host2")
		} else {
			suite.Fail("Unexpected pool %s", poolID)
		}
	}
	suite.checkPoolCapacity(mgr, "recover")

	// verify starting again is ok
	suite.NoError(manager.Start())

	manager.Stop()
	// verify stopping again is ok
	manager.Stop()
}

// TestStartError tests Start error due to recovery error
func (suite *HostPoolManagerTestSuite) TestStartError() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	expectedErr := errors.New("something bad")
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).
		Return(nil, expectedErr)

	manager := setupTestManager(
		make(map[string][]string),
		make(map[string]string),
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
	)
	suite.Error(manager.Start())
}

func (suite *HostPoolManagerTestSuite) TestReconcile() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	loader := suite.setupAgentMapLoader(ctrl)
	loader.Load(nil)
	host.GetAgentMap().HostCapacities = map[string]*host.ResourceCapacity{
		"host1": suite.hostCapacity,
		"host2": suite.hostCapacity,
		"host3": suite.hostCapacity,
		"host4": suite.hostCapacity,
	}
	poolIndex := map[string][]string{
		"pool0": {"host1"},
		"pool1": {"host2", "host3"},
	}
	hostToPoolMap := map[string]string{
		"host1": "pool0",
		"host2": "pool1",
		"host3": "pool1",
	}

	testCases := []struct {
		tcName                string
		dbHosts               []*pbhost.HostInfo
		expectedhostToPoolMap map[string]string
		expectedNewHost       string
		expectedNumEvents     int
	}{
		{
			tcName: "in-sync-with-db",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool1"},
				{Hostname: "host3", CurrentPool: "pool1"},
			},
			expectedhostToPoolMap: hostToPoolMap,
		},
		{
			tcName: "new-host-in-db",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool1"},
				{Hostname: "host3", CurrentPool: "pool1"},
				{Hostname: "host4"},
			},
			expectedhostToPoolMap: map[string]string{
				"host1": "pool0",
				"host2": "pool1",
				"host3": "pool1",
				"host4": "default",
			},
			expectedNewHost:   "host4",
			expectedNumEvents: 1,
		},
		{
			tcName: "host-removed-from-db",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool1"},
			},
			expectedhostToPoolMap: map[string]string{
				"host1": "pool0",
				"host2": "pool1",
			},
			expectedNumEvents: 1,
		},
		{
			tcName: "host-in-different-pool",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool0"},
				{Hostname: "host3", CurrentPool: "pool1"},
			},
			expectedhostToPoolMap: map[string]string{
				"host1": "pool0",
				"host2": "pool0",
				"host3": "pool1",
			},
			expectedNumEvents: 1,
		},
		{
			tcName: "new-pool-in-db",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool1"},
				{Hostname: "host3", CurrentPool: "pool2"},
			},
			expectedhostToPoolMap: map[string]string{
				"host1": "pool0",
				"host2": "pool1",
				"host3": "pool2",
			},
			expectedNumEvents: 1,
		},
		{
			tcName: "pool-with-no-host-in-db",
			dbHosts: []*pbhost.HostInfo{
				{Hostname: "host1", CurrentPool: "pool0"},
				{Hostname: "host2", CurrentPool: "pool0"},
				{Hostname: "host3", CurrentPool: "pool0"},
			},
			expectedhostToPoolMap: map[string]string{
				"host1": "pool0",
				"host2": "pool0",
				"host3": "pool0",
			},
			expectedNumEvents: 2,
		},
	}

	for _, tc := range testCases {
		tcName := tc.tcName
		suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).
			Return(tc.dbHosts, nil)
		if tc.expectedNewHost != "" {
			suite.mockHostInfoOps.EXPECT().UpdatePool(
				gomock.Any(),
				tc.expectedNewHost,
				common.DefaultHostPoolID,
				common.DefaultHostPoolID).
				Return(nil)
		}
		manager := setupTestManager(
			poolIndex,
			hostToPoolMap,
			suite.eventStreamHandler,
			suite.mockHostInfoOps)
		mgr := manager.(*hostPoolManager)
		// flush pending events
		preEvents, _ := suite.eventStreamHandler.GetEvents()

		suite.NoError(mgr.reconcile(), tcName)

		suite.checkHostPoolMap(
			tc.expectedhostToPoolMap,
			mgr,
			"reconcile: %s",
			tcName)
		suite.checkPoolIndexConsistency(mgr, "reconcile: %s", tcName)
		suite.checkPoolCapacity(mgr, "reconcile: %s", tcName)

		// Check all the old pools are still present.
		pools := mgr.Pools()
		for poolID := range poolIndex {
			suite.Contains(pools, poolID, "reconcile: %s: %s", poolID, tcName)
		}

		// Check event published.
		events, err := suite.eventStreamHandler.GetEvents()
		suite.NoError(err, tcName)
		suite.Equal(
			tc.expectedNumEvents,
			len(events)-len(preEvents),
			"reconcile: %s: %s",
			tcName, events)
	}
}

func (suite *HostPoolManagerTestSuite) TestReconcileError() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	expectedErr := errors.New("something bad")
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).
		Return(nil, expectedErr)

	manager := setupTestManager(
		make(map[string][]string),
		make(map[string]string),
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
	)
	suite.Error(manager.(*hostPoolManager).reconcile())
}

func (suite *HostPoolManagerTestSuite) TestReconcileUpdateError() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	dbHosts := []*pbhost.HostInfo{
		{Hostname: "host0"},
	}
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).
		Return(dbHosts, nil)
	expectedErr := errors.New("something bad")
	suite.mockHostInfoOps.EXPECT().UpdatePool(
		gomock.Any(),
		"host0",
		common.DefaultHostPoolID,
		common.DefaultHostPoolID).
		Return(expectedErr)

	manager := setupTestManager(
		make(map[string][]string),
		make(map[string]string),
		suite.eventStreamHandler,
		suite.mockHostInfoOps,
	)
	suite.NoError(manager.(*hostPoolManager).reconcile())
	_, err := manager.GetPoolByHostname("host1")
	suite.Error(err)
}

func (suite *HostPoolManagerTestSuite) checkHostPoolMap(
	expected map[string]string,
	mgr *hostPoolManager,
	msgFormat string,
	msgArgs ...interface{},
) {
	msg := fmt.Sprintf(msgFormat, msgArgs...)
	suite.Equal(len(expected), len(mgr.hostToPoolMap), msg)
	for hostname, expectedPool := range expected {
		actualPool, err := mgr.GetPoolByHostname(hostname)
		suite.NoError(err, "%s: %s", hostname, msg)
		if err == nil {
			suite.Equal(expectedPool, actualPool.ID(), "%s: %s", hostname, msg)
		}
	}
}

// Checks that poolIndex is consistent with hostPoolMap.
func (suite *HostPoolManagerTestSuite) checkPoolIndexConsistency(
	mgr *hostPoolManager,
	msgFormat string,
	msgArgs ...interface{},
) {
	msg := fmt.Sprintf(msgFormat, msgArgs...)
	for poolID, pool := range mgr.Pools() {
		for hostname := range pool.Hosts() {
			actualPool, err := mgr.GetPoolByHostname(hostname)
			suite.NoError(err, "%s: %s", hostname, msg)
			if err == nil {
				suite.Equal(poolID, actualPool.ID(), "%s: %s", hostname, msg)
			}
		}
	}
}

// TestRefreshPoolCapacity verifies that pool capacities are updated correctly.
func (suite *HostPoolManagerTestSuite) TestRefreshPoolCapacity() {
	poolIndex := map[string][]string{
		"pool0": {"host1", "host4", "host5"},
		"pool1": {"host2", "host3"},
	}
	hostToPoolMap := map[string]string{
		"host1": "pool0",
		"host2": "pool1",
		"host3": "pool1",
		"host4": "pool0",
		"host5": "pool0",
	}
	manager := setupTestManager(
		poolIndex,
		hostToPoolMap,
		suite.eventStreamHandler,
		suite.mockHostInfoOps)
	mgr := manager.(*hostPoolManager)

	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	loader := suite.setupAgentMapLoader(ctrl)
	loader.Load(nil)
	host.GetAgentMap().HostCapacities = map[string]*host.ResourceCapacity{
		"host1": suite.hostCapacity,
		"host2": suite.hostCapacity,
		"host3": suite.hostCapacity,
		"host4": suite.hostCapacity,
		"host5": suite.hostCapacity,
	}

	mgr.refreshPoolCapacity()
	suite.checkPoolCapacity(mgr, "refreshPoolCapacity")
}

// Checks that the capacities of all Pools is as expected.
func (suite *HostPoolManagerTestSuite) checkPoolCapacity(
	mgr *hostPoolManager,
	msgFormat string,
	msgArgs ...interface{},
) {
	msg := fmt.Sprintf(msgFormat, msgArgs...)
	for poolID, pool := range mgr.Pools() {
		numHosts := float64(len(pool.Hosts()))
		expected := host.ResourceCapacity{
			Physical: scalar.Resources{
				CPU:  numHosts * float64(2.0),
				Mem:  numHosts * float64(4.0),
				Disk: numHosts * float64(10.0),
				GPU:  numHosts * float64(1.0),
			},
			Slack: scalar.Resources{
				CPU: numHosts * float64(1.5),
			},
		}
		suite.Equal(expected, pool.Capacity(), "%s: %s", poolID, msg)
	}
}

// TestChangeHostPool tests various cases of changing pool for a host.
func (suite *HostPoolManagerTestSuite) TestChangeHostPool() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	loader := suite.setupAgentMapLoader(ctrl)
	loader.Load(nil)

	host.GetAgentMap().HostCapacities = map[string]*host.ResourceCapacity{
		"host2": suite.hostCapacity,
		"host3": suite.hostCapacity,
	}

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
		srcPoolID, destPoolID, host string
		updated, isErr              bool
		updatedErr                  error
		srcCapacity, dstCapacity    host.ResourceCapacity
	}{
		"success": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool3",
			host:       "host2",
			updated:    true,
			dstCapacity: host.ResourceCapacity{
				Physical: scalar.Resources{
					CPU:  2.0 * float64(2.0),
					Mem:  2.0 * float64(4.0),
					Disk: 2.0 * float64(10.0),
					GPU:  2.0 * float64(1.0),
				},
				Slack: scalar.Resources{
					CPU: 2.0 * float64(1.5),
				},
			},
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
		"update-host-info-failure": {
			pools:      poolIndex,
			srcPoolID:  "pool2",
			destPoolID: "pool3",
			host:       "host2",
			updated:    true,
			isErr:      true,
			updatedErr: errors.New("some bad things happened"),
		},
	}

	for tcName, tc := range testCases {
		if tc.updated {
			suite.mockHostInfoOps.EXPECT().UpdatePool(
				gomock.Any(),
				tc.host,
				tc.destPoolID,
				tc.destPoolID,
			).Return(tc.updatedErr)
		}

		suite.manager = setupTestManager(
			tc.pools,
			hostToPoolMap,
			suite.eventStreamHandler,
			suite.mockHostInfoOps,
		)

		err := suite.manager.ChangeHostPool(tc.host, tc.srcPoolID, tc.destPoolID)
		if tc.isErr {
			suite.Error(err, tcName)
		} else {
			suite.NoError(err, tcName)

			p, err := suite.manager.GetPoolByHostname(tc.host)
			suite.NoError(err, tcName)
			suite.Equal(tc.destPoolID, p.ID())

			if tc.destPoolID != tc.srcPoolID {
				destPool, err := suite.manager.GetPool(tc.destPoolID)
				suite.NoError(err, tcName)
				suite.Contains(destPool.Hosts(), tc.host, tcName)

				srcPool, err := suite.manager.GetPool(tc.srcPoolID)
				suite.NoError(err, tcName)
				suite.NotContains(srcPool.Hosts(), tc.host, tcName)

				// check pool capacity change
				suite.Equal(tc.srcCapacity, srcPool.Capacity(), tcName)
				suite.Equal(tc.dstCapacity, destPool.Capacity(), tcName)

				// check event
				events, err := suite.eventStreamHandler.GetEvents()
				suite.NoError(err, tcName)
				suite.Equal(1, len(events))
				suite.Equal(
					pbeventstream.Event_HOST_EVENT,
					events[0].GetType())
				suite.Equal(tc.host, events[0].GetHostEvent().GetHostname())
				suite.Equal(
					pbhost.HostEvent_TYPE_HOST_POOL,
					events[0].GetHostEvent().GetType())
				suite.Equal(
					tc.destPoolID,
					events[0].GetHostEvent().GetHostPoolEvent().GetPool())
			}
		}
	}
}

func (suite *HostPoolManagerTestSuite) TestGetDesiredPool() {
	testCases := map[string]struct {
		hostname       string
		hostInfo       *pbhost.HostInfo
		getHostInfoErr error
		expectedPool   string
		expectedErrMsg string
	}{
		"success": {
			hostname: "host1",
			hostInfo: &pbhost.HostInfo{
				DesiredPool: "pool1",
			},
			expectedPool: "pool1",
		},
		"empty-hostname": {
			expectedErrMsg: "hostname is empty",
		},
		"get-host-info-failure": {
			hostname:       "host1",
			getHostInfoErr: errors.New("some bad things happened"),
			expectedErrMsg: "some bad things happened",
		},
	}

	for tcName, tc := range testCases {
		if len(tc.hostname) > 0 {
			suite.mockHostInfoOps.EXPECT().Get(
				gomock.Any(),
				tc.hostname,
			).Return(tc.hostInfo, tc.getHostInfoErr)
		}

		pool, err := suite.manager.GetDesiredPool(tc.hostname)
		if len(tc.expectedErrMsg) > 0 {
			suite.EqualError(
				err, tc.expectedErrMsg, "test case: %s", tcName)
		} else {
			suite.NoError(err, "test case: %s", tcName)
		}
		suite.Equal(tc.expectedPool, pool, "test case: %s", tcName)
	}
}

func (suite *HostPoolManagerTestSuite) TestUpdateDesiredPool() {
	testCases := map[string]struct {
		hostname          string
		poolID            string
		updateHostInfoErr error
		expectedErrMsg    string
	}{
		"success": {
			hostname: "host1",
			poolID:   "pool1",
		},
		"empty-hostname": {
			expectedErrMsg: "hostname is empty",
		},
		"empty-poolID": {
			hostname:       "host1",
			expectedErrMsg: "poolID is empty",
		},
		"update-host-info-failure": {
			hostname:          "host1",
			poolID:            "pool1",
			updateHostInfoErr: errors.New("some bad things happened"),
			expectedErrMsg:    "some bad things happened",
		},
	}

	for tcName, tc := range testCases {
		if len(tc.hostname) > 0 && len(tc.poolID) > 0 {
			suite.mockHostInfoOps.EXPECT().UpdateDesiredPool(
				gomock.Any(),
				tc.hostname,
				tc.poolID,
			).Return(tc.updateHostInfoErr)
		}

		err := suite.manager.UpdateDesiredPool(tc.hostname, tc.poolID)
		if len(tc.expectedErrMsg) > 0 {
			suite.EqualError(
				err, tc.expectedErrMsg, "test case: %s", tcName)
		} else {
			suite.NoError(err, "test case: %s", tcName)
		}
	}
}

// makeAgentsResponse makes a fake GetAgents response from Mesos master.
func (suite *HostPoolManagerTestSuite) makeAgentsResponse() *pbmesosmaster.Response_GetAgents {
	response := &pbmesosmaster.Response_GetAgents{
		Agents: []*pbmesosmaster.Response_GetAgents_Agent{},
	}
	pidUp := fmt.Sprintf("slave(0)@%s:0.0.0.0", suite.upMachines[0].GetIp())
	hostnameUp := suite.upMachines[0].GetHostname()
	agentUp := &pbmesosmaster.Response_GetAgents_Agent{
		AgentInfo: &pbmesos.AgentInfo{
			Hostname: &hostnameUp,
		},
		Pid: &pidUp,
	}
	response.Agents = append(response.Agents, agentUp)

	pidDraining := fmt.Sprintf(
		"slave(0)@%s:0.0.0.0", suite.drainingMachines[0].GetIp())
	hostnameDraining := suite.drainingMachines[0].GetHostname()
	agentDraining := &pbmesosmaster.Response_GetAgents_Agent{
		AgentInfo: &pbmesos.AgentInfo{
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
	mockHostInfoOps *orm_mocks.MockHostInfoOps,
) HostPoolManager {
	scope := tally.NoopScope
	manager := &hostPoolManager{
		reconcileInternal:  _testReconcileInterval,
		eventStreamHandler: eventStreamHandler,
		hostInfoOps:        mockHostInfoOps,
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

// setupAgentMapLoader creates a host.Loader with the proper mocks.
func (suite *HostPoolManagerTestSuite) setupAgentMapLoader(
	ctrl *gomock.Controller,
) *host.Loader {
	mockMasterOperatorClient := mpbmocks.NewMockMasterOperatorClient(suite.ctrl)

	response := suite.makeAgentsResponse()
	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	mockMasterOperatorClient.EXPECT().Agents().Return(response, nil)
	mockMasterOperatorClient.EXPECT().GetMaintenanceStatus().Return(nil, nil)
	for _, a := range response.GetAgents() {
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

	loader := &host.Loader{
		OperatorClient: mockMasterOperatorClient,
		Scope:          tally.NewTestScope("", map[string]string{}),
		HostInfoOps:    suite.mockHostInfoOps,
	}
	return loader
}
