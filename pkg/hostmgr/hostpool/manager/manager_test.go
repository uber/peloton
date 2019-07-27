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

	"github.com/uber/peloton/pkg/hostmgr/hostpool"

	"github.com/stretchr/testify/suite"
)

const (
	_testPoolIDTemplate = "pool%d"
)

// HostPoolManagerTestSuite is test suite for host pool manager.
type HostPoolManagerTestSuite struct {
	suite.Suite

	manager HostPoolManager
}

// SetupTest is setup function for this suite.
func (suite *HostPoolManagerTestSuite) SetupTest() {
	suite.manager = New()
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
			poolIndex:      map[string][]string{},
			hostToPoolMap:  map[string]string{},
			expectedErrMsg: fmt.Sprintf("host %s not found", testHostname),
		},
		"host-pool-not-found": {
			poolIndex: map[string][]string{
				"pool2": {testHostname},
			},
			hostToPoolMap: map[string]string{
				testHostname: expectedPoolID,
			},
			expectedErrMsg: fmt.Sprintf("host pool %s not found", expectedPoolID),
		},
	}

	for tcName, tc := range testCases {
		manager := setupTestManager(tc.poolIndex, tc.hostToPoolMap)

		_, err := manager.GetPoolByHostname(testHostname)
		if tc.expectedErrMsg != "" {
			suite.EqualError(err, tc.expectedErrMsg, "test case %s", tcName)
		} else {
			suite.NoErrorf(err, "test case %s", tcName)
		}
	}
}

// TestRegisterPool tests creating a host pool with given pool ID in parallel.
// It pre-registers 10 test host pools to the host manager and tests with
// another 20 host pool registration attempts, 10 of them were pre-registered already.
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
			notFoundErrMsg := fmt.Sprintf("host pool %s not found", poolID)
			suite.manager.DeregisterPool(poolID)
			_, err := suite.manager.GetPool(poolID)
			suite.EqualError(err, notFoundErrMsg)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

// setupTestManager set up test host manager by constructing
// a new host pool manager with given pool index and host index.
func setupTestManager(
	poolIndex map[string][]string,
	hostToPoolMap map[string]string,
) HostPoolManager {
	manager := &hostPoolManager{
		poolIndex:     map[string]hostpool.HostPool{},
		hostToPoolMap: map[string]string{},
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

// preRegisterTestPools creates given number of test host pools to given host pool manager
// with ID following the pattern as 'poolX'.
func preRegisterTestPools(manager HostPoolManager, numPools int) {
	for i := 0; i < numPools; i++ {
		manager.RegisterPool(fmt.Sprintf(_testPoolIDTemplate, i))
	}
}
