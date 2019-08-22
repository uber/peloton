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

package hostpool

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

// HostPoolTestSuite is test suite for host pool.
type HostPoolTestSuite struct {
	suite.Suite

	hp HostPool
}

// SetupTest is setup function for this suite.
func (suite *HostPoolTestSuite) SetupTest() {
	suite.hp = New("test_pool", tally.NoopScope)
}

// TestHostPoolTestSuite runs HostPoolTestSuite.
func TestHostPoolTestSuite(t *testing.T) {
	suite.Run(t, new(HostPoolTestSuite))
}

// TestAddHost tests adding a host to host pool.
func (suite *HostPoolTestSuite) TestAddHost() {
	testHostname := "host1"

	testCases := map[string]struct {
		hosts    []string
		poolSize int
	}{
		"host-not-found": {
			hosts:    []string{},
			poolSize: 1,
		},
		"host-found": {
			hosts:    []string{testHostname},
			poolSize: 1,
		},
	}

	for tcName, tc := range testCases {
		setupTestPool(suite.hp, tc.hosts)

		suite.hp.Add(testHostname)

		hosts := suite.hp.Hosts()
		_, ok := hosts[testHostname]
		suite.Equal(tc.poolSize, len(hosts), "test case %s", tcName)
		suite.Equal(true, ok, "test case %s", tcName)
	}
}

// TestDeleteHost tests deleting a host from host pool.
func (suite *HostPoolTestSuite) TestDeleteHost() {
	testHostname := "host1"

	testCases := map[string]struct {
		hosts    []string
		poolSize int
	}{
		"host-found": {
			hosts:    []string{testHostname},
			poolSize: 0,
		},
		"host-not-found": {
			hosts:    []string{},
			poolSize: 0,
		},
	}

	for tcName, tc := range testCases {
		setupTestPool(suite.hp, tc.hosts)

		suite.hp.Delete(testHostname)

		hosts := suite.hp.Hosts()
		_, ok := hosts[testHostname]
		suite.Equal(tc.poolSize, len(hosts), "test case %s", tcName)
		suite.Equal(false, ok, "test case %s", tcName)
	}
}

// setupTestPool set up test host pool by adding given hosts to the test host pool.
func setupTestPool(pool HostPool, hosts []string) {
	for _, host := range hosts {
		pool.Add(host)
	}
}
