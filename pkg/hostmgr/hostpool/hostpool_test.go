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

	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

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

// TestRefreshCapacity tests updating the pool capacity.
func (suite *HostPoolTestSuite) TestRefreshCapacity() {
	hosts := []string{"h1", "h2", "h3"}
	setupTestPool(suite.hp, hosts)

	phy := scalar.Resources{
		CPU:  float64(2.0),
		Mem:  float64(4.0),
		Disk: float64(10.0),
		GPU:  float64(1.0),
	}
	slack := scalar.Resources{
		CPU: float64(1.5),
	}
	rc := map[string]*host.ResourceCapacity{
		"h1": {Physical: phy, Slack: slack},
		"h2": {Physical: phy, Slack: slack},
		"h3": {Physical: phy, Slack: slack},
	}

	testCases := map[string]struct {
		hostCapacities map[string]*host.ResourceCapacity
		expected       host.ResourceCapacity
	}{
		"host-found": {
			hostCapacities: rc,
			expected: host.ResourceCapacity{
				Physical: scalar.Resources{
					CPU:  3.0 * float64(2.0),
					Mem:  3.0 * float64(4.0),
					Disk: 3.0 * float64(10.0),
					GPU:  3.0 * float64(1.0),
				},
				Slack: scalar.Resources{
					CPU: 3.0 * float64(1.5),
				},
			},
		},
		"host-not-found": {},
	}

	for tcName, tc := range testCases {
		suite.hp.RefreshCapacity(tc.hostCapacities)
		suite.Equal(tc.expected, suite.hp.Capacity(), tcName)
	}
}

// setupTestPool set up test host pool by adding given hosts to the test host pool.
func setupTestPool(pool HostPool, hosts []string) {
	for _, host := range hosts {
		pool.Add(host)
	}
}
