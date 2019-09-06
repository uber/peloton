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

package hostmgrsvc

import (
	"fmt"
	"strings"
	"testing"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/hostsummary"
	hostsummary_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/hostsummary/mocks"
	hostcache_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/mocks"
	plugins_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/plugins/mocks"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"golang.org/x/net/context"
)

const (
	_perHostCPU = 10.0
	_perHostMem = 20.0
)

var (
	rootCtx    = context.Background()
	_testJobID = uuid.New()
	_podIDFmt  = _testJobID + "-%d"
)

// generate launchable pod specs
func generateLaunchablePods(numPods int) []*hostmgr.LaunchablePod {
	var pods []*hostmgr.LaunchablePod
	for i := 0; i < numPods; i++ {
		pods = append(pods, &hostmgr.LaunchablePod{
			PodId: &peloton.PodID{Value: fmt.Sprintf(_podIDFmt, i)},
			Spec: &pbpod.PodSpec{
				Containers: []*pbpod.ContainerSpec{
					{
						Name: uuid.New(),
						Resource: &pbpod.ResourceSpec{
							CpuLimit:   1.0,
							MemLimitMb: 100.0,
						},
						Ports: []*pbpod.PortSpec{
							{
								Name:  "port",
								Value: 80,
							},
						},
						Image: "nginx",
					},
				},
			},
		})
	}
	return pods
}

// HostMgrHandlerTestSuite tests the v1alpha service handler for hostmgr
type HostMgrHandlerTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testScope tally.TestScope
	hostCache *hostcache_mocks.MockHostCache
	plugin    *plugins_mocks.MockPlugin
	handler   *ServiceHandler
}

// SetupTest sets up tests in HostMgrHandlerTestSuite
func (suite *HostMgrHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.testScope = tally.NewTestScope("", map[string]string{})

	suite.plugin = plugins_mocks.NewMockPlugin(suite.ctrl)
	suite.hostCache = hostcache_mocks.NewMockHostCache(suite.ctrl)

	suite.handler = &ServiceHandler{
		plugin:    suite.plugin,
		hostCache: suite.hostCache,
	}
}

// TearDownTest tears down tests in HostMgrHandlerTestSuite
func (suite *HostMgrHandlerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

// TestGetHostCache tests the GetHostCache API.
func (suite *HostMgrHandlerTestSuite) TestGetHostCache() {
	defer suite.ctrl.Finish()

	allocated := scalar.Resources{
		CPU:  1,
		Mem:  10,
		Disk: 100,
		GPU:  2,
	}
	capacity := scalar.Resources{
		CPU:  2,
		Mem:  20,
		Disk: 150,
		GPU:  3,
	}

	summary := hostsummary_mocks.NewMockHostSummary(suite.ctrl)
	summary.EXPECT().GetHostname().Return("h1")
	summary.EXPECT().GetHostStatus().Return(hostsummary.ReadyHost)
	summary.EXPECT().GetAllocated().Return(models.HostResources{
		NonSlack: allocated,
	})
	summary.EXPECT().GetCapacity().Return(models.HostResources{
		NonSlack: capacity,
	})

	suite.hostCache.EXPECT().
		GetSummaries().
		Return([]hostsummary.HostSummary{summary})

	resp, err := suite.handler.GetHostCache(rootCtx, nil)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Len(resp.Summaries, 1)
	suite.Equal(&svc.GetHostCacheResponse_Summary{
		Hostname: "h1",
		Status:   "1",
		Allocation: []*hostmgr.Resource{
			{Kind: "cpu", Capacity: 1},
			{Kind: "mem", Capacity: 10},
			{Kind: "disk", Capacity: 100},
			{Kind: "gpu", Capacity: 2},
		},
		Capacity: []*hostmgr.Resource{
			{Kind: "cpu", Capacity: 2},
			{Kind: "mem", Capacity: 20},
			{Kind: "disk", Capacity: 150},
			{Kind: "gpu", Capacity: 3},
		},
	}, resp.Summaries[0])
}

// TestAcquireHosts tests AcquireHosts API
func (suite *HostMgrHandlerTestSuite) TestAcquireHosts() {
	defer suite.ctrl.Finish()

	testTable := map[string]struct {
		filter       *hostmgr.HostFilter
		filterResult map[string]uint32
		leases       []*hostmgr.HostLease
		errMsg       string
	}{
		"acquire-success": {
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pbpod.ResourceSpec{
						CpuLimit:   _perHostCPU,
						MemLimitMb: _perHostMem,
					},
				},
			},
			filterResult: map[string]uint32{
				strings.ToLower("HOST_FILTER_MATCH"): 1,
			},
			leases: []*hostmgr.HostLease{
				{
					HostSummary: &pbhost.HostSummary{
						Hostname: "test",
					},
					LeaseId: &hostmgr.LeaseID{
						Value: uuid.New(),
					},
				},
			},
		},
	}
	for ttName, tt := range testTable {
		req := &svc.AcquireHostsRequest{
			Filter: tt.filter,
		}
		suite.hostCache.EXPECT().
			AcquireLeases(tt.filter).
			Return(tt.leases, tt.filterResult)

		resp, err := suite.handler.AcquireHosts(rootCtx, req)
		if tt.errMsg != "" {
			suite.Equal(tt.errMsg, err.Error(), "test case %s", ttName)
			continue
		}
		suite.NoError(err, "test case %s", ttName)
		suite.Equal(tt.leases, resp.GetHosts())
		suite.Equal(tt.filterResult, resp.GetFilterResultCounts())
	}
}

// TestLaunchPods tests LaunchPods API
func (suite *HostMgrHandlerTestSuite) TestLaunchPods() {
	defer suite.ctrl.Finish()

	testTable := map[string]struct {
		errMsg         string
		launchablePods []*hostmgr.LaunchablePod
		leaseID        *hostmgr.LeaseID
		hostname       string
	}{
		"launch-pods-success": {
			launchablePods: generateLaunchablePods(10),
			hostname:       "host-name",
			leaseID:        &hostmgr.LeaseID{Value: uuid.New()},
		},
	}
	for ttName, tt := range testTable {
		req := &svc.LaunchPodsRequest{
			LeaseId:  tt.leaseID,
			Hostname: tt.hostname,
			Pods:     tt.launchablePods,
		}
		var launchablePods []*models.LaunchablePod
		var released []*peloton.PodID
		for _, pod := range tt.launchablePods {
			launchablePods = append(launchablePods, &models.LaunchablePod{
				PodId: pod.GetPodId(),
				Spec:  pod.GetSpec(),
				Ports: pod.GetPorts(),
			})
			released = append(released, pod.GetPodId())
			suite.hostCache.EXPECT().
				GetHostHeldForPod(pod.GetPodId()).
				Return("different-host-name")
		}

		suite.hostCache.EXPECT().
			ReleaseHoldForPods("different-host-name", released).
			Return(nil)

		suite.hostCache.EXPECT().
			CompleteLease(tt.hostname, tt.leaseID.GetValue(), gomock.Any()).
			Return(nil)

		suite.plugin.
			EXPECT().
			LaunchPods(gomock.Any(), launchablePods, tt.hostname).
			Return(nil, nil)

		resp, err := suite.handler.LaunchPods(rootCtx, req)
		if tt.errMsg != "" {
			suite.Equal(tt.errMsg, err.Error(), "test case %s", ttName)
			continue
		}
		suite.NoError(err, "test case %s", ttName)
		suite.Equal(&svc.LaunchPodsResponse{}, resp)
	}
}

// TestLaunchPods tests LaunchPods API fails due to plugin error
func (suite *HostMgrHandlerTestSuite) TestLaunchPodsPluginFailure() {
	defer suite.ctrl.Finish()

	hostname := "host-name"
	leaseID := &hostmgr.LeaseID{Value: uuid.New()}
	pods := generateLaunchablePods(10)

	req := &svc.LaunchPodsRequest{
		LeaseId:  leaseID,
		Hostname: hostname,
		Pods:     pods,
	}

	suite.hostCache.EXPECT().
		GetHostHeldForPod(gomock.Any()).
		Return(hostname).Times(len(pods))

	suite.hostCache.EXPECT().
		CompleteLease(hostname, leaseID.GetValue(), gomock.Any()).
		Return(nil)

	var launchablePods []*models.LaunchablePod
	for _, pod := range pods {
		launchablePods = append(launchablePods, &models.LaunchablePod{
			PodId: pod.GetPodId(),
			Spec:  pod.GetSpec(),
		})
	}

	suite.plugin.
		EXPECT().
		LaunchPods(gomock.Any(), launchablePods, hostname).
		Return(nil, errors.New("test error"))

	resp, err := suite.handler.LaunchPods(rootCtx, req)
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *HostMgrHandlerTestSuite) TestTerminateLease() {
	defer suite.ctrl.Finish()

	testTable := map[string]struct {
		errMsg   string
		leaseID  *hostmgr.LeaseID
		hostname string
	}{
		"terminate-leases-success": {
			hostname: "host-name",
			leaseID:  &hostmgr.LeaseID{Value: uuid.New()},
		},
	}
	for ttName, tt := range testTable {
		req := &svc.TerminateLeasesRequest{
			Leases: []*svc.TerminateLeasesRequest_LeasePair{
				{
					Hostname: tt.hostname,
					LeaseId:  tt.leaseID,
				},
			},
		}

		suite.hostCache.EXPECT().
			TerminateLease(tt.hostname, tt.leaseID.GetValue()).
			Return(nil)

		resp, err := suite.handler.TerminateLeases(rootCtx, req)
		if tt.errMsg != "" {
			suite.Equal(tt.errMsg, err.Error(), "test case %s", ttName)
			continue
		}
		suite.NoError(err, "test case %s", ttName)
		suite.Equal(&svc.TerminateLeasesResponse{}, resp)
	}
}

func (suite *HostMgrHandlerTestSuite) TestKillPods() {
	defer suite.ctrl.Finish()

	var pods []*peloton.PodID
	for i := 0; i < 10; i++ {
		pods = append(pods, &peloton.PodID{Value: uuid.New()})
	}
	req := &svc.KillPodsRequest{
		PodIds: pods,
	}

	for _, pod := range pods {
		suite.plugin.
			EXPECT().
			KillPod(gomock.Any(), pod.GetValue()).
			Return(nil)
	}
	resp, err := suite.handler.KillPods(rootCtx, req)
	suite.NoError(err)
	suite.Equal(&svc.KillPodsResponse{}, resp)
}

func (suite *HostMgrHandlerTestSuite) TestKillAndHoldPods() {
	defer suite.ctrl.Finish()

	badHost := "badhost"
	hosts := []string{"host1", badHost, "host3"}
	var entries []*svc.KillAndHoldPodsRequest_Entry
	podsMap := make(map[string][]*peloton.PodID)
	for i := 0; i < 10; i++ {
		pod := &peloton.PodID{Value: uuid.New()}
		entries = append(entries,
			&svc.KillAndHoldPodsRequest_Entry{
				PodId:      pod,
				HostToHold: hosts[i%len(hosts)],
			})
		podsMap[hosts[i%len(hosts)]] = append(podsMap[hosts[i%len(hosts)]], pod)
	}
	req := &svc.KillAndHoldPodsRequest{
		Entries: entries,
	}

	for host, pods := range podsMap {
		suite.hostCache.
			EXPECT().
			HoldForPods(host, pods).
			Return(nil)
	}
	for host, pods := range podsMap {
		var err error
		if host == badHost {
			err = fmt.Errorf("kill pod failure")
		}
		for _, pod := range pods {
			suite.plugin.
				EXPECT().
				KillPod(gomock.Any(), pod.GetValue()).
				Return(err)
		}
	}
	suite.hostCache.
		EXPECT().
		ReleaseHoldForPods(badHost, podsMap[badHost]).
		Return(nil)

	_, err := suite.handler.KillAndHoldPods(rootCtx, req)
	suite.Error(err)
}

// TestHostManagerTestSuite runs the HostMgrHandlerTestSuite
func TestHostManagerTestSuite(t *testing.T) {
	suite.Run(t, new(HostMgrHandlerTestSuite))
}
