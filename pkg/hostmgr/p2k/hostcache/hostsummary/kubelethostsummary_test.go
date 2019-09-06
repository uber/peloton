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

package hostsummary

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/pkg/hostmgr/models"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	"go.uber.org/atomic"
)

// TestKubeletHostSummarySetCapacity
// test set capacity would update available resources
func (suite *HostSummaryTestSuite) TestKubeletHostSummarySetCapacity() {
	testTable := map[string]struct {
		capacity          scalar.Resources
		allocated         scalar.Resources
		expectedAvailable scalar.Resources
	}{
		"normal-test-case-1": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			allocated: scalar.Resources{
				CPU: 1.0,
				Mem: 20.0,
			},
			expectedAvailable: scalar.Resources{
				CPU: 3.0,
				Mem: 80.0,
			},
		},
		"normal-test-case-2": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			allocated: scalar.Resources{
				CPU: 3.0,
				Mem: 10.0,
			},
			expectedAvailable: scalar.Resources{
				CPU: 1.0,
				Mem: 90.0,
			},
		},
		"all-capacity-allocated": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			allocated: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			expectedAvailable: scalar.Resources{
				CPU: 0.0,
				Mem: 0.0,
			},
		},
		"allocated-more-than-capacity": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			allocated: scalar.Resources{
				CPU: 10.0,
				Mem: 200.0,
			},
			expectedAvailable: scalar.Resources{
				CPU: 0.0,
				Mem: 0.0,
			},
		},
		"allocated-partial-resources-more-than-capacity": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			allocated: scalar.Resources{
				CPU: 1.0,
				Mem: 200.0,
			},
			expectedAvailable: scalar.Resources{
				CPU: 0.0,
				Mem: 0.0,
			},
		},
	}

	for _, test := range testTable {
		ks := NewKubeletHostSummary(_hostname, models.HostResources{}, _version)
		ks.(*kubeletHostSummary).allocated.NonSlack = test.allocated
		ks.SetCapacity(models.HostResources{
			Slack:    scalar.Resources{},
			NonSlack: test.capacity,
		})
		suite.Equal(ks.GetCapacity().NonSlack, test.capacity)
		suite.Equal(ks.GetAllocated().NonSlack, test.allocated)
		suite.Equal(ks.GetAvailable().NonSlack, test.expectedAvailable)
	}
}

// TestKubeletHostSummarySetAvailable tests set
// available is noop for k8s host
func (suite *HostSummaryTestSuite) TestKubeletHostSummarySetAvailable() {
	ks := NewKubeletHostSummary(_hostname, models.HostResources{}, _version)

	oldAvailable := scalar.Resources{CPU: 2.0}
	newAvailable := scalar.Resources{CPU: 4.0}

	ks.(*kubeletHostSummary).available.NonSlack = oldAvailable
	ks.SetAvailable(models.HostResources{
		Slack:    scalar.Resources{},
		NonSlack: newAvailable,
	})

	suite.Equal(ks.GetAvailable().NonSlack, oldAvailable)
}

// TestKubeletHostSummaryHandlePodEvent makes sure that we release the pod resources when we
// get a pod deleted event.
// In this test we acquire the host that has room for 1 pod, handle a delete
// event, and make sure we can re-acquire that host for another pod.
// This test is run with 100 goroutines trying to acquire the host, and once
// one of the thread succeeds, we send a DeletePod event to notify that the
// pod ran to completion and was deleted.
// Then we expect another thread to acquire that host successfully.
// We should count 2 total successful matches.
func (suite *HostSummaryTestSuite) TestKubeletHostSummaryHandlePodEvent() {
	s := NewKubeletHostSummary(_hostname, models.HostResources{}, _version).(*kubeletHostSummary)
	s.status = ReadyHost
	s.allocated.NonSlack = CreateResource(0, 0)
	s.SetCapacity(models.HostResources{
		NonSlack: _capacity,
	})

	filter := &hostmgr.HostFilter{
		ResourceConstraint: &hostmgr.ResourceConstraint{
			Minimum: &pod.ResourceSpec{
				CpuLimit:   9.0,
				MemLimitMb: 90.0,
			},
		},
	}

	specMap := map[string]*pbpod.PodSpec{
		"podid1": {
			Containers: []*pbpod.ContainerSpec{
				{Resource: &pbpod.ResourceSpec{
					CpuLimit:   9.0,
					MemLimitMb: 90.0,
				}},
			},
		},
	}

	matches, failures := atomic.NewInt32(0), atomic.NewInt32(0)
	eventHandled := atomic.NewBool(false)

	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				match := s.TryMatch(filter)
				if match.Result != hostmgr.HostFilterResult_HOST_FILTER_MATCH {
					failures.Inc()
					runtime.Gosched()
					continue
				}
				matches.Inc()
				err := s.CompleteLease(s.leaseID, specMap)
				suite.NoError(err)

				if old := eventHandled.Swap(true); old {
					continue
				}

				evt := &p2kscalar.PodEvent{
					EventType: p2kscalar.DeletePod,
					Event: &pod.PodEvent{
						PodId: &peloton.PodID{
							Value: "podid1",
						},
					},
				}
				s.HandlePodEvent(evt)
			}
		}()
	}

	wg.Wait()
	suite.Equal(int32(2), matches.Load())
	suite.Equal(int32(998), failures.Load())
}

func (suite *HostSummaryTestSuite) TestCompleteLaunchPod() {
	s := NewKubeletHostSummary(_hostname, models.HostResources{}, _version)
	s.CompleteLaunchPod(&models.LaunchablePod{
		PodId: &peloton.PodID{Value: "podid1"},
		Spec:  &pod.PodSpec{},
		Ports: map[string]uint32{"p1": 31001, "p2": 31010},
	})
	hl := s.GetHostLease()
	equalPortRanges(suite.T(), hl.HostSummary.AvailablePorts, 31000, 31000, 31002, 31009, 31011, 32000)

	evt := &p2kscalar.PodEvent{
		EventType: p2kscalar.DeletePod,
		Event: &pod.PodEvent{
			PodId: &peloton.PodID{Value: "podid1"},
			ContainerStatus: []*pod.ContainerStatus{
				{Ports: map[string]uint32{"p0": 31010}},
			},
		},
	}
	s.HandlePodEvent(evt)
	hl = s.GetHostLease()
	equalPortRanges(suite.T(), hl.HostSummary.AvailablePorts, 31000, 31000, 31002, 32000)
}

// TestKubeletHostSummaryCompleteLease tests CompleteLease function of host summary
func (suite *HostSummaryTestSuite) TestKubeletHostSummaryCompleteLease() {
	testTable := map[string]struct {
		errExpected     bool
		errMsg          string
		podToSpecMap    map[string]*pbpod.PodSpec
		podIDPreExisted bool
		// allocation before the test
		beforeAllocated scalar.Resources
		// Expected allocation after this test
		afterAllocated scalar.Resources
		leaseID        string
		inputLeaseID   string
		beforeStatus   HostStatus
		afterStatus    HostStatus
	}{

		"complete-lease-placing-host": {
			errExpected: false,
			// 5 pods each with 1 Cpu and 10 Mem
			podToSpecMap:    GeneratePodSpecWithRes(5, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			afterAllocated:  CreateResource(6.0, 60.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    PlacingHost,
			afterStatus:     ReadyHost,
		},
		"complete-lease-insufficient-resources": {
			errExpected: true,
			errMsg: fmt.Sprintf(
				"code:invalid-argument message:pod validation failed: host has insufficient resources"),
			// 10 pods each with 1 Cpu and 10 Mem
			podToSpecMap:    GeneratePodSpecWithRes(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			afterAllocated:  CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    PlacingHost,
			// even if it has insufficient resources, it will still unlock the
			// host status and make it Ready because the previous lease is
			// meanlingless at this point.
			afterStatus: ReadyHost,
		},
		// this is to track bugs where jobmgr launches a pod on the host
		// on which a pod with same podID is already running
		"complete-lease-but-pod-already-exists": {
			errExpected: true,
			errMsg: fmt.Sprintf(
				"code:invalid-argument message:pod %v already exists on the host",
				_podID),
			// 10 pods each with 1 Cpu and 10 Mem
			podToSpecMap:    GeneratePodSpecWithRes(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			afterAllocated:  CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			podIDPreExisted: true,
			inputLeaseID:    _leaseID,
			beforeStatus:    PlacingHost,
			// even if it has insufficient resources, it will still unlock the
			// host status and make it Ready because the previous lease is
			// meanlingless at this point.
			afterStatus: ReadyHost,
		},
		"complete-lease-ready-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host status is not Placing"),
			podToSpecMap:    GeneratePodSpecWithRes(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			afterAllocated:  CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    ReadyHost,
			afterStatus:     ReadyHost,
		},
		"complete-lease-id-mismatch": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host leaseID does not match"),
			podToSpecMap:    GeneratePodSpecWithRes(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			afterAllocated:  CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    uuid.New(),
			beforeStatus:    PlacingHost,
			afterStatus:     PlacingHost,
		},
	}

	for ttName, tt := range testTable {
		// create a host with 10 CPU and 100Mem
		s := NewKubeletHostSummary(_hostname, models.HostResources{}, _version).(*kubeletHostSummary)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.pods = newPodInfoMap()
		podInfo := &podInfo{
			state: pbpod.PodState_POD_STATE_LAUNCHED,
			spec: &pbpod.PodSpec{
				Containers: []*pbpod.ContainerSpec{
					{Resource: &pbpod.ResourceSpec{
						CpuLimit:   1.0,
						MemLimitMb: 10.0,
					}},
				},
			},
		}

		s.pods.AddPodInfo(_podID, podInfo)
		s.allocated.NonSlack = tt.beforeAllocated
		s.leaseID = tt.leaseID
		s.SetCapacity(models.HostResources{
			NonSlack: _capacity,
		})

		if tt.podIDPreExisted {
			tt.podToSpecMap[_podID] = nil
		}

		err := s.CompleteLease(tt.inputLeaseID, tt.podToSpecMap)
		if tt.errExpected {
			// complete with error, only the previous pods should exist in host summary
			suite.Equal(s.pods.GetSize(), 1, "test case: %s", ttName)
			suite.Error(err, "test case: %s", ttName)
			suite.Equal(tt.errMsg, err.Error(), "test case: %s", ttName)
		} else {
			// complete without error, all pods in podToSpecMap should be added
			suite.Equal(s.pods.GetSize(), len(tt.podToSpecMap)+1, "test case: %s", ttName)
			for id, spec := range tt.podToSpecMap {
				info, ok := s.pods.GetPodInfo(id)
				suite.True(ok)
				suite.Equal(info.spec, spec, "test case: %s", ttName)
				suite.Equal(
					info.state,
					pbpod.PodState_POD_STATE_LAUNCHED,
					"test case: %s", ttName,
				)
			}
			suite.NoError(err, "test case: %s", ttName)
		}
		suite.Equal(tt.afterAllocated, s.allocated.NonSlack, "test case: %s", ttName)
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case: %s", ttName)
	}
}

func equalPortRanges(t *testing.T, prs []*pbhost.PortRange, ports ...int) {
	require.Equal(t, len(ports)/2, len(prs), ports)
	for i, n := 0, len(ports); i < n; i += 2 {
		pr := prs[i/2]
		require.True(t, int(pr.Begin) == ports[i] && int(pr.End) == ports[i+1], pr)
	}
}

func TestPortAllocation(t *testing.T) {
	// list of begin, end pairs
	buildPortRanges := func(ports ...int) (all []*pbhost.PortRange) {
		for i, n := 0, len(ports); i < n; i += 2 {
			all = append(all, &pbhost.PortRange{Begin: uint64(ports[i]), End: uint64(ports[i+1])})
		}
		return all
	}

	t.Run("toPortRanges", func(t *testing.T) {
		all := toPortRanges([]int{10005, 10000, 10003, 10001, 10007, 10006})
		equalPortRanges(t, all, 10000, 10001, 10003, 10003, 10005, 10007)
	})

	t.Run("subtractPortRanges single range", func(t *testing.T) {
		avail := buildPortRanges(10010, 10020)

		for _, tc := range []struct {
			used []int
			left []int
		}{
			{[]int{10000, 10009}, []int{10010, 10020}},
			{[]int{10000, 10010}, []int{10011, 10020}},
			{[]int{10000, 10020}, []int{}},
			{[]int{10011, 10015}, []int{10010, 10010, 10016, 10020}},
			{[]int{10015, 10020}, []int{10010, 10014}},
			{[]int{10021, 10029}, []int{10010, 10020}},
		} {
			t.Logf("tc=%v", tc)
			used := buildPortRanges(tc.used...)
			equalPortRanges(t, subtractPortRanges(avail, used), tc.left...)
		}
	})

	t.Run("subtractPortRanges multiple ranges", func(t *testing.T) {
		for _, tc := range []struct {
			avail []int
			used  []int
			left  []int
		}{
			{
				[]int{10010, 10020, 10030, 10040},
				[]int{10010, 10015, 10016, 10035, 10036, 10040},
				[]int{},
			},
			{
				[]int{10010, 10020, 10030, 10040},
				[]int{10009, 10011, 10013, 10015, 10020, 10035},
				[]int{10012, 10012, 10016, 10019, 10036, 10040},
			},
		} {
			t.Logf("tc=%v", tc)
			avail := buildPortRanges(tc.avail...)
			used := buildPortRanges(tc.used...)
			equalPortRanges(t, subtractPortRanges(avail, used), tc.left...)
		}
	})

	t.Run("appendMerged", func(t *testing.T) {
		for _, tc := range []struct {
			merged []int
			pr     []int
			got    []int
		}{
			{
				[]int{},
				[]int{10015, 10020},
				[]int{10015, 10020},
			},
			{
				[]int{10010, 10020},
				[]int{10015, 10020},
				[]int{10010, 10020},
			},
			{
				[]int{10010, 10020},
				[]int{10021, 10030},
				[]int{10010, 10030},
			},
			{
				[]int{10010, 10020},
				[]int{10022, 10030},
				[]int{10010, 10020, 10022, 10030},
			},
		} {
			t.Logf("tc=%v", tc)
			merged := buildPortRanges(tc.merged...)
			pr := buildPortRanges(tc.pr...)[0]
			equalPortRanges(t, appendMerged(merged, pr), tc.got...)
		}
	})

	t.Run("mergePortRanges", func(t *testing.T) {
		for _, tc := range []struct {
			avail  []int
			unused []int
			merged []int
		}{
			{
				[]int{},
				[]int{10010, 10020, 10030, 10040},
				[]int{10010, 10020, 10030, 10040},
			},
			{
				[]int{10000, 10009, 10021, 10029},
				[]int{10010, 10020, 10030, 10040},
				[]int{10000, 10040},
			},
			{
				[]int{10000, 10010, 10040, 10050},
				[]int{10020, 10030},
				[]int{10000, 10010, 10020, 10030, 10040, 10050},
			},
			{
				[]int{10000, 10010, 10040, 10050},
				[]int{10001, 10015, 10045, 10060},
				[]int{10000, 10015, 10040, 10060},
			},
		} {
			t.Logf("tc=%v", tc)
			avail := buildPortRanges(tc.avail...)
			unused := buildPortRanges(tc.unused...)
			equalPortRanges(t, mergePortRanges(avail, unused), tc.merged...)
		}
	})
}
