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

package hostcache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"go.uber.org/atomic"

	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

var (
	// capacity of host for testing
	_capacity = &peloton.Resources{
		Cpu:   10.0,
		MemMb: 100.0,
	}
	// host name string
	_hostname = "host"
	// host resource version
	_version = "1234"
	// lease ID
	_leaseID = uuid.New()
	// pod ID
	_podID = uuid.New()
)

// HostCacheTestSuite is test suite for p2k host cache package
type HostCacheTestSuite struct {
	suite.Suite
}

// SetupTest is setup function for this suite
func (suite *HostCacheTestSuite) SetupTest() {
	// no mocks to setup yet
}

// TearDownTest is teardown function for this suite
func (suite *HostCacheTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestHostCacheTestSuite(t *testing.T) {
	suite.Run(t, new(HostCacheTestSuite))
}

func createResource(cpu, mem float64) (r scalar.Resources) {
	r.CPU = cpu
	r.Mem = mem
	return r
}

// generatePodToResMap generates a map of podIDs to resources where each pod
// gets the specified `cpu` and `mem`
func generatePodToResMap(
	numPods int,
	cpu, mem float64,
) map[string]scalar.Resources {
	podMap := make(map[string]scalar.Resources)
	for i := 0; i < numPods; i++ {
		podMap[uuid.New()] = createResource(cpu, mem)
	}
	return podMap
}

// TestHandlePodEvent makes sure that we release the pod resources when we
// get a pod deleted event.
// In this test we acquire the host that has room for 1 pod, handle a delete
// event, and make sure we can re-acquire that host for another pod.
// This test is run with 100 goroutines trying to acquire the host, and once
// one of the thread succeeds, we send a DeletePod event to notify that the
// pod ran to completion and was deleted.
// Then we expect another thread to acquire that host successfully.
// We should count 2 total successful matches.
func (suite *HostCacheTestSuite) TestHandlePodEvent() {
	s := newHostSummary(_hostname, _capacity, _version).(*hostSummary)
	s.status = ReadyHost
	s.allocated = createResource(0, 0)

	filter := &hostmgr.HostFilter{
		ResourceConstraint: &hostmgr.ResourceConstraint{
			Minimum: &pod.ResourceSpec{
				CpuLimit:   9.0,
				MemLimitMb: 90.0,
			},
		},
	}

	resMap := map[string]scalar.Resources{
		"podid1": createResource(9, 90),
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
				err := s.CompleteLease(s.leaseID, resMap)
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
				err = s.HandlePodEvent(evt)
				suite.NoError(err)
			}
		}()
	}

	wg.Wait()
	suite.Equal(int32(2), matches.Load())
	suite.Equal(int32(998), failures.Load())
}

// TestTryMatchReadyHost tests various combinations of trying to match host
// with the input host filter
func (suite *HostCacheTestSuite) TestTryMatchReadyHost() {
	testTable := map[string]struct {
		expectedResult hostmgr.HostFilterResult
		allocated      scalar.Resources
		filter         *hostmgr.HostFilter
		beforeStatus   HostStatus
		afterStatus    HostStatus
	}{
		"match-success-ready-host": {
			expectedResult: hostmgr.HostFilterResult_HOST_FILTER_MATCH,
			// available cpu 9.0 mem 90
			allocated: createResource(1.0, 10.0),
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   2.0,
						MemLimitMb: 2.0,
					},
				},
			},
			beforeStatus: ReadyHost,
			afterStatus:  PlacingHost,
		},
		"match-fail-insufficient-resources-host-full": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_INSUFFICIENT_RESOURCES,
			// available cpu 0 mem 90, host is fully allocated for CPUs
			allocated: createResource(10.0, 10.0),
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   2.0,
						MemLimitMb: 2.0,
					},
				},
			},
			beforeStatus: ReadyHost,
			afterStatus:  ReadyHost,
		},
		"match-fail-insufficient-resources": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_INSUFFICIENT_RESOURCES,
			// available cpu 9 mem 90
			allocated: createResource(1.0, 10.0),
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						// demand is more than available resources
						CpuLimit:   20.0,
						MemLimitMb: 2.0,
					},
				},
			},
			beforeStatus: ReadyHost,
			afterStatus:  ReadyHost,
		},
		"match-fail-status-mismatch-placing": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    createResource(1.0, 1.0),
			filter:       &hostmgr.HostFilter{},
			beforeStatus: PlacingHost,
			afterStatus:  PlacingHost,
		},
		"match-fail-status-mismatch-held": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    createResource(1.0, 1.0),
			filter:       &hostmgr.HostFilter{},
			beforeStatus: HeldHost,
			afterStatus:  HeldHost,
		},
		"match-fail-status-mismatch-reserved": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    createResource(1.0, 1.0),
			filter:       &hostmgr.HostFilter{},
			beforeStatus: ReservedHost,
			afterStatus:  ReservedHost,
		},
	}

	for ttName, tt := range testTable {
		s := newHostSummary(_hostname, _capacity, _version).(*hostSummary)
		s.status = tt.beforeStatus
		s.allocated = tt.allocated

		match := s.TryMatch(tt.filter)

		suite.Equal(tt.expectedResult, match.Result, "test case is %s", ttName)
		if tt.expectedResult != hostmgr.HostFilterResult_HOST_FILTER_MATCH {
			// make sure lease ID is empty
			suite.Equal(
				emptyLeaseID,
				s.leaseID,
				"test case is %s", ttName,
			)
		}
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case %s", ttName)
	}
}

// TestHostSummaryTerminateLease tests TerminateLease function of host summary
func (suite *HostCacheTestSuite) TestHostSummaryTerminateLease() {
	testTable := map[string]struct {
		errExpected      bool
		errMsg           string
		podToResMap      map[string]scalar.Resources
		preExistingPodID string
		// allocation before the test
		beforeAllocated scalar.Resources
		leaseID         string
		inputLeaseID    string
		beforeStatus    HostStatus
		afterStatus     HostStatus
	}{

		"terminate-lease-placing-host": {
			errExpected: false,
			// 5 pods each with 1 Cpu and 10 Mem
			podToResMap:     generatePodToResMap(5, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    PlacingHost,
			afterStatus:     ReadyHost,
		},
		"terminate-lease-held-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:invalid status 4"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    HeldHost,
			afterStatus:     HeldHost,
		},
		"terminate-lease-ready-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:invalid status 1"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    ReadyHost,
			afterStatus:     ReadyHost,
		},
		"terminate-lease-id-mismatch": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host leaseID does not match"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    uuid.New(),
			beforeStatus:    PlacingHost,
			afterStatus:     PlacingHost,
		},
	}

	for ttName, tt := range testTable {
		// create a host with 10 CPU and 100Mem
		s := newHostSummary(_hostname, _capacity, _version).(*hostSummary)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.podToResMap = generatePodToResMap(1, 1.0, 10.0)
		s.allocated = tt.beforeAllocated
		s.leaseID = tt.leaseID

		if tt.preExistingPodID != "" {
			s.podToResMap[tt.preExistingPodID] = scalar.Resources{}
			tt.podToResMap[tt.preExistingPodID] = scalar.Resources{}
		}

		err := s.TerminateLease(tt.inputLeaseID)
		if tt.errExpected {
			suite.Error(err)
			suite.Equal(tt.errMsg, err.Error(), "test case: %s", ttName)
		} else {
			suite.NoError(err, "test case: %s", ttName)
		}
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case: %s", ttName)
	}
}

// TestHostSummaryCompleteLease tests CompleteLease function of host summary
func (suite *HostCacheTestSuite) TestHostSummaryCompleteLease() {
	testTable := map[string]struct {
		errExpected      bool
		errMsg           string
		podToResMap      map[string]scalar.Resources
		preExistingPodID string
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
			podToResMap:     generatePodToResMap(5, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			afterAllocated:  createResource(6.0, 60.0),
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
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			afterAllocated:  createResource(1.0, 10.0),
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
				"code:invalid-argument message:pod validation failed: pod %v already exists on the host",
				_podID),
			// 10 pods each with 1 Cpu and 10 Mem
			podToResMap:      generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated:  createResource(1.0, 10.0),
			afterAllocated:   createResource(1.0, 10.0),
			leaseID:          _leaseID,
			preExistingPodID: _podID,
			inputLeaseID:     _leaseID,
			beforeStatus:     PlacingHost,
			// even if it has insufficient resources, it will still unlock the
			// host status and make it Ready because the previous lease is
			// meanlingless at this point.
			afterStatus: ReadyHost,
		},
		"complete-lease-held-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host status is not Placing"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			afterAllocated:  createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    HeldHost,
			afterStatus:     HeldHost,
		},
		"complete-lease-ready-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host status is not Placing"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			afterAllocated:  createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    ReadyHost,
			afterStatus:     ReadyHost,
		},
		"complete-lease-id-mismatch": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host leaseID does not match"),
			podToResMap:     generatePodToResMap(10, 1.0, 10.0),
			beforeAllocated: createResource(1.0, 10.0),
			afterAllocated:  createResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    uuid.New(),
			beforeStatus:    PlacingHost,
			afterStatus:     PlacingHost,
		},
	}

	for ttName, tt := range testTable {
		// create a host with 10 CPU and 100Mem
		s := newHostSummary(_hostname, _capacity, _version).(*hostSummary)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.podToResMap = generatePodToResMap(1, 1.0, 10.0)
		s.allocated = tt.beforeAllocated
		s.leaseID = tt.leaseID

		if tt.preExistingPodID != "" {
			s.podToResMap[tt.preExistingPodID] = scalar.Resources{}
			tt.podToResMap[tt.preExistingPodID] = scalar.Resources{}
		}

		err := s.CompleteLease(tt.inputLeaseID, tt.podToResMap)
		if tt.errExpected {
			suite.Error(err)
			suite.Equal(tt.errMsg, err.Error(), "test case: %s", ttName)
		} else {
			suite.NoError(err, "test case: %s", ttName)
		}
		suite.Equal(tt.afterAllocated, s.allocated, "test case: %s", ttName)
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case: %s", ttName)
	}
}
