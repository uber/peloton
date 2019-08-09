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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
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

func mockHostSummary() *hostSummary {
	return newHostSummary(_hostname, _capacity, _version).(*hostSummary)
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
		heldPodIDs     map[string]time.Time
		filter         *hostmgr.HostFilter
		beforeStatus   HostStatus
		afterStatus    HostStatus
	}{
		"match-success-ready-host": {
			expectedResult: hostmgr.HostFilterResult_HOST_FILTER_MATCH,
			// available cpu 9.0 mem 90
			allocated:  createResource(1.0, 10.0),
			heldPodIDs: nil,
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
			allocated:  createResource(10.0, 10.0),
			heldPodIDs: nil,
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
			allocated:  createResource(1.0, 10.0),
			heldPodIDs: nil,
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
			heldPodIDs:   nil,
			filter:       &hostmgr.HostFilter{},
			beforeStatus: PlacingHost,
			afterStatus:  PlacingHost,
		},
		"match-fail-status-mismatch-held": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    createResource(1.0, 1.0),
			heldPodIDs:   map[string]time.Time{uuid.New(): time.Now()},
			filter:       &hostmgr.HostFilter{},
			beforeStatus: ReadyHost,
			afterStatus:  ReadyHost,
		},
		"match-fail-status-mismatch-reserved": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    createResource(1.0, 1.0),
			heldPodIDs:   nil,
			filter:       &hostmgr.HostFilter{},
			beforeStatus: ReservedHost,
			afterStatus:  ReservedHost,
		},
	}

	for ttName, tt := range testTable {
		s := mockHostSummary()
		s.status = tt.beforeStatus
		s.allocated = tt.allocated
		s.heldPodIDs = tt.heldPodIDs

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
		s := mockHostSummary()
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
		s := mockHostSummary()
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

func TestHoldForPod(t *testing.T) {
	id := &peloton.PodID{Value: uuid.New()}
	testCases := map[string]struct {
		heldPodIDs map[string]time.Time
		id         *peloton.PodID
		status     HostStatus
		errStr     string
	}{
		"added": {
			map[string]time.Time{}, id, ReadyHost, ""},
		"noop because previously added": {
			map[string]time.Time{id.GetValue(): time.Now()}, id, ReadyHost, ""},
		"failed because host is reserved": {
			map[string]time.Time{}, id, ReservedHost, "code:invalid-argument message:invalid status 3 for holding"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			s := mockHostSummary()
			s.status = tc.status
			s.heldPodIDs = tc.heldPodIDs

			err := s.HoldForPod(tc.id)
			if tc.errStr != "" {
				require.Error(err, tc.errStr)
				return
			}
			require.NoError(err)
			_, ok := s.heldPodIDs[tc.id.GetValue()]
			require.True(ok)
		})
	}
}

func TestReleaseHoldForPod(t *testing.T) {
	id := &peloton.PodID{Value: uuid.New()}
	testCases := map[string]struct {
		heldPodIDs  map[string]time.Time
		id          *peloton.PodID
		expectedLen int
	}{
		"deleted": {
			map[string]time.Time{id.GetValue(): time.Now()}, id, 0},
		"noop because not held": {
			map[string]time.Time{uuid.New(): time.Now()}, id, 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			s := mockHostSummary()
			s.heldPodIDs = tc.heldPodIDs
			s.ReleaseHoldForPod(tc.id)
			require.Equal(tc.expectedLen, len(s.heldPodIDs))
		})
	}
}

func TestGetHeldPods(t *testing.T) {
	require := require.New(t)
	s := mockHostSummary()
	ids := map[string]struct{}{}
	for i := 0; i < 10; i++ {
		id := &peloton.PodID{Value: uuid.New()}
		require.NoError(s.HoldForPod(id))
		ids[id.GetValue()] = struct{}{}
	}
	heldPods := s.GetHeldPods()
	for _, id := range heldPods {
		_, ok := ids[id.GetValue()]
		require.True(ok)
	}
}

func TestDeleteExpiredHolds(t *testing.T) {
	deadline := time.Now()
	t1 := deadline.Truncate(time.Minute)
	t2 := deadline.Add(time.Minute)
	p1 := &peloton.PodID{Value: uuid.New()}
	p2 := &peloton.PodID{Value: uuid.New()}
	testCases := map[string]struct {
		heldPodIDs map[string]time.Time
		isFree     bool
		expired    map[string]struct{}
	}{
		"free all": {
			map[string]time.Time{p1.GetValue(): t1, p2.GetValue(): t1},
			true,
			map[string]struct{}{
				p1.GetValue(): {},
				p2.GetValue(): {},
			}},
		"free some": {
			map[string]time.Time{p1.GetValue(): t1, p2.GetValue(): t2},
			false,
			map[string]struct{}{
				p1.GetValue(): {},
			}},
		"free none": {
			map[string]time.Time{p1.GetValue(): t2, p2.GetValue(): t2},
			false,
			nil},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			s := mockHostSummary()
			s.heldPodIDs = tc.heldPodIDs
			isFree, _, expired := s.DeleteExpiredHolds(deadline)
			require.Equal(tc.isFree, isFree)
			require.Equal(len(tc.expired), len(expired))
			for _, p := range expired {
				_, ok := tc.expired[p.GetValue()]
				require.True(ok)
			}
		})
	}
}
