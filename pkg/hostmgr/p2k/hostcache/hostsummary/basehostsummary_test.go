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
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// HostSummaryTestSuite is test suite for p2k host summary package.
type HostSummaryTestSuite struct {
	suite.Suite
}

// SetupTest is setup function for this suite
func (suite *HostSummaryTestSuite) SetupTest() {
	// no mocks to setup yet
}

// TearDownTest is teardown function for this suite
func (suite *HostSummaryTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestHostSummaryTestSuite(t *testing.T) {
	suite.Run(t, new(HostSummaryTestSuite))
}

// TestTryMatchReadyHost tests various combinations of trying to match host
// with the input host filter
func (suite *HostSummaryTestSuite) TestTryMatchReadyHost() {
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
			allocated:  CreateResource(1.0, 10.0),
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
			allocated:  CreateResource(10.0, 10.0),
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
			allocated:  CreateResource(1.0, 10.0),
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
			allocated:    CreateResource(1.0, 1.0),
			heldPodIDs:   nil,
			filter:       &hostmgr.HostFilter{},
			beforeStatus: PlacingHost,
			afterStatus:  PlacingHost,
		},
		"match-fail-status-mismatch-held": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    CreateResource(1.0, 1.0),
			heldPodIDs:   map[string]time.Time{uuid.New(): time.Now()},
			filter:       &hostmgr.HostFilter{},
			beforeStatus: ReadyHost,
			afterStatus:  ReadyHost,
		},
		"match-fail-status-mismatch-reserved": {
			expectedResult: hostmgr.
				HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			allocated:    CreateResource(1.0, 1.0),
			heldPodIDs:   nil,
			filter:       &hostmgr.HostFilter{},
			beforeStatus: ReservedHost,
			afterStatus:  ReservedHost,
		},
	}

	for ttName, tt := range testTable {
		s := NewFakeHostSummary(_hostname, _version, _capacity)
		s.status = tt.beforeStatus
		s.allocated.NonSlack = tt.allocated
		s.capacity.NonSlack = _capacity
		s.available.NonSlack = _capacity.Subtract(tt.allocated)
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
func (suite *HostSummaryTestSuite) TestHostSummaryTerminateLease() {
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
			podToResMap:     GeneratePodToResMap(5, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    PlacingHost,
			afterStatus:     ReadyHost,
		},
		"terminate-lease-ready-host": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:invalid status 1"),
			podToResMap:     GeneratePodToResMap(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    _leaseID,
			beforeStatus:    ReadyHost,
			afterStatus:     ReadyHost,
		},
		"terminate-lease-id-mismatch": {
			errExpected:     true,
			errMsg:          fmt.Sprintf("code:invalid-argument message:host leaseID does not match"),
			podToResMap:     GeneratePodToResMap(10, 1.0, 10.0),
			beforeAllocated: CreateResource(1.0, 10.0),
			leaseID:         _leaseID,
			inputLeaseID:    uuid.New(),
			beforeStatus:    PlacingHost,
			afterStatus:     PlacingHost,
		},
	}

	for ttName, tt := range testTable {
		// create a host with 10 CPU and 100Mem
		s := NewFakeHostSummary(_hostname, _version, _capacity)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.allocated.NonSlack = tt.beforeAllocated
		s.available = s.capacity.Subtract(s.allocated)
		s.leaseID = tt.leaseID

		if tt.preExistingPodID != "" {
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
func (suite *HostSummaryTestSuite) TestHostSummaryCompleteLease() {
	testTable := map[string]struct {
		errExpected      bool
		errMsg           string
		preExistingPodID string
		leaseID          string
		inputLeaseID     string
		beforeStatus     HostStatus
		afterStatus      HostStatus
	}{
		"complete-lease-placing-host": {
			errExpected:  false,
			leaseID:      _leaseID,
			inputLeaseID: _leaseID,
			beforeStatus: PlacingHost,
			afterStatus:  ReadyHost,
		},
		"complete-lease-ready-host": {
			errExpected:  true,
			errMsg:       fmt.Sprintf("code:invalid-argument message:host status is not Placing"),
			leaseID:      _leaseID,
			inputLeaseID: _leaseID,
			beforeStatus: ReadyHost,
			afterStatus:  ReadyHost,
		},
		"complete-lease-id-mismatch": {
			errExpected:  true,
			errMsg:       fmt.Sprintf("code:invalid-argument message:host leaseID does not match"),
			leaseID:      _leaseID,
			inputLeaseID: uuid.New(),
			beforeStatus: PlacingHost,
			afterStatus:  PlacingHost,
		},
	}

	for ttName, tt := range testTable {
		s := NewFakeHostSummary(_hostname, _version, _capacity)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.leaseID = tt.leaseID
		s.capacity.NonSlack = _capacity

		err := s.CompleteLease(tt.inputLeaseID, nil)
		if tt.errExpected {
			suite.Error(err)
			suite.Equal(tt.errMsg, err.Error(), "test case: %s", ttName)
		} else {
			suite.NoError(err, "test case: %s", ttName)
		}
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case: %s", ttName)
	}
}

// TestHostSummaryHandlePodEvent tests HandlePodEvent
func (suite *HostSummaryTestSuite) TestHostSummaryHandlePodEvent() {
	testTable := map[string]struct {
		eventType  p2kscalar.PodEventType
		eventState string
		podState   pbpod.PodState
		deleted    bool
	}{
		"deleted-pod-event": {
			eventType: p2kscalar.DeletePod,
			deleted:   true,
		},
		"update-pod-non-terminal-event": {
			eventType:  p2kscalar.UpdatePod,
			eventState: pbpod.PodState_POD_STATE_RUNNING.String(),
			podState:   pbpod.PodState_POD_STATE_RUNNING,
		},
		"add-pod-non-terminal-event": {
			eventType:  p2kscalar.AddPod,
			eventState: pbpod.PodState_POD_STATE_KILLING.String(),
			podState:   pbpod.PodState_POD_STATE_KILLING,
		},
		"update-pod-terminal-event": {
			eventType:  p2kscalar.UpdatePod,
			eventState: pbpod.PodState_POD_STATE_KILLED.String(),
			deleted:    true,
		},
		"add-pod-terminal-event": {
			eventType:  p2kscalar.AddPod,
			eventState: pbpod.PodState_POD_STATE_FAILED.String(),
			deleted:    true,
		},
	}

	for ttName, tt := range testTable {
		podID := uuid.New()
		s := NewFakeHostSummary(_hostname, _version, _capacity)
		s.pods = newPodInfoMap()
		s.pods.AddPodSpec(podID, &pbpod.PodSpec{})

		s.HandlePodEvent(&p2kscalar.PodEvent{
			EventType: tt.eventType,
			Event: &pbpod.PodEvent{
				PodId:       &peloton.PodID{Value: podID},
				ActualState: tt.eventState,
			},
		})

		info, ok := s.pods.GetPodInfo(podID)
		if tt.deleted {
			suite.False(ok, "test case: %s", ttName)
		} else {
			suite.Equal(info.state, tt.podState, "test case: %s", ttName)
		}
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
			s := NewFakeHostSummary(_hostname, _version, _capacity)
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
			s := NewFakeHostSummary(_hostname, _version, _capacity)
			s.heldPodIDs = tc.heldPodIDs
			s.ReleaseHoldForPod(tc.id)
			require.Equal(tc.expectedLen, len(s.heldPodIDs))
		})
	}
}

func TestGetHeldPods(t *testing.T) {
	require := require.New(t)
	s := NewFakeHostSummary(_hostname, _version, _capacity)
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
			s := NewFakeHostSummary(_hostname, _version, _capacity)
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
