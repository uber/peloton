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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	"go.uber.org/atomic"
)

// TestKubeletHostSummarySetCapacity
// test set capacity would update available resources
func (suite *HostCacheTestSuite) TestKubeletHostSummarySetCapacity() {
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
		ks := newKubeletHostSummary(_hostname, nil, _version)
		ks.(*kubeletHostSummary).allocated = test.allocated
		ks.SetCapacity(test.capacity)
		suite.Equal(ks.GetCapacity(), test.capacity)
		suite.Equal(ks.GetAllocated(), test.allocated)
		suite.Equal(ks.GetAvailable(), test.expectedAvailable)
	}
}

// TestKubeletHostSummarySetAvailable tests set
// available is noop for k8s host
func (suite *HostCacheTestSuite) TestKubeletHostSummarySetAvailable() {
	ks := newKubeletHostSummary(_hostname, nil, _version)

	oldAvailable := scalar.Resources{CPU: 2.0}
	newAvailable := scalar.Resources{CPU: 4.0}

	ks.(*kubeletHostSummary).available = oldAvailable
	ks.SetAvailable(newAvailable)

	suite.Equal(ks.GetAvailable(), oldAvailable)
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
func (suite *HostCacheTestSuite) TestKubeletHostSummaryHandlePodEvent() {
	s := newKubeletHostSummary(_hostname, nil, _version).(*kubeletHostSummary)
	s.status = ReadyHost
	s.allocated = createResource(0, 0)
	s.SetCapacity(_capacity)

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
				s.HandlePodEvent(evt)
			}
		}()
	}

	wg.Wait()
	suite.Equal(int32(2), matches.Load())
	suite.Equal(int32(998), failures.Load())
}

// TestKubeletHostSummaryCompleteLease tests CompleteLease function of host summary
func (suite *HostCacheTestSuite) TestKubeletHostSummaryCompleteLease() {
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
		s := newKubeletHostSummary(_hostname, nil, _version).(*kubeletHostSummary)
		s.status = tt.beforeStatus
		// initialize host cache with a podMap
		s.podToResMap = generatePodToResMap(1, 1.0, 10.0)
		s.allocated = tt.beforeAllocated
		s.leaseID = tt.leaseID
		s.SetCapacity(_capacity)

		if tt.preExistingPodID != "" {
			s.podToResMap[tt.preExistingPodID] = scalar.Resources{}
			tt.podToResMap[tt.preExistingPodID] = scalar.Resources{}
		}

		err := s.CompleteLease(tt.inputLeaseID, tt.podToResMap)
		if tt.errExpected {
			suite.Error(err, "test case: %s", ttName)
			suite.Equal(tt.errMsg, err.Error(), "test case: %s", ttName)
		} else {
			suite.NoError(err, "test case: %s", ttName)
		}
		suite.Equal(tt.afterAllocated, s.allocated, "test case: %s", ttName)
		suite.Equal(tt.afterStatus, s.GetHostStatus(), "test case: %s", ttName)
	}
}
