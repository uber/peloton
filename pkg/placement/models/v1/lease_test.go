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

package models_v1

import (
	"testing"

	host "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/stretchr/testify/require"
)

// Tests that the lease object returns the correct info given the
// HostLease object.
func TestLease(t *testing.T) {
	t.Run("port count", func(t *testing.T) {
		hl := &hostmgr.HostLease{
			LeaseId: &hostmgr.LeaseID{Value: "l1"},
			HostSummary: &host.HostSummary{
				Hostname:  "h1",
				Resources: &peloton.Resources{},
			},
		}
		lease := NewOffer(hl, nil)
		_, ports := lease.GetAvailableResources()
		require.Equal(t, uint64(0), ports)
	})

	t.Run("port count non-zero", func(t *testing.T) {
		hl := &hostmgr.HostLease{
			LeaseId: &hostmgr.LeaseID{Value: "l1"},
			HostSummary: &host.HostSummary{
				Hostname:  "h1",
				Resources: &peloton.Resources{},
				AvailablePorts: []*host.PortRange{
					{Begin: 1, End: 2},
					{Begin: 4, End: 4},
				},
			},
		}
		lease := NewOffer(hl, nil)
		_, ports := lease.GetAvailableResources()
		require.Equal(t, uint64(3), ports)
	})

	t.Run("non-nil tasks", func(t *testing.T) {
		hl := &hostmgr.HostLease{
			LeaseId: &hostmgr.LeaseID{Value: "l1"},
			HostSummary: &host.HostSummary{
				Hostname:  "h1",
				Resources: &peloton.Resources{},
				AvailablePorts: []*host.PortRange{
					{Begin: 1, End: 2},
					{Begin: 4, End: 4},
				},
			},
		}
		lease := NewOffer(hl, []*resmgr.Task{
			{Name: "t1"},
		})
		_, ports := lease.GetAvailableResources()
		require.Equal(t, uint64(3), ports)
	})
}
