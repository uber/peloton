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

package offers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	host "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	models_mocks "github.com/uber/peloton/pkg/placement/models/mocks"
	"github.com/uber/peloton/pkg/placement/offers"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v1"
)

func setupHostManager(t *testing.T) (
	*gomock.Controller,
	*host_mocks.MockHostManagerServiceYARPCClient,
	*resource_mocks.MockResourceManagerServiceYARPCClient,
	offers.Service,
	context.Context,
) {
	ctrl := gomock.NewController(t)
	mockHostManager := host_mocks.NewMockHostManagerServiceYARPCClient(ctrl)
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	metrics := metrics.NewMetrics(tally.NoopScope)
	service := NewService(mockHostManager, mockResourceManager, metrics)
	return ctrl, mockHostManager, mockResourceManager, service, context.Background()
}

// TestOfferServiceAcquire tests the Acquire call of offer.Service.
func TestOfferServiceAcquire(t *testing.T) {
	t.Run("acquire failures", func(t *testing.T) {
		ctrl, mockHostManager, _, service, ctx := setupHostManager(t)
		defer ctrl.Finish()

		needs := plugins.PlacementNeeds{}
		filter := plugins_v1.PlacementNeedsToHostFilter(needs)

		// Acquire Host Offers API call failed.
		mockHostManager.EXPECT().
			AcquireHosts(
				gomock.Any(),
				&hostsvc.AcquireHostsRequest{Filter: filter}).
			Return(nil, errors.New("acquire hosts failed"))
		hosts, reason := service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
		require.True(t, strings.Contains(reason, _failedToAcquireHosts))
		require.Len(t, hosts, 0)

		// Acquire Host Offers does not return any offer
		mockHostManager.EXPECT().
			AcquireHosts(
				gomock.Any(),
				&hostsvc.AcquireHostsRequest{Filter: filter}).
			Return(&hostsvc.AcquireHostsResponse{
				Hosts: nil,
			}, nil)
		hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
		require.Equal(t, reason, _noHostsAcquired)
		require.Len(t, hosts, 0)
	})

	t.Run("acquire success", func(t *testing.T) {
		ctrl, mockHostManager, mockResManager, service, ctx := setupHostManager(t)
		defer ctrl.Finish()

		needs := plugins.PlacementNeeds{}
		filter := plugins_v1.PlacementNeedsToHostFilter(needs)

		// Acquire Host Offers get tasks failure
		filterResult := map[string]uint32{
			"MISMATCH_CONSTRAINTS": 3,
			"MISMATCH_GPU":         5,
		}
		filterResultStr, _ := json.Marshal(filterResult)
		hostOffers := &hostsvc.AcquireHostsResponse{
			Hosts: []*hostmgr.HostLease{
				{
					HostSummary: &host.HostSummary{
						Hostname: "hostname",
					},
					LeaseId: &hostmgr.LeaseID{
						Value: "lease1",
					},
				},
			},
			FilterResultCounts: filterResult,
		}

		mockResManager.EXPECT().
			GetTasksByHosts(
				gomock.Any(),
				&resmgrsvc.GetTasksByHostsRequest{
					Type:      resmgr.TaskType_UNKNOWN,
					Hostnames: []string{"hostname"},
				},
			).
			Return(&resmgrsvc.GetTasksByHostsResponse{}, nil)

		// Acquire Host Offers successful call
		mockHostManager.EXPECT().
			AcquireHosts(
				gomock.Any(),
				&hostsvc.AcquireHostsRequest{Filter: filter}).
			Return(hostOffers, nil)

		hosts, reason := service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
		assert.Equal(t, string(filterResultStr), reason)
		require.Equal(t, 1, len(hosts))
		assert.Equal(t, "hostname", hosts[0].Hostname())
	})
}

// TestOfferServiceRelease tests the Release call of offer.Service.
func TestOfferServiceRelease(t *testing.T) {
	t.Run("release no hosts", func(t *testing.T) {
		ctrl, _, _, service, ctx := setupHostManager(t)
		defer ctrl.Finish()

		leases := []models.Offer{}
		service.Release(ctx, leases)
	})

	t.Run("terminate lease success", func(t *testing.T) {
		ctrl, mockHostManager, _, service, ctx := setupHostManager(t)
		defer ctrl.Finish()

		lease := models_mocks.NewMockOffer(ctrl)
		lease.EXPECT().ID().Return("lease1")
		lease.EXPECT().Hostname().Return("hostname1")

		leases := []models.Offer{lease}

		mockHostManager.EXPECT().
			TerminateLeases(
				gomock.Any(),
				&hostsvc.TerminateLeasesRequest{
					Leases: []*hostsvc.TerminateLeasesRequest_LeasePair{
						{
							Hostname: "hostname1",
							LeaseId: &hostmgr.LeaseID{
								Value: "lease1",
							},
						},
					},
				}).
			Return(nil, nil)

		service.Release(ctx, leases)
	})
}
