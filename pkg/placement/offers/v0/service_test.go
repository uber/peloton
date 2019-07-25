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
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v0"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestOfferService_Dequeue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockHostManager := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	metrics := metrics.NewMetrics(tally.NoopScope)
	service := NewService(mockHostManager, mockResourceManager, metrics)

	ctx := context.Background()
	needs := plugins.PlacementNeeds{}
	filter := plugins_v0.PlacementNeedsToHostFilter(needs)

	// Acquire Host Offers API call failed.
	mockHostManager.EXPECT().
		AcquireHostOffers(
			gomock.Any(),
			&hostsvc.AcquireHostOffersRequest{Filter: filter}).
		Return(nil, errors.New("acquire host offers failed"))
	hosts, reason := service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
	assert.Equal(t, reason, _failedToAcquireHostOffers)

	// Acquire Host Offers API response has error
	mockHostManager.EXPECT().
		AcquireHostOffers(
			gomock.Any(),
			&hostsvc.AcquireHostOffersRequest{Filter: filter}).
		Return(&hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				Failure: &hostsvc.AcquireHostOffersFailure{
					Message: "acquire host offers response err",
				},
			}}, nil)
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
	assert.Equal(t, reason, _failedToAcquireHostOffers)

	// Acquire Host Offers does not return any offer
	mockHostManager.EXPECT().
		AcquireHostOffers(
			gomock.Any(),
			&hostsvc.AcquireHostOffersRequest{Filter: filter}).
		Return(&hostsvc.AcquireHostOffersResponse{
			HostOffers: nil,
		}, nil)
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
	assert.Equal(t, reason, _noHostOffers)

	// Acquire Host Offers get tasks failure
	filterResult := map[string]uint32{
		"MISMATCH_CONSTRAINTS": 3,
		"MISMATCH_GPU":         5,
	}
	filterResultStr, _ := json.Marshal(filterResult)
	hostOffers := &hostsvc.AcquireHostOffersResponse{
		HostOffers: []*hostsvc.HostOffer{
			{
				Hostname: "hostname",
			},
		},
		FilterResultCounts: filterResult,
	}

	tasksRequest := &resmgrsvc.GetTasksByHostsRequest{
		Type:      resmgr.TaskType_UNKNOWN,
		Hostnames: []string{"hostname"},
	}

	gomock.InOrder(
		mockHostManager.EXPECT().
			AcquireHostOffers(
				gomock.Any(),
				&hostsvc.AcquireHostOffersRequest{
					Filter: filter,
				},
			).Return(hostOffers, nil),
		mockResourceManager.EXPECT().GetTasksByHosts(gomock.Any(), tasksRequest).
			Return(nil, errors.New("get tasks by host failed")),
	)
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)

	// Acquire Host Offers successful call
	gomock.InOrder(
		mockHostManager.EXPECT().
			AcquireHostOffers(
				gomock.Any(),
				&hostsvc.AcquireHostOffersRequest{
					Filter: filter,
				},
			).Return(hostOffers, nil),
		mockResourceManager.EXPECT().
			GetTasksByHosts(gomock.Any(),
				&resmgrsvc.GetTasksByHostsRequest{
					Type:      resmgr.TaskType_UNKNOWN,
					Hostnames: []string{"hostname"},
				},
			).Return(
			&resmgrsvc.GetTasksByHostsResponse{
				HostTasksMap: map[string]*resmgrsvc.TaskList{
					"hostname": {
						Tasks: []*resmgr.Task{
							{
								Name:     "task",
								Hostname: "hostname",
							},
						},
					},
				},
				Error: nil,
			}, nil),
	)
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, needs)
	assert.Equal(t, string(filterResultStr), reason)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "hostname", hosts[0].Hostname())
}

func TestOfferService_Return(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockHostManager := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	metrics := metrics.NewMetrics(tally.NoopScope)
	service := NewService(mockHostManager, mockResourceManager, metrics)
	ctx := context.Background()
	hostOffer := &hostsvc.HostOffer{
		Id:       &peloton.HostOfferID{Value: "pelotonid"},
		Hostname: "hostname",
		AgentId:  &mesos.AgentID{Value: &[]string{"agentid"}[0]},
	}
	hostOffers := []*hostsvc.HostOffer{
		hostOffer,
	}
	offers := []models.Offer{
		models_v0.NewHostOffers(hostOffer, nil, time.Now()),
	}

	// Test empty host offers
	service.Release(ctx, nil)

	// Release Host Offers error
	mockHostManager.EXPECT().
		ReleaseHostOffers(
			gomock.Any(),
			&hostsvc.ReleaseHostOffersRequest{
				HostOffers: hostOffers,
			},
		).
		Return(
			&hostsvc.ReleaseHostOffersResponse{},
			errors.New("release host offers api error"))
	service.Release(ctx, offers)

	// Release Host Offers API error
	mockHostManager.EXPECT().
		ReleaseHostOffers(
			gomock.Any(),
			&hostsvc.ReleaseHostOffersRequest{
				HostOffers: hostOffers,
			},
		).
		Return(
			&hostsvc.ReleaseHostOffersResponse{
				Error: &hostsvc.ReleaseHostOffersResponse_Error{},
			},
			nil,
		)
	service.Release(ctx, offers)

	// Release Host Offers successful call
	gomock.InOrder(
		mockHostManager.EXPECT().
			ReleaseHostOffers(
				gomock.Any(),
				&hostsvc.ReleaseHostOffersRequest{
					HostOffers: hostOffers,
				},
			).
			Return(
				&hostsvc.ReleaseHostOffersResponse{},
				nil,
			),
	)

	service.Release(ctx, offers)
}
