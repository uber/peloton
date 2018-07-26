package offers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"

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
	filter := &hostsvc.HostFilter{}

	// Acquire Host Offers API call failed.
	mockHostManager.EXPECT().
		AcquireHostOffers(
			gomock.Any(),
			&hostsvc.AcquireHostOffersRequest{Filter: filter}).
		Return(nil, errors.New("acquire host offers failed"))
	hosts, reason := service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)
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
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)
	assert.Equal(t, reason, _failedToAcquireHostOffers)

	// Acquire Host Offers does not return any offer
	mockHostManager.EXPECT().
		AcquireHostOffers(
			gomock.Any(),
			&hostsvc.AcquireHostOffersRequest{Filter: filter}).
		Return(&hostsvc.AcquireHostOffersResponse{
			HostOffers: nil,
		}, nil)
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)
	assert.Equal(t, reason, _failedToAcquireHostOffers)

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
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)

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
	hosts, reason = service.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)
	assert.Equal(t, string(filterResultStr), reason)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "hostname", hosts[0].GetOffer().Hostname)
	assert.Equal(t, 1, len(hosts[0].GetTasks()))
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
		Hostname: "hostname",
	}
	hostOffers := []*hostsvc.HostOffer{
		hostOffer,
	}
	offers := []*models.HostOffers{
		models.NewHostOffers(hostOffer, nil, time.Now()),
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
