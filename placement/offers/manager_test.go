package offers

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/placement/models"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestOfferManager_Dequeue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockHostManager := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	manager := NewManager(mockHostManager, mockResourceManager)

	ctx := context.Background()
	filter := &hostsvc.HostFilter{}
	hostOffers := &hostsvc.AcquireHostOffersResponse{
		HostOffers: []*hostsvc.HostOffer{
			{
				Hostname: "hostname",
			},
		},
		FilterResultCounts: map[string]uint32{},
	}
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
	offers, _, err := manager.Acquire(ctx, true, resmgr.TaskType_UNKNOWN, filter)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(offers))
	assert.Equal(t, "hostname", offers[0].Offer().Hostname)
	assert.Equal(t, 1, len(offers[0].Tasks()))
}

func TestOfferManager_Return(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockHostManager := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	manager := NewManager(mockHostManager, mockResourceManager)

	hostOffer := &hostsvc.HostOffer{
		Hostname: "hostname",
	}
	hostOffers := []*hostsvc.HostOffer{
		hostOffer,
	}
	offers := []*models.Offer{
		models.NewOffer(hostOffer, nil, time.Now()),
	}
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

	ctx := context.Background()
	err := manager.Release(ctx, offers)
	assert.NoError(t, err)
}
