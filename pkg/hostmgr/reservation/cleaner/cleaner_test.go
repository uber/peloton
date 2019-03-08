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

package cleaner

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/pkg/common/util"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	offerpool_mocks "github.com/uber/peloton/pkg/hostmgr/offer/offerpool/mocks"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
)

const (
	_perHostCPU  = 10.0
	_perHostMem  = 20.0
	_perHostDisk = 30.0
	pelotonRole  = "peloton"
)

var (
	_testAgent    = "agent"
	_testOfferID  = "testOffer"
	_testVolumeID = "testVolume"
	_testKey      = "testKey"
	_testValue    = "testValue"
)

type mockMesosStreamIDProvider struct {
}

func (msp *mockMesosStreamIDProvider) GetMesosStreamID(ctx context.Context) string {
	return "stream"
}

func (msp *mockMesosStreamIDProvider) GetFrameworkID(ctx context.Context) *mesos.FrameworkID {
	return nil
}

func TestCleanUnusedResources(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSchedulerClient := mpb_mocks.NewMockSchedulerClient(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)
	mockOfferPool := offerpool_mocks.NewMockPool(ctrl)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	cleaner := NewCleaner(
		mockOfferPool,
		testScope,
		mockVolumeStore,
		mockSchedulerClient,
		&mockMesosStreamIDProvider{})

	reservation := &mesos.Resource_ReservationInfo{
		Labels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(_perHostCPU).
			WithRole(pelotonRole).
			WithReservation(reservation).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(_perHostMem).
			WithReservation(reservation).
			WithRole(pelotonRole).
			Build(),
	}
	offer := createMesosOffer(resources)
	reservedOffers := make(map[string]*mesos.Offer)
	reservedOffers[offer.GetId().GetValue()] = offer
	hostOffers := make(map[string]map[string]*mesos.Offer)
	hostOffers[offer.GetHostname()] = reservedOffers

	gomock.InOrder(
		mockOfferPool.EXPECT().GetOffers(summary.Reserved).Return(hostOffers, 4),
		mockOfferPool.EXPECT().RemoveReservedOffer(offer.GetHostname(), offer.GetId().GetValue()),
		mockSchedulerClient.EXPECT().
			Call(
				gomock.Eq("stream"),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify implicit reconcile call.
				call := msg.(*sched.Call)
				assert.Equal(t, sched.Call_ACCEPT, call.GetType())
				assert.Equal(t, "", call.GetFrameworkId().GetValue())
				assert.Equal(t, []*mesos.OfferID{offer.GetId()}, call.GetAccept().GetOfferIds())
				assert.Equal(t, 1, len(call.GetAccept().GetOperations()))
				assert.Equal(
					t,
					mesos.Offer_Operation_UNRESERVE,
					call.GetAccept().GetOperations()[0].GetType())
				for _, res := range call.GetAccept().GetOperations()[0].GetUnreserve().GetResources() {
					assert.Equal(t, reservation, res.GetReservation())
					assert.NotEqual(t, res.GetName(), "disk")
				}
			}).
			Return(nil),
	)

	cleaner.Run(nil)
}

func TestCleanVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSchedulerClient := mpb_mocks.NewMockSchedulerClient(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)
	mockOfferPool := offerpool_mocks.NewMockPool(ctrl)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	cleaner := NewCleaner(
		mockOfferPool,
		testScope,
		mockVolumeStore,
		mockSchedulerClient,
		&mockMesosStreamIDProvider{})

	reservation := &mesos.Resource_ReservationInfo{
		Labels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &_testVolumeID,
		},
	}
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(_perHostCPU).
			WithRole(pelotonRole).
			WithReservation(reservation).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(_perHostMem).
			WithReservation(reservation).
			WithRole(pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(_perHostDisk).
			WithRole(pelotonRole).
			WithReservation(reservation).
			WithDisk(diskInfo).
			Build(),
	}
	offer := createMesosOffer(resources)
	reservedOffers := make(map[string]*mesos.Offer)
	reservedOffers[offer.GetId().GetValue()] = offer
	hostOffers := make(map[string]map[string]*mesos.Offer)
	hostOffers[offer.GetHostname()] = reservedOffers
	volumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	volumeInfo := &volume.PersistentVolumeInfo{
		State:     volume.VolumeState_CREATED,
		GoalState: volume.VolumeState_DELETED,
	}

	gomock.InOrder(
		mockOfferPool.EXPECT().GetOffers(summary.Reserved).Return(hostOffers, 4),
		mockVolumeStore.EXPECT().GetPersistentVolume(gomock.Any(), volumeID).Return(volumeInfo, nil),
		mockVolumeStore.EXPECT().UpdatePersistentVolume(gomock.Any(), volumeInfo).Return(nil),
		mockOfferPool.EXPECT().RemoveReservedOffer(offer.GetHostname(), offer.GetId().GetValue()),
		mockSchedulerClient.EXPECT().
			Call(
				gomock.Eq("stream"),
				gomock.Any()).
			Do(func(_ string, msg proto.Message) {
				// Verify implicit reconcile call.
				call := msg.(*sched.Call)
				assert.Equal(t, sched.Call_ACCEPT, call.GetType())
				assert.Equal(t, "", call.GetFrameworkId().GetValue())
				assert.Equal(t, []*mesos.OfferID{offer.GetId()}, call.GetAccept().GetOfferIds())
				assert.Equal(t, 2, len(call.GetAccept().GetOperations()))
				assert.Equal(
					t,
					mesos.Offer_Operation_DESTROY,
					call.GetAccept().GetOperations()[0].GetType())
				destroyResource := call.GetAccept().GetOperations()[0].GetDestroy().GetVolumes()[0]
				assert.Equal(t, "disk", destroyResource.GetName())
				assert.Equal(t, reservation, destroyResource.GetReservation())
				assert.Equal(
					t,
					mesos.Offer_Operation_UNRESERVE,
					call.GetAccept().GetOperations()[1].GetType())
				for _, res := range call.GetAccept().GetOperations()[1].GetUnreserve().GetResources() {
					assert.Equal(t, reservation, res.GetReservation())
				}
			}).
			Return(nil),
	)

	cleaner.Run(nil)
}

func TestNotCleanVolumeIfVolumeGoalstateNotDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSchedulerClient := mpb_mocks.NewMockSchedulerClient(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)
	mockOfferPool := offerpool_mocks.NewMockPool(ctrl)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	cleaner := NewCleaner(
		mockOfferPool,
		testScope,
		mockVolumeStore,
		mockSchedulerClient,
		&mockMesosStreamIDProvider{})

	reservation := &mesos.Resource_ReservationInfo{
		Labels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &_testVolumeID,
		},
	}
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(_perHostCPU).
			WithRole(pelotonRole).
			WithReservation(reservation).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(_perHostMem).
			WithReservation(reservation).
			WithRole(pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(_perHostDisk).
			WithRole(pelotonRole).
			WithReservation(reservation).
			WithDisk(diskInfo).
			Build(),
	}
	offer := createMesosOffer(resources)
	reservedOffers := make(map[string]*mesos.Offer)
	reservedOffers[offer.GetId().GetValue()] = offer
	hostOffers := make(map[string]map[string]*mesos.Offer)
	hostOffers[offer.GetHostname()] = reservedOffers
	volumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	volumeInfo := &volume.PersistentVolumeInfo{
		GoalState: volume.VolumeState_CREATED,
	}

	gomock.InOrder(
		mockOfferPool.EXPECT().GetOffers(summary.Reserved).Return(hostOffers, 4),
		mockVolumeStore.EXPECT().GetPersistentVolume(gomock.Any(), volumeID).Return(volumeInfo, nil),
	)

	cleaner.Run(nil)
}

func createMesosOffer(res []*mesos.Resource) *mesos.Offer {
	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &_testOfferID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: res,
	}
}
