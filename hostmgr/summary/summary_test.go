package summary

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	constraint_mocks "code.uber.internal/infra/peloton/common/constraints/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

var (
	_testKey0    = "testkey0"
	_testKey1    = "testkey1"
	_testValue0  = "testvalue0"
	_testValue1  = "testvalue1"
	_cpuName     = "cpus"
	_pelotonRole = "peloton"
	_memName     = "mem"
	_diskName    = "disk"
	_gpuName     = "gpus"
	_portsName   = "ports"
	_testAgent   = "agent"

	_cpuRes = util.NewMesosResourceBuilder().
		WithName(_cpuName).
		WithValue(1.0).
		Build()
	_memRes = util.NewMesosResourceBuilder().
		WithName(_memName).
		WithValue(1.0).
		Build()
	_diskRes = util.NewMesosResourceBuilder().
			WithName(_diskName).
			WithValue(1.0).
			Build()
	_gpuRes = util.NewMesosResourceBuilder().
		WithName(_gpuName).
		WithValue(1.0).
		Build()
	_portsRes = util.NewMesosResourceBuilder().
			WithName(_portsName).
			WithRanges(util.CreatePortRanges(
			map[uint32]bool{1: true, 2: true})).
		Build()
)

type HostOfferSummaryTestSuite struct {
	suite.Suite

	offer           *mesos.Offer
	labels1         *mesos.Labels
	labels2         *mesos.Labels
	ctrl            *gomock.Controller
	mockVolumeStore *store_mocks.MockPersistentVolumeStore
}

func (suite *HostOfferSummaryTestSuite) SetupTest() {
	suite.labels1 = &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_testKey0,
				Value: &_testValue0,
			},
		},
	}
	suite.labels2 = &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_testKey1,
				Value: &_testValue1,
			},
		},
	}
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockVolumeStore = store_mocks.NewMockPersistentVolumeStore(suite.ctrl)
}

// Test various constraint matching cases.
func (suite *HostOfferSummaryTestSuite) TestConstraintMatchForResources() {

	testTable := []struct {
		expected hostsvc.HostFilterResult
		filter   *hostsvc.HostFilter
		offer    *mesos.Offer
		msg      string
	}{
		{
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
						GpuLimit:    1.0,
					},
				},
			},
			offer: &mesos.Offer{
				Resources: []*mesos.Resource{
					_cpuRes,
					_memRes,
					_diskRes,
					_gpuRes,
					_portsRes,
				},
			},
			msg: "Enough resource with GPU",
		},
		{
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  2.0,
						DiskLimitMb: 1.0,
						GpuLimit:    1.0,
					},
				},
			},
			offer: &mesos.Offer{
				Resources: []*mesos.Resource{
					_cpuRes,
					_memRes,
					_diskRes,
					_gpuRes,
				},
			},
			msg: "Not enough memory",
		},
		{
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(3),
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
					},
				},
			},
			offer: &mesos.Offer{
				Resources: []*mesos.Resource{
					_cpuRes,
					_memRes,
					_diskRes,
					_portsRes,
				},
			},
			msg: "Not enough ports",
		},
		{
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
					},
				},
			},
			offer: &mesos.Offer{
				Resources: []*mesos.Resource{
					_cpuRes,
					_memRes,
					_diskRes,
				},
			},
			msg: "Enough resource without GPU",
		},
		{
			expected: hostsvc.HostFilterResult_MISMATCH_GPU,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
					},
				},
			},
			offer: &mesos.Offer{
				Resources: []*mesos.Resource{
					_cpuRes,
					_memRes,
					_diskRes,
					_gpuRes,
				},
			},
			msg: "GPU machines are exclusive",
		},
		{
			expected: hostsvc.HostFilterResult_MISMATCH_STATUS,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
					},
				},
			},
			offer: nil,
			msg:   "Empty offer map",
		},
	}

	for _, tt := range testTable {
		offerMap := make(map[string]*mesos.Offer)

		if tt.offer != nil {
			offerMap["o1"] = tt.offer
		}

		suite.Equal(
			tt.expected,
			matchHostFilter(
				offerMap,
				tt.filter,
				nil),
			tt.msg,
		)
	}
}

func (suite *HostOfferSummaryTestSuite) createReservedMesosOffer(
	offerID string) *mesos.Offer {

	reservation1 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels1,
	}
	reservation2 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels2,
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &offerID,
		},
	}
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(_cpuName).
			WithValue(1.0).
			WithRole(_pelotonRole).
			WithReservation(reservation1).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(_memName).
			WithValue(2.0).
			WithReservation(reservation2).
			WithRole(_pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(_diskName).
			WithValue(3.0).
			WithRole(_pelotonRole).
			WithReservation(reservation1).
			WithDisk(diskInfo).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(_gpuName).
			WithValue(5.0).
			Build(),
	}

	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: rs,
	}
}

func (suite *HostOfferSummaryTestSuite) createUnreservedMesosOffer(
	offerID string) *mesos.Offer {
	rs := []*mesos.Resource{
		_cpuRes,
		_memRes,
		_diskRes,
		_gpuRes,
	}

	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: rs,
	}
}

func (suite *HostOfferSummaryTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestHostOfferSummaryTestSuite(t *testing.T) {
	suite.Run(t, new(HostOfferSummaryTestSuite))
}

func (suite *HostOfferSummaryTestSuite) TestAddRemoveHybridOffers() {
	defer suite.ctrl.Finish()
	// Add offer concurrently.
	reservedOffers := 5
	unreservedOffers := 5
	nOffers := reservedOffers + unreservedOffers
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	hybridSummary := New(suite.mockVolumeStore).(*hostSummary)

	suite.False(hybridSummary.HasOffer())
	suite.False(hybridSummary.HasAnyOffer())

	var offers []*mesos.Offer
	for i := 0; i < reservedOffers; i++ {
		offerID := fmt.Sprintf("reserved-%d", i)
		offers = append(offers, suite.createReservedMesosOffer(offerID))
	}
	for i := 0; i < unreservedOffers; i++ {
		offerID := fmt.Sprintf("unreserved-%d", i)
		offers = append(offers, suite.createUnreservedMesosOffer(offerID))
	}

	volumeInfo := &volume.PersistentVolumeInfo{}

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(context.Background(), gomock.Any()).
		AnyTimes().
		Return(volumeInfo, nil)
	suite.mockVolumeStore.EXPECT().
		UpdatePersistentVolume(context.Background(), gomock.Any(), volume.VolumeState_CREATED).
		AnyTimes().
		Return(nil)

	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			defer wg.Done()

			status := hybridSummary.AddMesosOffer(context.Background(), offer)
			suite.Equal(ReadyOffer, status)
		}(offer)
	}
	wg.Wait()

	// Verify aggregated resources for reserved part.
	suite.Equal(reservedOffers, len(hybridSummary.reservedOffers))
	suite.Equal(
		reservedOffers,
		len(hybridSummary.reservedResources[suite.labels1.String()].Volumes),
	)
	suite.Empty(hybridSummary.reservedResources[suite.labels2.String()].Volumes)
	for label, res := range hybridSummary.reservedResources {
		if label == suite.labels1.String() {
			suite.Equal(res.Resources.CPU, 5.0)
			suite.Equal(res.Resources.Disk, 0.0)
		} else {
			suite.Equal(res.Resources.Mem, 10.0)
		}
	}

	// Verify resources for unreserved part.
	suite.True(hybridSummary.HasOffer())
	suite.True(hybridSummary.HasAnyOffer())
	unreservedAmount := hybridSummary.UnreservedAmount()
	suite.Equal(5.0, unreservedAmount.CPU)
	suite.Equal(5.0, unreservedAmount.Mem)
	suite.Equal(5.0, unreservedAmount.Disk)
	suite.Equal(5.0, unreservedAmount.GPU)

	// Remove offer concurrently.
	wg = sync.WaitGroup{}
	wg.Add(nOffers)
	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			defer wg.Done()

			status, offer := hybridSummary.RemoveMesosOffer(*offer.Id.Value)
			suite.Equal(ReadyOffer, status)
			suite.NotNil(offer)
		}(offer)
	}
	wg.Wait()

	// Verify aggregated resources.
	suite.Empty(hybridSummary.reservedOffers)
	suite.Empty(hybridSummary.reservedResources)
	suite.Empty(hybridSummary.unreservedOffers)
}

func (suite *HostOfferSummaryTestSuite) TestTryMatch() {
	defer suite.ctrl.Finish()
	offer := suite.createUnreservedMesosOffer("offer-id")

	testTable := []struct {
		match  hostsvc.HostFilterResult
		offers []*mesos.Offer

		evaluateRes   constraints.EvaluateResult
		evaluateErr   error
		initialStatus CacheStatus
		noMock        bool
	}{
		{
			match:         hostsvc.HostFilterResult_MATCH,
			offers:        []*mesos.Offer{offer},
			evaluateRes:   constraints.EvaluateResultMatch,
			initialStatus: ReadyOffer,
		},
		{
			match:         hostsvc.HostFilterResult_MATCH,
			offers:        []*mesos.Offer{offer},
			evaluateRes:   constraints.EvaluateResultNotApplicable,
			initialStatus: ReadyOffer,
		},

		{
			match:         hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateRes:   constraints.EvaluateResultMismatch,
			initialStatus: ReadyOffer,
		},
		{
			match:         hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateErr:   errors.New("some error"),
			initialStatus: ReadyOffer,
		},
		{
			match:         hostsvc.HostFilterResult_MISMATCH_STATUS,
			initialStatus: PlacingOffer,
			noMock:        true,
		},
	}

	for _, tt := range testTable {
		ctrl := gomock.NewController(suite.T())
		mockEvaluator := constraint_mocks.NewMockEvaluator(ctrl)

		s := New(suite.mockVolumeStore).(*hostSummary)
		s.status = tt.initialStatus
		suite.Equal(tt.initialStatus, s.AddMesosOffer(context.Background(), offer))
		filter := &hostsvc.HostFilter{
			SchedulingConstraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind: task.LabelConstraint_TASK,
				},
			},
		}

		lv := constraints.GetHostLabelValues(_testAgent, offer.Attributes)

		if !tt.noMock {
			mockEvaluator.
				EXPECT().
				Evaluate(
					gomock.Eq(filter.SchedulingConstraint),
					gomock.Eq(lv)).
				Return(tt.evaluateRes, tt.evaluateErr)
		}

		match, offers := s.TryMatch(filter, mockEvaluator)
		suite.Equal(tt.match, match, "test case is %v", tt)
		suite.Equal(tt.offers, offers)
		ctrl.Finish()
	}
}

func (suite *HostOfferSummaryTestSuite) TestResetExpiredPlacingOfferStatus() {
	defer suite.ctrl.Finish()

	now := time.Now()

	testTable := []struct {
		initialStatus                CacheStatus
		statusPlacingOfferExpiration time.Time
		resetExpected                bool
		msg                          string
	}{
		{
			initialStatus:                ReadyOffer,
			statusPlacingOfferExpiration: now,
			resetExpected:                false,
			msg:                          "HostSummary in ReadyOffer status",
		},
		{
			initialStatus:                PlacingOffer,
			statusPlacingOfferExpiration: now.Add(10 * time.Minute),
			resetExpected:                false,
			msg:                          "HostSummary in PlacingOffer status, has not timed out",
		},
		{
			initialStatus:                PlacingOffer,
			statusPlacingOfferExpiration: now.Add(-10 * time.Minute),
			resetExpected:                true,
			msg:                          "HostSummary in PlacingOffer status, has timed out",
		},
	}

	for _, tt := range testTable {
		s := &hostSummary{
			status: tt.initialStatus,
			statusPlacingOfferExpiration: tt.statusPlacingOfferExpiration,
		}
		reset, _ := s.ResetExpiredPlacingOfferStatus(now)
		suite.Equal(tt.resetExpected, reset, tt.msg)
	}
}

// TODO: Add the following test cases:
// - Ready offers are used for constraints matching
// - placing offers are skipped for constraint matching
// - return offer call path;
// - remove offer call path;
// - count of `readyOffers`;
// - when resources on a summary already sent for placement, new offers are added as `PlacingOffer` state.
