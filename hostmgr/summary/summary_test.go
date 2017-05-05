package summary

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	constraint_mocks "code.uber.internal/infra/peloton/common/constraints/mocks"
	"code.uber.internal/infra/peloton/util"
	"github.com/golang/mock/gomock"
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

	offer   *mesos.Offer
	labels1 *mesos.Labels
	labels2 *mesos.Labels
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
}

// Test various constraint matching cases.
func (suite *HostOfferSummaryTestSuite) TestConstraintMatchForResources() {

	testTable := []struct {
		expected   MatchResult
		constraint *hostsvc.Constraint
		offer      *mesos.Offer
		msg        string
	}{
		{
			expected: Matched,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			expected: InsufficientResources,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			expected: InsufficientResources,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			expected: Matched,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			expected: MismatchGPU,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			expected: MismatchStatus,
			constraint: &hostsvc.Constraint{
				HostLimit: 1,
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
			matchConstraint(
				offerMap,
				tt.constraint,
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
	// Add offer concurrently.
	reservedOffers := 5
	unreservedOffers := 5
	nOffers := reservedOffers + unreservedOffers
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	hybridSummary := New().(*hostSummary)

	suite.False(hybridSummary.HasOffer())

	var offers []*mesos.Offer
	for i := 0; i < reservedOffers; i++ {
		offerID := fmt.Sprintf("reserved-%d", i)
		offers = append(offers, suite.createReservedMesosOffer(offerID))
	}
	for i := 0; i < unreservedOffers; i++ {
		offerID := fmt.Sprintf("unreserved-%d", i)
		offers = append(offers, suite.createUnreservedMesosOffer(offerID))
	}

	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			defer wg.Done()

			status := hybridSummary.AddMesosOffer(offer)
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
	offer := suite.createUnreservedMesosOffer("offer-id")

	testTable := []struct {
		match  MatchResult
		offers []*mesos.Offer

		evaluateRes   constraints.EvaluateResult
		evaluateErr   error
		initialStatus CacheStatus
		noMock        bool
	}{
		{
			match:         Matched,
			offers:        []*mesos.Offer{offer},
			evaluateRes:   constraints.EvaluateResultMatch,
			initialStatus: ReadyOffer,
		},
		{
			match:         Matched,
			offers:        []*mesos.Offer{offer},
			evaluateRes:   constraints.EvaluateResultNotApplicable,
			initialStatus: ReadyOffer,
		},

		{
			match:         MismatchAttributes,
			evaluateRes:   constraints.EvaluateResultMismatch,
			initialStatus: ReadyOffer,
		},
		{
			match:         MismatchAttributes,
			evaluateErr:   errors.New("some error"),
			initialStatus: ReadyOffer,
		},
		{
			match:         MismatchStatus,
			initialStatus: PlacingOffer,
			noMock:        true,
		},
	}

	for _, tt := range testTable {
		ctrl := gomock.NewController(suite.T())
		mockEvaluator := constraint_mocks.NewMockEvaluator(ctrl)

		s := New().(*hostSummary)
		s.status = tt.initialStatus
		suite.Equal(tt.initialStatus, s.AddMesosOffer(offer))
		constraint := &hostsvc.Constraint{
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
					gomock.Eq(constraint.SchedulingConstraint),
					gomock.Eq(lv)).
				Return(tt.evaluateRes, tt.evaluateErr)
		}

		match, offers := s.TryMatch(constraint, mockEvaluator)
		suite.Equal(tt.match, match, "test case is %v", tt)
		suite.Equal(tt.offers, offers)
		ctrl.Finish()
	}
}

// TODO: Add the following test cases:
// - Ready offers are used for constraints matching
// - placing offers are skipped for constraint matching
// - return offer call path;
// - remove offer call path;
// - count of `readyOffers`;
// - when resources on a summary already sent for placement, new offers are added as `PlacingOffer` state.
