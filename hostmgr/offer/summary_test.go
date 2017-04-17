package offer

import (
	"fmt"
	"sync"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/hostmgr/reservation"
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
	_testAgent   = "agent"
)

type HostOfferSummaryTestSuite struct {
	suite.Suite

	reservedHostOffer *hostOfferSummary
	offer             *mesos.Offer
	labels1           *mesos.Labels
	labels2           *mesos.Labels
	diskInfo          *mesos.Resource_DiskInfo
}

func (suite *HostOfferSummaryTestSuite) SetupTest() {
	suite.reservedHostOffer = &hostOfferSummary{
		unreservedOffers: make(map[string]*mesos.Offer),
		status:           ReadyOffer,
		reservedOffers:   make(map[string]*mesos.Offer),
		reservedResources: make(
			map[string]*reservation.ReservedResources),
	}

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

func (suite *HostOfferSummaryTestSuite) getMesosOffer(
	offerID string) *mesos.Offer {

	reservation1 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels1,
	}
	reservation2 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels2,
	}
	suite.diskInfo = &mesos.Resource_DiskInfo{
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
			WithDisk(suite.diskInfo).
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

func (suite *HostOfferSummaryTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestHostOfferSummaryTestSuite(t *testing.T) {
	suite.Run(t, new(HostOfferSummaryTestSuite))
}

func (suite *HostOfferSummaryTestSuite) TestAddRemoveReservedMesosOffer() {
	// Add offer concurrently.
	nOffers := 10
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	var offers []*mesos.Offer
	for i := 0; i < nOffers; i++ {
		offerID := fmt.Sprintf("agent-%d", i)
		offer := suite.getMesosOffer(offerID)
		offers = append(offers, offer)
	}
	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			suite.reservedHostOffer.addMesosOffer(offer)
			wg.Done()
		}(offer)
	}
	wg.Wait()

	// Verify aggregated resources.
	suite.Equal(nOffers, len(suite.reservedHostOffer.reservedOffers))
	suite.Equal(
		nOffers,
		len(suite.reservedHostOffer.reservedResources[suite.labels1.String()].Volumes),
	)
	suite.Equal(
		0,
		len(suite.reservedHostOffer.reservedResources[suite.labels2.String()].Volumes),
	)
	for label, res := range suite.reservedHostOffer.reservedResources {
		if label == suite.labels1.String() {
			suite.Equal(res.Resources.CPU, 10.0)
			suite.Equal(res.Resources.Disk, 0.0)
		} else {
			suite.Equal(res.Resources.Mem, 20.0)
		}
	}

	// Remove offer concurrently.
	wg = sync.WaitGroup{}
	wg.Add(nOffers)
	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			suite.reservedHostOffer.removeMesosOffer(*offer.Id.Value)
			wg.Done()
		}(offer)
	}
	wg.Wait()

	// Verify aggregated resources.
	suite.Equal(0, len(suite.reservedHostOffer.reservedOffers))
	suite.Equal(0, len(suite.reservedHostOffer.reservedResources))
}

// TODO: Add the following test cases:
// - Ready offers are used for constraints matching
// - placing offers are skipped for constaint matching
// - return offer call path;
// - remove offer call path;
// - count of `readyOffers`;
// - when resources on a summary already sent for placement, new offers are added as `PlacingOffer` state.
