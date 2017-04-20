package reservation

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/util"
)

const (
	_pelotonRole = "peloton"
	_cpuNum      = 1.0
	_memNum      = 2.0
	_diskNum     = 3.0
)

var (
	_testKey0   = "testkey0"
	_testKey1   = "testkey1"
	_testValue0 = "testvalue0"
	_testValue1 = "testvalue1"
	_cpuName    = "cpus"
	_memName    = "mem"
	_diskName   = "disk"
)

type ReservationTestSuite struct {
	suite.Suite

	offer    *mesos.Offer
	labels1  *mesos.Labels
	labels2  *mesos.Labels
	diskInfo *mesos.Resource_DiskInfo
}

func (suite *ReservationTestSuite) SetupTest() {
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
	reservation1 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels1,
	}
	reservation2 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels2,
	}
	suite.diskInfo = &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &_testKey0,
		},
	}
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(_cpuName).
			WithValue(_cpuNum).
			WithRole(_pelotonRole).
			WithReservation(reservation1).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(_memName).
			WithValue(_memNum).
			WithReservation(reservation2).
			WithRole(_pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(_diskName).
			WithValue(_diskNum).
			WithRole(_pelotonRole).
			WithReservation(reservation1).
			WithDisk(suite.diskInfo).
			Build(),
	}
	suite.offer = &mesos.Offer{
		Resources: rs,
	}
}

func (suite *ReservationTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestReservationTestSuite(t *testing.T) {
	suite.Run(t, new(ReservationTestSuite))
}

func (suite *ReservationTestSuite) TestGetReservationResources() {
	offers := []*mesos.Offer{}
	nOffers := 10
	for i := 0; i < nOffers; i++ {
		offers = append(offers, suite.offer)
	}
	suite.True(HasLabeledReservedResources(suite.offer))
	reservedResources := GetLabeledReservedResources(offers)
	for label, res := range reservedResources {
		if label == suite.labels1.String() {
			suite.Equal(res.Resources.CPU, 10.0)
			suite.Equal(res.Resources.Disk, 0.0)
			suite.Equal(len(res.Volumes), nOffers)
		} else {
			suite.Equal(res.Resources.Mem, 20.0)
		}
	}
}

func (suite *ReservationTestSuite) TestGetReservationResourcesNoReservation() {
	offers := []*mesos.Offer{}
	resNoReservation := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(_diskName).
			WithValue(_diskNum).
			WithDisk(suite.diskInfo).
			Build(),
	}
	offer := &mesos.Offer{
		Resources: resNoReservation,
	}
	nOffers := 10
	for i := 0; i < nOffers; i++ {
		offers = append(offers, offer)
	}
	suite.False(HasLabeledReservedResources(offer))
	suite.Empty(GetLabeledReservedResources(offers))
}

func (suite *ReservationTestSuite) TestGetReservationResourcesNoLabels() {
	offers := []*mesos.Offer{}
	reservation := &mesos.Resource_ReservationInfo{}
	resNoReservation := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(_cpuName).
			WithValue(_cpuNum).
			WithRole(_pelotonRole).
			WithReservation(reservation).
			Build(),
	}
	offer := &mesos.Offer{
		Resources: resNoReservation,
	}
	nOffers := 10
	for i := 0; i < nOffers; i++ {
		offers = append(offers, offer)
	}
	suite.False(HasLabeledReservedResources(offer))
	suite.Empty(GetLabeledReservedResources(offers))
}
