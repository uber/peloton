package reserver

import (
	"context"
	"errors"
	"strconv"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/hostmgr/config"
	"code.uber.internal/infra/peloton/hostmgr/metrics"
	offerpool_mocks "code.uber.internal/infra/peloton/hostmgr/offer/offerpool/mocks"
	sum "code.uber.internal/infra/peloton/hostmgr/summary"
	summary_mocks "code.uber.internal/infra/peloton/hostmgr/summary/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	_cpuName  = "cpus"
	_memName  = "mem"
	_diskName = "disk"
	_gpuName  = "gpus"

	_defaultResourceValue = 2
)

var (
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
	_testAgent = "agent"
)

// ReserverTestSuite test suite for Reserver
type ReserverTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	mockPool *offerpool_mocks.MockPool
	reserver Reserver
}

func (suite *ReserverTestSuite) SetupTest() {
	// Setting up the test by that each test will have
	// its own Reserver
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()
	metrics := metrics.NewMetrics(tally.NoopScope)

	config := &config.Config{}

	suite.mockPool = offerpool_mocks.NewMockPool(suite.mockCtrl)
	suite.reserver = NewReserver(
		metrics,
		config,
		suite.mockPool,
	)
}

func TestReserver(t *testing.T) {
	suite.Run(t, new(ReserverTestSuite))
}

// TestReserverStart tests the start for the Reserver
func (suite *ReserverTestSuite) TestReserverStart() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("error")),
	)
	suite.reserver.Start()
}

func (suite *ReserverTestSuite) TestReserverStop() {
	suite.reserver.Stop()
}

// Testing reservation with No Tasks
func (suite *ReserverTestSuite) TestReservationWithNoTasks() {
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "No items in reservation queue")
}

// Testing reservation with Nil Task
func (suite *ReserverTestSuite) TestReservationWithNilTasks() {
	reservation := createReservation()
	reservation.Task = nil
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "Not a valid task")
}

// Testing reservation if that fulfills
func (suite *ReserverTestSuite) TestReservation() {
	reservation := createReservation()
	_, err := suite.reserver.Reserve(context.Background())
	suite.Error(err)

	suite.reserver.EnqueueReservation(context.Background(), reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
	)
	_, err = suite.reserver.Reserve(context.Background())
	suite.NoError(err)
}

// Testing reservation when ther is no host available
func (suite *ReserverTestSuite) TestNoHostReservation() {
	reservation := createReservation()
	_, err := suite.reserver.Reserve(context.Background())
	suite.Error(err)

	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
	)
	_, err = suite.reserver.Reserve(context.Background())
	suite.Error(err)
	suite.Contains(err.Error(), "reservation failed")
}

// Testing reservation while get summay for the host fails
func (suite *ReserverTestSuite) TestHostSummaryReservationError() {
	reservation := createReservation()
	_, err := suite.reserver.Reserve(context.Background())
	suite.Error(err)

	suite.reserver.GetReservationQueue().Enqueue(reservation)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("Error")),
	)
	_, err = suite.reserver.Reserve(context.Background())
	suite.Error(err)
	suite.Contains(err.Error(), "reservation failed")
}

// Testing to find the completed reservation if that succeeds
func (suite *ReserverTestSuite) TestFindCompletedReservations() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.Equal(len(failed), 0)
	item, err := suite.reserver.DequeueCompletedReservation(
		context.Background(),
		1)
	suite.NoError(err)
	suite.NotNil(item)
}

func (suite *ReserverTestSuite) TestDequeueCompletedReservations() {
	_, err := suite.reserver.DequeueCompletedReservation(
		context.Background(),
		1)
	require.Error(suite.T(), err)
	suite.Equal(err.Error(), errNoCompletedReservation.Error())

}

// Testing to find the completed reservation however gets the error in reservation
func (suite *ReserverTestSuite) TestFindCompletedReservationsError() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("error")).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
}

// testing the completed reservation  while get status of host has error
func (suite *ReserverTestSuite) TestFindCompletedReservationsCasError() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
}

// Testing the successful reservation
func (suite *ReserverTestSuite) TestCancelReservation() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		summary.EXPECT().GetHostStatus().Return(sum.ReservedHost).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
	err = suite.reserver.CancelReservations(failed)
	suite.NoError(err)
}

// Testing if cancel reservation failed with no host error
func (suite *ReserverTestSuite) TestCancelReservationError() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("error")).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		summary.EXPECT().GetHostStatus().Return(sum.ReservedHost).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
	err = suite.reserver.CancelReservations(failed)
	suite.NoError(err)
}

// Testing if cancel reservation fails
func (suite *ReserverTestSuite) TestCancelReservationNil() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("error")).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		summary.EXPECT().GetHostStatus().Return(sum.ReservedHost).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
	failed["host1"] = nil
	err = suite.reserver.CancelReservations(failed)
	suite.NoError(err)
}

// This tests when we could not update the status of the host to placing
func (suite *ReserverTestSuite) TestMakeHostAvailableError() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetOffers(gomock.Any()).Return(suite.createUnreservedMesosOffers(2)),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(errors.New("error")),
		summary.EXPECT().GetHostStatus().Return(sum.ReservedHost).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 1)
	failed["host1"] = nil
	err = suite.reserver.CancelReservations(failed)
	suite.NoError(err)
}

// Testing the scenario when getting resources from host offers failed
func (suite *ReserverTestSuite) TestgetResourcesFromHostOffersFailed() {
	reservation := createReservation()
	suite.reserver.GetReservationQueue().Enqueue(reservation)
	summary := summary_mocks.NewMockHostSummary(suite.mockCtrl)
	gomock.InOrder(
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(summary, nil).Times(1),
		summary.EXPECT().GetHostStatus().Return(sum.ReadyHost).Times(2),
		summary.EXPECT().CasStatus(gomock.Any(), gomock.Any()).Return(nil),
		suite.mockPool.EXPECT().GetHostSummary(gomock.Any()).Return(nil, errors.New("error")).Times(1),
	)
	_, err := suite.reserver.Reserve(context.Background())
	suite.NoError(err)
	failed := suite.reserver.FindCompletedReservations(context.Background())
	suite.NotNil(failed)
	suite.Equal(len(failed), 0)
}

func (suite *ReserverTestSuite) createUnreservedMesosOffer(
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

func (suite *ReserverTestSuite) createUnreservedMesosOffers(count int) map[string]*mesos.Offer {
	offers := make(map[string]*mesos.Offer)
	for i := 0; i < count; i++ {
		offerID := "offer-id-" + strconv.Itoa(i)
		offers[offerID] = suite.createUnreservedMesosOffer(offerID)
	}
	return offers
}

func createReservation() *hostsvc.Reservation {
	return &hostsvc.Reservation{
		Task:  createResMgrTask(),
		Hosts: createHostInfo(),
	}
}

func createHostInfo() []*hostsvc.HostInfo {
	return []*hostsvc.HostInfo{
		{
			Hostname: "host1",
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName(_cpuName).
					WithValue(_defaultResourceValue).
					Build(),
				util.NewMesosResourceBuilder().
					WithName(_memName).
					WithValue(_defaultResourceValue).
					Build(),
				util.NewMesosResourceBuilder().
					WithName(_diskName).
					WithValue(_defaultResourceValue).
					Build(),
				util.NewMesosResourceBuilder().
					WithName(_gpuName).
					WithValue(_defaultResourceValue).
					Build(),
			},
		},
	}
}

func createResMgrTask() *resmgr.Task {
	return &resmgr.Task{
		Name:     "task",
		Hostname: "hostname",
		Type:     resmgr.TaskType_UNKNOWN,
		Id: &peloton.TaskID{
			Value: "task-1",
		},
		Constraint: &task.Constraint{},
		Resource: &task.ResourceConfig{
			MemLimitMb:  1,
			GpuLimit:    1,
			DiskLimitMb: 1,
			CpuLimit:    1,
		},
	}
}
