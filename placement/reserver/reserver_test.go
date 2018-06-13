package reserver

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/placement/config"
	hosts_mock "code.uber.internal/infra/peloton/placement/hosts/mocks"
	"code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	// represents the max size of the preemption queue
	_maxReservationQueueSize = 10000
)

type ReserverTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	hostService *hosts_mock.MockService
	reserver    Reserver
}

func (suite *ReserverTestSuite) SetupTest() {
	// Setting up the test by that each test will have
	// its own Reserver and host service
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()
	metrics := metrics.NewMetrics(tally.NoopScope)

	suite.hostService = hosts_mock.NewMockService(suite.mockCtrl)
	config := &config.PlacementConfig{}

	reservationQueue := queue.NewQueue(
		"reservatiom-queue",
		reflect.TypeOf(resmgr.Task{}), _maxReservationQueueSize)

	suite.reserver = NewReserver(
		metrics,
		config,
		suite.hostService,
		reservationQueue,
	)
}

func TestReserver(t *testing.T) {
	suite.Run(t, new(ReserverTestSuite))
}

func (suite *ReserverTestSuite) TestReserverStart() {
	suite.reserver.Start()
}

func (suite *ReserverTestSuite) TestReserverStop() {
	suite.reserver.Stop()
}

// TestReservation tries to test the reservation is working as expected
func (suite *ReserverTestSuite) TestReservation() {
	task := createResMgrTask()
	hosts := []*models.Host{
		{
			Host: creareHostInfo(),
		},
	}

	// Calling mockups for the host service which reserver will call
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts, nil)
	suite.hostService.EXPECT().ReserveHost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Adding task to reservation queue which is simulating the behavior
	// for placement engine got the task from Resource Manager
	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), float64(0))
	suite.Nil(err)
}

// TestReservationNoTasks tests if there is no task in the queue
// to reserve. We are simulating that by not adding any task in
// reservation queue.
func (suite *ReserverTestSuite) TestReservationNoTasks() {
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "No items in reservation queue")
}

// TestReservationInvalidTask making the resmgr task invalid
// by making id nil, which simulate error condition task is not valid
func (suite *ReserverTestSuite) TestReservationInvalidTask() {
	task := createResMgrTask()
	task.Id = nil
	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "Not a valid task")
}

// TestReservationErrorinAcquire simulates the error in Acquire call
// from the HostService and verify that.
func (suite *ReserverTestSuite) TestReservationErrorinAcquire() {
	task := createResMgrTask()
	suite.hostService.EXPECT().
		GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("error in acquire hosts"))

	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noHostsTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "error in acquire hosts")
}

// TestReservationErrorInReservation simulates the error in ReserveHosts call
// from the HostService and verify that.
func (suite *ReserverTestSuite) TestReservationErrorInReservation() {
	task := createResMgrTask()
	hosts := []*models.Host{
		{
			Host: creareHostInfo(),
		},
	}
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts, nil)
	suite.hostService.EXPECT().ReserveHost(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("error in reserve hosts"))

	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noHostsTimeoutPenalty.Seconds())
	suite.Error(err)
	suite.Contains(err.Error(), "error in reserve hosts")
}

// TestTaskLen tests the TaskLen function which gets the
// length of the tasks in HostModel
func (suite *ReserverTestSuite) TestTaskLen() {
	host := &models.Host{
		Host: creareHostInfo(),
		Tasks: []*resmgr.Task{
			createResMgrTask(),
		},
	}
	suite.Equal(taskLen(host), 1)
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
	}
}

func creareHostInfo() *hostsvc.HostInfo {
	return &hostsvc.HostInfo{
		Hostname: "hostname",
	}
}
