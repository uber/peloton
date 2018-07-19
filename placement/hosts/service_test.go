package hosts

import (
	"context"
	"errors"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/placement/metrics"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	_error    = "error"
	_hostname = "hostname"
)

// ServiceTestSuite for testing the hosts service
type ServiceTestSuite struct {
	// suite
	suite.Suite
	// mock ontroller
	mockCtrl *gomock.Controller
	// resource manager mock client
	resmgrClient *resource_mocks.MockResourceManagerServiceYARPCClient
	// host manager mock client
	hostMgrClient *host_mocks.MockInternalHostServiceYARPCClient
	// metrics object
	metrics *metrics.Metrics
	// host service object
	hostService Service
}

// SetupTest is setting up the common mock clients for all the tests
// we need to do it in setup tests as they have to be different for each task
func (suite *ServiceTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.resmgrClient = resource_mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.hostMgrClient = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)
	suite.metrics = metrics.NewMetrics(tally.NoopScope)
	suite.hostService = NewService(suite.hostMgrClient, suite.resmgrClient, suite.metrics)
}

// Running test suite
func TestHostService(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}

// TestHostsService_AcquireHosts tests the acquire hosts call and
// validating the desired output, without error
func (suite *ServiceTestSuite) TestHostsService_AcquireHosts() {
	defer suite.mockCtrl.Finish()

	ctx := context.Background()
	filter := &hostsvc.HostFilter{}
	hosts := &hostsvc.GetHostsResponse{
		Hosts: []*hostsvc.HostInfo{
			{
				Hostname: _hostname,
			},
		},
	}
	task := createResMgrTask()

	gomock.InOrder(
		suite.hostMgrClient.EXPECT().
			GetHosts(
				gomock.Any(),
				getHostRequest(filter),
			).Return(hosts, nil),
		suite.resmgrClient.EXPECT().
			GetTasksByHosts(gomock.Any(),
				&resmgrsvc.GetTasksByHostsRequest{
					Type:      resmgr.TaskType_UNKNOWN,
					Hostnames: []string{_hostname},
				},
			).Return(
			&resmgrsvc.GetTasksByHostsResponse{
				HostTasksMap: map[string]*resmgrsvc.TaskList{
					_hostname: {
						Tasks: []*resmgr.Task{
							task,
						},
					},
				},
				Error: nil,
			}, nil),
	)

	hostsRet, err := suite.hostService.GetHosts(ctx, task, filter)
	//validating the same out which we are passing in the mock calls
	suite.NoError(err)
	suite.Equal(1, len(hostsRet))
	suite.Equal(_hostname, hostsRet[0].GetHost().Hostname)
	suite.Equal(1, len(hostsRet[0].Tasks))
}

// TestHostsService_ReserveHosts tests the ReserveHosts call
func (suite *ServiceTestSuite) TestHostsService_ReserveHosts() {
	defer suite.mockCtrl.Finish()
	ctx := context.Background()
	err := suite.hostService.ReserveHost(ctx, nil, nil)
	suite.Error(err)
	suite.Equal(err.Error(), _notImplemented)
}

// TestHostsService_GetCompletedReservation tests the GetCompletedReservation call
func (suite *ServiceTestSuite) TestHostsService_GetCompletedReservation() {
	defer suite.mockCtrl.Finish()
	ctx := context.Background()
	_, err := suite.hostService.GetCompletedReservation(ctx)
	suite.Error(err)
	suite.Equal(err.Error(), _notImplemented)
}

// TestHostsService_ErrorInGetTasks is testing the error in GetTasks()
// call of resource manager client
func (suite *ServiceTestSuite) TestHostsService_ErrorInGetTasks() {
	defer suite.mockCtrl.Finish()
	ctx := context.Background()
	filter := &hostsvc.HostFilter{}
	hosts := &hostsvc.GetHostsResponse{
		Hosts: []*hostsvc.HostInfo{
			{
				Hostname: _hostname,
			},
		},
	}

	gomock.InOrder(
		suite.hostMgrClient.EXPECT().
			GetHosts(
				gomock.Any(),
				getHostRequest(filter),
			).Return(hosts, nil),
		// Simulating error in resmgr getTasks call
		suite.resmgrClient.EXPECT().
			GetTasksByHosts(gomock.Any(),
				&resmgrsvc.GetTasksByHostsRequest{
					Type:      resmgr.TaskType_UNKNOWN,
					Hostnames: []string{_hostname},
				},
			).Return(
			nil, errors.New(_error)),
	)
	hostsRet, err := suite.hostService.GetHosts(ctx, createResMgrTask(), filter)
	suite.Error(err)
	suite.Nil(hostsRet)
	// validating the same error been passed
	suite.Equal(err.Error(), _error)
}

// TestHostsService_ErrorInGetHosts tests the error in GetHosts call from
// host manager
func (suite *ServiceTestSuite) TestHostsService_ErrorInGetHosts() {
	defer suite.mockCtrl.Finish()
	ctx := context.Background()
	filter := &hostsvc.HostFilter{}

	gomock.InOrder(
		suite.hostMgrClient.EXPECT().
			GetHosts(
				gomock.Any(),
				getHostRequest(filter),
			).Return(nil, errors.New(_error)),
	)
	hostsRet, err := suite.hostService.GetHosts(ctx, createResMgrTask(), filter)
	suite.Error(err)
	suite.Nil(hostsRet)
	suite.Equal(err.Error(), _failedToAcquireHosts)
}

// TestHostsService_ErrorResponseInGetHosts tests the error in response
// validiating the same error in tests which is been generated at the time
// of generating the mock call
func (suite *ServiceTestSuite) TestHostsService_ErrorResponseInGetHosts() {
	defer suite.mockCtrl.Finish()
	errFailed := "failed in gethosts"
	ctx := context.Background()
	filter := &hostsvc.HostFilter{}
	hosts := &hostsvc.GetHostsResponse{
		Hosts: nil,
		// Generating the error
		Error: &hostsvc.GetHostsResponse_Error{
			Failure: &hostsvc.GetHostsFailure{
				Message: errFailed,
			},
		},
	}

	gomock.InOrder(
		suite.hostMgrClient.EXPECT().
			GetHosts(
				gomock.Any(),
				getHostRequest(filter),
			).Return(hosts, nil),
	)
	hostsRet, err := suite.hostService.GetHosts(ctx, createResMgrTask(), filter)
	suite.Error(err)
	suite.Nil(hostsRet)
	// validating the same error
	suite.Contains(err.Error(), errFailed)
}

// createResMgrTask returns the dummy resource manager task
func createResMgrTask() *resmgr.Task {
	return &resmgr.Task{
		Name:     "task",
		Hostname: _hostname,
		Type:     resmgr.TaskType_UNKNOWN,
	}
}

// getHostRequest returns the GetHostsRequest based on the passed filter
func getHostRequest(filter *hostsvc.HostFilter) *hostsvc.GetHostsRequest {
	return &hostsvc.GetHostsRequest{
		Filter: filter,
	}
}
