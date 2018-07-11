package cli

import (
	"context"
	"fmt"
	"testing"

	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type hostmgrActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostmgr *host_mocks.MockHostServiceYARPCClient
	ctx         context.Context
}

func TestHostmgrActions(t *testing.T) {
	suite.Run(t, new(hostmgrActionsTestSuite))
}

func (suite *hostmgrActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostmgr = host_mocks.NewMockHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *hostmgrActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *hostmgrActionsTestSuite) TestClient_HostMaintenanceStartAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &host_svc.StartMaintenanceResponse{}

	suite.mockHostmgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostMaintenanceStartAction("hostname:0.0.0.0")
	suite.NoError(err)

	// Test StartMaintenance error
	suite.mockHostmgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake StartMaintenance error"))
	err = c.HostMaintenanceStartAction("hostname:0.0.0.0")
	suite.Error(err)

	// Test empty machine error
	err = c.HostMaintenanceStartAction("")
	suite.Error(err)

	//Test duplicate machineID error
	err = c.HostMaintenanceStartAction("hostname:0.0.0.0, hostname:0.0.0.0")
	suite.Error(err)

	// Test invalid input error
	err = c.HostMaintenanceStartAction("Invalid")
	suite.Error(err)
}

func (suite *hostmgrActionsTestSuite) TestClient_HostMaintenanceCompleteAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &host_svc.CompleteMaintenanceResponse{}

	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostMaintenanceCompleteAction("hostname:0.0.0.0")
	suite.NoError(err)

	//Test CompleteMaintenance error
	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake CompleteMaintenance error"))
	err = c.HostMaintenanceCompleteAction("hostname:0.0.0.0")
	suite.Error(err)

	// Test empty machineID error
	err = c.HostMaintenanceCompleteAction("")
	suite.Error(err)

	//Test duplicate machineID error
	err = c.HostMaintenanceCompleteAction("hostname:0.0.0.0, hostname:0.0.0.0")
	suite.Error(err)

	// Test invalid input error
	err = c.HostMaintenanceStartAction("Invalid")
	suite.Error(err)
}

func (suite *hostmgrActionsTestSuite) TestClient_HostQueryAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &host_svc.QueryHostsResponse{
		HostInfos: make([]*host.HostInfo, 1),
	}

	suite.mockHostmgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostQueryAction("HOST_STATE_DRAINING")
	suite.NoError(err)

	suite.mockHostmgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake QueryHosts error"))
	err = c.HostQueryAction("")
	suite.Error(err)
}
