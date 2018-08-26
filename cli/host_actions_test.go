package cli

import (
	"context"
	"fmt"
	"testing"

	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	hostsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type hostmgrActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostmgr *hostmocks.MockHostServiceYARPCClient
	ctx         context.Context
}

func TestHostmgrActions(t *testing.T) {
	suite.Run(t, new(hostmgrActionsTestSuite))
}

func (suite *hostmgrActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostmgr = hostmocks.NewMockHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *hostmgrActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *hostmgrActionsTestSuite) TestClientHostMaintenanceStartAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &hostsvc.StartMaintenanceResponse{}

	suite.mockHostmgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostMaintenanceStartAction("hostname")
	suite.NoError(err)

	// Test StartMaintenance error
	suite.mockHostmgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake StartMaintenance error"))
	err = c.HostMaintenanceStartAction("hostname")
	suite.Error(err)

	// Test empty hostname error
	err = c.HostMaintenanceStartAction("")
	suite.Error(err)

	//Test duplicate hostname error
	err = c.HostMaintenanceStartAction("hostname, hostname")
	suite.Error(err)

	// Test invalid input error
	err = c.HostMaintenanceStartAction("hostname,,")
	suite.Error(err)
}

func (suite *hostmgrActionsTestSuite) TestClientHostMaintenanceCompleteAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &hostsvc.CompleteMaintenanceResponse{}

	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostMaintenanceCompleteAction("hostname")
	suite.NoError(err)

	//Test CompleteMaintenance error
	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake CompleteMaintenance error"))
	err = c.HostMaintenanceCompleteAction("hostname")
	suite.Error(err)

	// Test empty hostname error
	err = c.HostMaintenanceCompleteAction("")
	suite.Error(err)

	//Test duplicate hostname error
	err = c.HostMaintenanceCompleteAction("hostname, hostname")
	suite.Error(err)

	// Test invalid input error
	err = c.HostMaintenanceStartAction("hostname,,")
	suite.Error(err)
}

func (suite *hostmgrActionsTestSuite) TestClientHostQueryAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	tt := []struct {
		debug bool
		resp  *hostsvc.QueryHostsResponse
		err   error
	}{
		{
			resp: &hostsvc.QueryHostsResponse{
				HostInfos: make([]*host.HostInfo, 1),
			},
			err: nil,
		},
		{
			debug: true,
			resp: &hostsvc.QueryHostsResponse{
				HostInfos: make([]*host.HostInfo, 1),
			},
			err: nil,
		},
		{
			resp: &hostsvc.QueryHostsResponse{
				HostInfos: []*host.HostInfo{},
			},
			err: nil,
		},
		{
			resp: nil,
			err:  fmt.Errorf("fake QueryHosts error"),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockHostmgr.EXPECT().
			QueryHosts(gomock.Any(), gomock.Any()).
			Return(t.resp, t.err)
		if t.err != nil {
			suite.Error(c.HostQueryAction(""))
		} else {
			suite.NoError(c.HostQueryAction("HOST_STATE_DRAINING"))
		}
	}
}
