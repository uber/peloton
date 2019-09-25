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

package cli

import (
	"context"
	"fmt"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	hostmocks "github.com/uber/peloton/.gen/peloton/api/v0/host/svc/mocks"
	pb_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	hostmgrsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmgrMocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/uber/peloton/pkg/common/util"
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
}

func (suite *hostmgrActionsTestSuite) TestClientHostMaintenanceCompleteAction() {
	c := Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	hostname := "hostname"
	resp := &hostsvc.CompleteMaintenanceResponse{
		Hostname: hostname}

	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err := c.HostMaintenanceCompleteAction(hostname)
	suite.NoError(err)

	//Test CompleteMaintenance error
	suite.mockHostmgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake CompleteMaintenance error"))
	err = c.HostMaintenanceCompleteAction(hostname)
	suite.Error(err)

	// Test empty hostname error
	err = c.HostMaintenanceCompleteAction("")
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

type hostmgrActionsInternalTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostMgr *hostmgrMocks.MockInternalHostServiceYARPCClient
	ctx         context.Context
}

func TestHostmgrInternalActions(t *testing.T) {
	suite.Run(t, new(hostmgrActionsInternalTestSuite))
}

func (suite *hostmgrActionsInternalTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = hostmgrMocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *hostmgrActionsInternalTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *hostmgrActionsInternalTestSuite) SetupTest() {
	log.Debug("SetupTest")
}

func (suite *hostmgrActionsInternalTestSuite) TearDownTest() {
	log.Debug("TearDownTest")
}

func newHost(
	hostname string,
	cpu float64,
	gpu float64,
	mem float64,
	disk float64) *hostmgrsvc.GetHostsByQueryResponse_Host {
	return &hostmgrsvc.GetHostsByQueryResponse_Host{
		Hostname: hostname,
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName("cpus").
				WithValue(cpu).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("mem").
				WithValue(mem).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(disk).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("gpu").
				WithValue(gpu).
				Build(),
		},
		Status: "ready",
	}
}

func (suite *hostmgrActionsInternalTestSuite) TestGetHostsByQuery() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	req := &hostmgrsvc.GetHostsByQueryRequest{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    1.0,
			GpuLimit:    2.0,
			MemLimitMb:  1000.0,
			DiskLimitMb: 10000.0,
		},
		CmpLess:   false,
		Hostnames: []string{"host1", "host3"},
	}

	resp := &hostmgrsvc.GetHostsByQueryResponse{
		Hosts: []*hostmgrsvc.GetHostsByQueryResponse_Host{
			newHost("host1", 2.0, 1.0, 1024.0, 20000.0),
			newHost("host2", 3.0, 2.0, 2048.0, 20000.0),
			newHost("host3", 3.0, 2.0, 2048.0, 20000.0),
		},
	}

	suite.mockHostMgr.EXPECT().GetHostsByQuery(gomock.Any(), req).Return(resp, nil)
	err := c.HostsGetAction(1.0, 2.0, 1000.0, 10000.0, false, "host1,host3", false)
	suite.NoError(err)
}

func (suite *hostmgrActionsInternalTestSuite) TestDisableKillTasks() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	suite.mockHostMgr.EXPECT().
		DisableKillTasks(gomock.Any(), gomock.Any()).
		Return(&hostmgrsvc.DisableKillTasksResponse{}, nil)

	suite.NoError(c.DisableKillTasksAction())
}

func (suite *hostmgrActionsInternalTestSuite) TestGetHostsByQueryLessThan() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	req := &hostmgrsvc.GetHostsByQueryRequest{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    4.0,
			GpuLimit:    3.0,
			MemLimitMb:  1000.0,
			DiskLimitMb: 10000.0,
		},
		CmpLess: true,
	}

	resp := &hostmgrsvc.GetHostsByQueryResponse{
		Hosts: []*hostmgrsvc.GetHostsByQueryResponse_Host{
			newHost("host1", 2.0, 1.0, 1024.0, 20000.0),
			newHost("host2", 3.0, 2.0, 2048.0, 20000.0),
		},
	}

	suite.mockHostMgr.EXPECT().GetHostsByQuery(gomock.Any(), req).Return(resp, nil)
	err := c.HostsGetAction(4.0, 3.0, 1000.0, 10000.0, true, "", false)
	suite.NoError(err)
}

func (suite *hostmgrActionsInternalTestSuite) TestGetHostsByQueryNoHost() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	req := &hostmgrsvc.GetHostsByQueryRequest{
		Resource: &pb_task.ResourceConfig{
			CpuLimit: 1.0,
			GpuLimit: 2.0,
		},
		CmpLess: false,
	}

	resp := &hostmgrsvc.GetHostsByQueryResponse{}

	suite.mockHostMgr.EXPECT().GetHostsByQuery(gomock.Any(), req).Return(resp, nil)
	err := c.HostsGetAction(1.0, 2.0, 0.0, 0.0, false, "", false)
	suite.NoError(err)
}
