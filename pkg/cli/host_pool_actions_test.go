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
	"errors"
	"testing"

	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	pb_hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	hostmocks "github.com/uber/peloton/.gen/peloton/api/v0/host/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type hostPoolActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostmgr *hostmocks.MockHostServiceYARPCClient
	ctx         context.Context
	pools       []*pb_host.HostPoolInfo
}

func TestHostPoolActionsTestSuite(t *testing.T) {
	suite.Run(t, new(hostPoolActionsTestSuite))
}

func (suite *hostPoolActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostmgr = hostmocks.NewMockHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()

	suite.pools = []*pb_host.HostPoolInfo{
		{
			Name:  "pool1",
			Hosts: []string{"host1", "host2"},
		},
	}
}

func (suite *hostPoolActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *hostPoolActionsTestSuite) newCli() *Client {
	return &Client{
		Debug:      false,
		hostClient: suite.mockHostmgr,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
}

// TestListHostPools tests success/error cases for ListHostPools
func (suite *hostPoolActionsTestSuite) TestListHostPools() {
	c := suite.newCli()

	testcases := map[string]struct {
		err      error
		response *pb_hostsvc.ListHostPoolsResponse
	}{
		"list-pool-success": {
			response: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
		},
		"list-pool-error": {
			err: errors.New("bogus"),
		},
	}

	for tcName, tc := range testcases {
		suite.mockHostmgr.EXPECT().
			ListHostPools(suite.ctx, &host_svc.ListHostPoolsRequest{}).
			Return(tc.response, tc.err)

		if tc.err == nil {
			suite.NoError(c.HostPoolList(), tcName)
		} else {
			suite.Error(c.HostPoolList(), tcName)
		}
	}
}

// TestListHostPoolHosts tests success/error cases for ListHostPoolHosts
func (suite *hostPoolActionsTestSuite) TestListHostPoolHosts() {
	c := suite.newCli()

	testcases := map[string]struct {
		err      error
		poolName string
		response *pb_hostsvc.ListHostPoolsResponse
	}{
		"list-hosts-success": {
			poolName: "pool1",
			response: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
		},
		"list-hosts-error": {
			poolName: "pool1",
			err:      errors.New("bogus"),
		},
		"list-hosts-unknown-pool": {
			poolName: "pool-unknown",
			response: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
			err: errors.New("pool-not-found"),
		},
	}

	for tcName, tc := range testcases {
		var respErr error
		if tc.response == nil {
			respErr = tc.err
		}
		suite.mockHostmgr.EXPECT().
			ListHostPools(suite.ctx, &host_svc.ListHostPoolsRequest{}).
			Return(tc.response, respErr)

		if tc.err == nil {
			suite.NoError(c.HostPoolListHosts(tc.poolName), tcName)
		} else {
			suite.Error(c.HostPoolListHosts(tc.poolName), tcName)
		}
	}
}

// TestCreateHostPool tests success/error cases for CreateHostPool
func (suite *hostPoolActionsTestSuite) TestCreateHostPool() {
	c := suite.newCli()

	testcases := map[string]struct {
		err      error
		poolName string
	}{
		"create-pool-success": {
			poolName: "pool-new",
		},
		"create-pool-error": {
			err: errors.New("bogus"),
		},
	}

	for tcName, tc := range testcases {
		suite.mockHostmgr.EXPECT().
			CreateHostPool(suite.ctx,
				&host_svc.CreateHostPoolRequest{
					Name: tc.poolName,
				}).
			Return(nil, tc.err)

		if tc.err == nil {
			suite.NoError(c.HostPoolCreate(tc.poolName), tcName)
		} else {
			suite.Error(c.HostPoolCreate(tc.poolName), tcName)
		}
	}
}

// TestDeleteHostPool tests success/error cases for DeleteHostPool
func (suite *hostPoolActionsTestSuite) TestDeleteHostPool() {
	c := suite.newCli()

	testcases := map[string]struct {
		err      error
		poolName string
	}{
		"delete-pool-success": {
			poolName: "pool1",
		},
		"delete-pool-error": {
			err: errors.New("bogus"),
		},
	}

	for tcName, tc := range testcases {
		suite.mockHostmgr.EXPECT().
			DeleteHostPool(suite.ctx,
				&host_svc.DeleteHostPoolRequest{
					Name: tc.poolName,
				}).
			Return(nil, tc.err)

		if tc.err == nil {
			suite.NoError(c.HostPoolDelete(tc.poolName), tcName)
		} else {
			suite.Error(c.HostPoolDelete(tc.poolName), tcName)
		}
	}
}

// TestChangeHostPool tests success/error cases for ChangeHostPool
func (suite *hostPoolActionsTestSuite) TestChangeHostPool() {
	c := suite.newCli()

	testcases := map[string]struct {
		host, sourcePool, destPool string
		err                        error
		listErr                    error
		listResponse               *pb_hostsvc.ListHostPoolsResponse
	}{
		"change-pool-success": {
			host:       "host1",
			sourcePool: "pool1",
			destPool:   "pool2",
		},
		"change-pool-error": {
			host:       "host1",
			sourcePool: "pool1",
			destPool:   "pool2",
			err:        errors.New("change-pool-failed"),
		},
		"change-pool-no-source-success": {
			host:     "host1",
			destPool: "pool2",
			listResponse: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
		},
		"change-pool-no-source-error": {
			host:     "host1",
			destPool: "pool2",
			err:      errors.New("change-pool-failed"),
			listResponse: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
		},
		"change-pool-no-source-list-error": {
			host:     "host1",
			destPool: "pool2",
			err:      errors.New("list-pools-failed"),
			listErr:  errors.New("list-pools-failed"),
		},
		"change-pool-no-source-bad-host": {
			host:     "host-unknown",
			destPool: "pool2",
			err:      errors.New("unknown-host"),
			listResponse: &pb_hostsvc.ListHostPoolsResponse{
				Pools: suite.pools,
			},
		},
	}

	for tcName, tc := range testcases {

		if len(tc.sourcePool) == 0 {
			suite.mockHostmgr.EXPECT().
				ListHostPools(suite.ctx, &host_svc.ListHostPoolsRequest{}).
				Return(tc.listResponse, tc.listErr)
		}
		if tc.listErr == nil && tc.host == "host1" {
			suite.mockHostmgr.EXPECT().
				ChangeHostPool(suite.ctx,
					&host_svc.ChangeHostPoolRequest{
						Hostname:        tc.host,
						SourcePool:      "pool1",
						DestinationPool: tc.destPool,
					}).
				Return(nil, tc.err)
		}

		if tc.err == nil {
			suite.NoError(
				c.HostPoolChangePool(tc.host, tc.sourcePool, tc.destPool),
				tcName)
		} else {
			suite.Error(
				c.HostPoolChangePool(tc.host, tc.sourcePool, tc.destPool),
				tcName)
		}
	}
}
