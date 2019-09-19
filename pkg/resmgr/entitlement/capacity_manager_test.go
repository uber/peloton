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

package entitlement

import (
	"context"
	"fmt"
	"strings"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	v1_host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc/mocks"

	"github.com/golang/mock/gomock"
)

// TestV0CapacityManager tests V0 capacity manager
func (s *EntitlementCalculatorTestSuite) TestV0CapacityManager() {
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	mockHostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Return(&hostsvc.ClusterCapacityResponse{
			PhysicalResources:      s.createClusterCapacity(),
			PhysicalSlackResources: s.createSlackClusterCapacity(),
		}, nil)

	capMgr := &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

	total, slack, err := capMgr.GetCapacity(context.Background())
	s.NoError(err)
	s.Len(total, 4)
	s.Len(slack, 4)

	mockHostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Return(nil, fmt.Errorf("v0 cluster capacity failed"))
	_, _, err = capMgr.GetCapacity(context.Background())
	s.Error(err)
	s.True(strings.Contains(err.Error(), "v0 cluster capacity failed"))
}

// TestV0HostPoolCapacity tests v0 GetHostPoolCapacity
func (s *EntitlementCalculatorTestSuite) TestV0HostPoolCapacity() {
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	capMgr := &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

	mockHostMgr.EXPECT().GetHostPoolCapacity(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetHostPoolCapacityResponse{
			Pools: []*hostsvc.HostPoolResources{
				{
					PoolName:         "p1",
					PhysicalCapacity: s.createClusterCapacity(),
					SlackCapacity:    s.createSlackClusterCapacity(),
				},
				{
					PoolName:         "p2",
					PhysicalCapacity: s.createClusterCapacity(),
				},
			},
		}, nil)

	hpCap, err := capMgr.GetHostPoolCapacity(context.Background())
	s.NoError(err)

	s.Contains(hpCap, "p1")
	s.Contains(hpCap, "p2")

	s.EqualValues(hpCap["p1"].Physical["cpu"], 100)
	s.EqualValues(hpCap["p1"].Physical["gpu"], 0)
	s.EqualValues(hpCap["p1"].Physical["memory"], 1000)
	s.EqualValues(hpCap["p1"].Physical["disk"], 6000)
	s.EqualValues(hpCap["p1"].Slack["cpu"], 80)
	s.EqualValues(hpCap["p1"].Slack["gpu"], 0)
	s.EqualValues(hpCap["p1"].Slack["memory"], 0)
	s.EqualValues(hpCap["p1"].Slack["disk"], 0)

	s.EqualValues(hpCap["p2"].Physical["cpu"], 100)
	s.EqualValues(hpCap["p2"].Physical["gpu"], 0)
	s.EqualValues(hpCap["p2"].Physical["memory"], 1000)
	s.EqualValues(hpCap["p2"].Physical["disk"], 6000)
	s.EqualValues(hpCap["p2"].Slack["cpu"], 0)
	s.EqualValues(hpCap["p2"].Slack["gpu"], 0)
	s.EqualValues(hpCap["p2"].Slack["memory"], 0)
	s.EqualValues(hpCap["p2"].Slack["disk"], 0)

	mockHostMgr.EXPECT().GetHostPoolCapacity(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("some error"))
	_, err = capMgr.GetHostPoolCapacity(context.Background())
	s.Error(err)
}

// TestV1AlphaCapacityManager tests v1 alpha capacity manager
func (s *EntitlementCalculatorTestSuite) TestV1AlphaCapacityManager() {
	mockV1HostMgr := v1_host_mocks.NewMockHostManagerServiceYARPCClient(
		s.mockCtrl)

	mockV1HostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Return(&svc.ClusterCapacityResponse{
			Capacity: convertV0ToV1HostResource(
				s.createClusterCapacity()),
			SlackCapacity: convertV0ToV1HostResource(
				s.createSlackClusterCapacity()),
		}, nil)

	capMgr := &v1AlphaCapacityManager{
		hostManagerV1: mockV1HostMgr,
	}

	total, slack, err := capMgr.GetCapacity(context.Background())
	s.NoError(err)
	s.Len(total, 4)
	s.Len(slack, 4)

	mockV1HostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Return(nil, fmt.Errorf("v1 cluster capacity failed"))
	_, _, err = capMgr.GetCapacity(context.Background())
	s.Error(err)
	s.True(strings.Contains(err.Error(), "v1 cluster capacity failed"))
}

// TestV1AlphaHostPoolCapacity tests v1 alpha GetHostPoolCapacity
func (s *EntitlementCalculatorTestSuite) TestV1AlphaHostPoolCapacity() {
	mockV1HostMgr := v1_host_mocks.NewMockHostManagerServiceYARPCClient(
		s.mockCtrl)
	capMgr := &v1AlphaCapacityManager{
		hostManagerV1: mockV1HostMgr,
	}

	_, err := capMgr.GetHostPoolCapacity(context.Background())
	s.Error(err)
}
