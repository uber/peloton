// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law orupd agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	hostpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/pkg/storage/objects/base"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"
)

type HostInfoObjectTestSuite struct {
	suite.Suite
}

func (s *HostInfoObjectTestSuite) SetupTest() {
	setupTestStore()
}

func TestHostInfoObjectSuite(t *testing.T) {
	suite.Run(t, new(HostInfoObjectTestSuite))
}

// TestHostInfo tests ORM DB operations for HostInfo
func (s *HostInfoObjectTestSuite) TestHostInfo() {
	db := NewHostInfoOps(testStore)

	testHostInfo := &hostpb.HostInfo{
		Hostname:  "hostname1",
		Ip:        "1.2.3.4",
		State:     hostpb.HostState_HOST_STATE_UP,
		GoalState: hostpb.HostState_HOST_STATE_DRAINING,
	}

	// Test Create
	err := db.Create(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.Ip,
		testHostInfo.State,
		testHostInfo.GoalState)
	s.NoError(err)

	// Test Get
	hostInfoGet, err := db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGet)

	// Test GetAll
	testHostInfo2 := &hostpb.HostInfo{
		Hostname:  "hostname2",
		Ip:        "5.6.7.8",
		State:     hostpb.HostState_HOST_STATE_UP,
		GoalState: hostpb.HostState_HOST_STATE_DRAINING,
	}
	err = db.Create(
		context.Background(),
		testHostInfo2.Hostname,
		testHostInfo2.Ip,
		testHostInfo2.State,
		testHostInfo2.GoalState)
	s.NoError(err)
	hostInfosAll, err := db.GetAll(context.Background())
	s.NoError(err)
	s.Len(hostInfosAll, 2)
	// fetched records in reverse created order / latest first
	s.Equal(testHostInfo2, hostInfosAll[0])
	s.Equal(testHostInfo, hostInfosAll[1])

	// Test Update
	testHostInfo.State = hostpb.HostState_HOST_STATE_DRAINING
	testHostInfo.GoalState = hostpb.HostState_HOST_STATE_DRAINED
	err = db.Update(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.State,
		testHostInfo.GoalState)
	s.NoError(err)
	hostInfoGot, err := db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGot)

	// Test Delete
	err = db.Delete(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	hostInfo, err := db.Get(context.Background(), testHostInfo.Hostname)
	s.Error(err)
	s.Nil(hostInfo)
	hostInfosAll, err = db.GetAll(context.Background())
	s.NoError(err)
	s.Len(hostInfosAll, 1)
	s.Equal(testHostInfo2, hostInfosAll[0])
}

// TestCreateGetGetAllDeleteHostInfoFail tests failure cases due to ORM Client errors
func (s *HostInfoObjectTestSuite) TestCreateGetGetAllDeleteHostInfoFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	db := NewHostInfoOps(mockStore)

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("Create failed"))
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(errors.New("Get failed"))
	mockClient.EXPECT().GetAll(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("GetAll failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("Delete failed"))

	ctx := context.Background()

	testHostInfo := &hostpb.HostInfo{
		Hostname:  "hostname1",
		Ip:        "1.2.3.4",
		State:     hostpb.HostState_HOST_STATE_UP,
		GoalState: hostpb.HostState_HOST_STATE_DRAINING,
	}

	err := db.Create(
		ctx,
		testHostInfo.Hostname,
		testHostInfo.Ip,
		testHostInfo.State,
		testHostInfo.GoalState)

	s.Error(err)
	s.Equal("Create failed", err.Error())

	_, err = db.Get(ctx, testHostInfo.Hostname)
	s.Error(err)
	s.Equal("Get failed", err.Error())

	_, err = db.GetAll(ctx)
	s.Error(err)
	s.Equal("GetAll failed", err.Error())

	err = db.Delete(ctx, testHostInfo.Hostname)
	s.Error(err)
	s.Equal("Delete failed", err.Error())
}

func (s *HostInfoObjectTestSuite) TestNewHostInfoFromHostInfoObject() {
	hostInfoObject := &HostInfoObject{
		Hostname:   &base.OptionalString{Value: "hostname"},
		IP:         "1.2.3.4",
		State:      "HOST_STATE_UP",
		GoalState:  "HOST_STATE_DRAINING",
		UpdateTime: time.Now(),
	}
	s.Equal(
		&hostpb.HostInfo{
			Hostname:  "hostname",
			Ip:        "1.2.3.4",
			State:     hostpb.HostState_HOST_STATE_UP,
			GoalState: hostpb.HostState_HOST_STATE_DRAINING,
		},
		newHostInfoFromHostInfoObject(hostInfoObject),
	)
}
