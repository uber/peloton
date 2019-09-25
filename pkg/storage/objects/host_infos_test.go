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
	"fmt"
	"sync"
	"testing"
	"time"

	hostpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pelotonpb "github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/uber/peloton/pkg/hostmgr/common"
	"github.com/uber/peloton/pkg/storage/objects/base"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type hostInfoObjectTestSuite struct {
	suite.Suite
	ctrl          *gomock.Controller
	mockOrmClient *ormmocks.MockClient
	hostInfoOps   *hostInfoOps
}

func (s *hostInfoObjectTestSuite) SetupTest() {
	setupTestStore()
	s.ctrl = gomock.NewController(s.T())
	s.mockOrmClient = ormmocks.NewMockClient(s.ctrl)
	s.hostInfoOps = &hostInfoOps{
		store: &Store{
			oClient: s.mockOrmClient,
			metrics: testStore.metrics,
		},
	}
}

func (s *hostInfoObjectTestSuite) TearDownTest() {
	db := &hostInfoOps{store: testStore}
	ctx := context.Background()

	hostInfos, err := db.GetAll(ctx)
	s.NoError(err)

	for _, h := range hostInfos {
		s.NoError(db.Delete(ctx, h.GetHostname()))
	}

	s.ctrl.Finish()
}

func TestHostInfoObjectSuite(t *testing.T) {
	suite.Run(t, new(hostInfoObjectTestSuite))
}

// TestHostInfo tests ORM DB operations for HostInfo
func (s *hostInfoObjectTestSuite) TestHostInfo() {
	db := &hostInfoOps{store: testStore}

	testHostInfo := &hostpb.HostInfo{
		Hostname:    "hostname1",
		Ip:          "1.2.3.4",
		State:       hostpb.HostState_HOST_STATE_UP,
		GoalState:   hostpb.HostState_HOST_STATE_DRAINING,
		CurrentPool: "pool1",
		DesiredPool: "pool2",
	}

	labels := make(map[string]string)
	for _, label := range testHostInfo.Labels {
		labels[label.Key] = label.Value
	}

	// Test Create
	err := db.Create(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.Ip,
		testHostInfo.State,
		testHostInfo.GoalState,
		labels,
		testHostInfo.CurrentPool,
		testHostInfo.DesiredPool)
	s.NoError(err)

	// Test Get
	hostInfoGet, err := db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGet)

	// Test GetAll
	testHostInfo2 := &hostpb.HostInfo{
		Hostname:    "hostname2",
		Ip:          "5.6.7.8",
		State:       hostpb.HostState_HOST_STATE_UP,
		GoalState:   hostpb.HostState_HOST_STATE_DRAINING,
		CurrentPool: "pool1",
		DesiredPool: "pool2",
	}
	labels2 := make(map[string]string)
	for _, label := range testHostInfo2.Labels {
		labels2[label.Key] = label.Value
	}
	err = db.Create(
		context.Background(),
		testHostInfo2.Hostname,
		testHostInfo2.Ip,
		testHostInfo2.State,
		testHostInfo2.GoalState,
		labels2,
		testHostInfo2.CurrentPool,
		testHostInfo2.DesiredPool)
	s.NoError(err)
	hostInfosAll, err := db.GetAll(context.Background())
	s.NoError(err)
	s.Len(hostInfosAll, 2)
	// fetched records in reverse created order / latest first
	s.Equal(testHostInfo2, hostInfosAll[0])
	s.Equal(testHostInfo, hostInfosAll[1])

	// Test UpdateState
	testHostInfo.State = hostpb.HostState_HOST_STATE_DRAINING
	err = db.UpdateState(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.State)
	s.NoError(err)
	hostInfoGot, err := db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGot)

	// Test UpdateGoalState
	testHostInfo.GoalState = hostpb.HostState_HOST_STATE_UP
	err = db.UpdateGoalState(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.GoalState)
	s.NoError(err)
	hostInfoGot, err = db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGot)

	// Test UpdateLabels
	testHostInfo.Labels = []*pelotonpb.Label{
		{
			Key:   "label1",
			Value: "value1",
		},
	}
	labels = make(map[string]string)
	for _, label := range testHostInfo.Labels {
		labels[label.Key] = label.Value
	}
	err = db.UpdateLabels(
		context.Background(),
		testHostInfo.Hostname,
		labels)
	s.NoError(err)
	hostInfoGot, err = db.Get(context.Background(), testHostInfo.Hostname)
	s.NoError(err)
	s.Equal(testHostInfo, hostInfoGot)

	// Test UpdateDesiredPool
	testHostInfo.DesiredPool = "pool1"
	err = db.UpdateDesiredPool(
		context.Background(),
		testHostInfo.Hostname,
		testHostInfo.DesiredPool)
	s.NoError(err)
	hostInfoGot, err = db.Get(context.Background(), testHostInfo.Hostname)
	s.EqualValues(testHostInfo.DesiredPool, hostInfoGot.DesiredPool)

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
func (s *hostInfoObjectTestSuite) TestCreateGetGetAllDeleteHostInfoFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	db := &hostInfoOps{
		store: mockStore,
	}

	mockClient.EXPECT().CreateIfNotExists(gomock.Any(), gomock.Any()).Return(errors.New("Create failed"))
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("Get failed"))
	mockClient.EXPECT().GetAll(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("GetAll failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("Delete failed"))

	ctx := context.Background()

	testHostInfo := &hostpb.HostInfo{
		Hostname:    "hostname1",
		Ip:          "1.2.3.4",
		State:       hostpb.HostState_HOST_STATE_UP,
		GoalState:   hostpb.HostState_HOST_STATE_DRAINING,
		CurrentPool: "pool1",
		DesiredPool: "pool2",
	}

	labels := make(map[string]string)
	for _, label := range testHostInfo.Labels {
		labels[label.Key] = label.Value
	}

	err := db.Create(
		ctx,
		testHostInfo.Hostname,
		testHostInfo.Ip,
		testHostInfo.State,
		testHostInfo.GoalState,
		labels,
		testHostInfo.CurrentPool,
		testHostInfo.DesiredPool)

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

// TestUpdatePools tests update current and desired pool on a host.
func (s *hostInfoObjectTestSuite) TestUpdatePools() {
	db := &hostInfoOps{store: testStore}

	ctx := context.Background()

	testHostInfo := &hostpb.HostInfo{
		Hostname:    "hostname1",
		Ip:          "1.2.3.4",
		State:       hostpb.HostState_HOST_STATE_UP,
		GoalState:   hostpb.HostState_HOST_STATE_DRAINING,
		CurrentPool: "pool1",
		DesiredPool: "pool1",
	}

	labels := make(map[string]string)
	for _, label := range testHostInfo.Labels {
		labels[label.Key] = label.Value
	}

	err := db.Create(
		ctx,
		testHostInfo.Hostname,
		testHostInfo.Ip,
		testHostInfo.State,
		testHostInfo.GoalState,
		labels,
		testHostInfo.CurrentPool,
		testHostInfo.DesiredPool,
	)
	s.NoError(err)

	err = db.UpdatePool(ctx, testHostInfo.Hostname, "pool2", "pool3")
	s.NoError(err)
	hostInfo, err := db.Get(ctx, testHostInfo.Hostname)
	s.NoError(err)
	s.EqualValues("pool2", hostInfo.CurrentPool)
	s.Equal("pool3", hostInfo.DesiredPool)

	err = db.UpdateDesiredPool(ctx, testHostInfo.Hostname, "pool2")
	s.NoError(err)
	hostInfo, err = db.Get(ctx, testHostInfo.Hostname)
	s.NoError(err)
	s.EqualValues("pool2", hostInfo.DesiredPool)
}

func (s *hostInfoObjectTestSuite) TestNewHostInfoFromHostInfoObject() {
	hostInfoObject := &HostInfoObject{
		Hostname:   &base.OptionalString{Value: "hostname"},
		IP:         "1.2.3.4",
		State:      "HOST_STATE_UP",
		GoalState:  "HOST_STATE_DRAINING",
		Labels:     "{}",
		UpdateTime: time.Now(),
	}
	info, err := newHostInfoFromHostInfoObject(hostInfoObject)
	s.NoError(err)
	s.Equal(
		&hostpb.HostInfo{
			Hostname:  "hostname",
			Ip:        "1.2.3.4",
			State:     hostpb.HostState_HOST_STATE_UP,
			GoalState: hostpb.HostState_HOST_STATE_DRAINING,
		},
		info,
	)
}

// TestCompareAndSetSuccess tests the success case of comparing and setting
// host info fields
func (s *hostInfoObjectTestSuite) TestCompareAndSetSuccess() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_DOWN",
		"current_pool": "stateless",
		"desired_pool": "stateless",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: hostpb.HostState_HOST_STATE_DRAINING,
	}

	compareFields := map[string]interface{}{
		common.GoalStateField: hostpb.HostState_HOST_STATE_DOWN,
	}

	s.mockOrmClient.EXPECT().Get(gomock.Any(), readHostInfoObject).Return(row, nil)
	s.mockOrmClient.EXPECT().
		Update(gomock.Any(), gomock.Any(), []string{"UpdateTime", "State"}).
		Do(func(_ context.Context, h *HostInfoObject, fieldsToUpdate ...string) {
			s.Equal(hostname, h.Hostname.String())
			s.Equal(
				hostInfoDiff[common.StateField].(hostpb.HostState).String(),
				h.State,
			)
		}).Return(nil)

	s.NoError(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetSuccess tests the failure case of comparing and setting
// host info fields due to error while reading host info
func (s *hostInfoObjectTestSuite) TestCompareAndSetGetFailure() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: hostpb.HostState_HOST_STATE_DRAINING,
	}

	compareFields := map[string]interface{}{
		common.GoalStateField: hostpb.HostState_HOST_STATE_DOWN,
	}

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetEmtpyGet tests the failure case of comparing and setting
// host info fields of a host whose entry doesn't exist in DB
func (s *hostInfoObjectTestSuite) TestCompareAndSetEmtpyGet() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(nil, nil)

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: hostpb.HostState_HOST_STATE_DRAINING,
	}

	compareFields := map[string]interface{}{
		common.GoalStateField: hostpb.HostState_HOST_STATE_DOWN,
	}

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetStateMatchFail tests the failure case of comparing and setting
// host info fields of a host due to state field mismatch
func (s *hostInfoObjectTestSuite) TestCompareAndSetStateMatchFail() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_DOWN",
		"current_pool": "stateless",
		"desired_pool": "stateless",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: hostpb.HostState_HOST_STATE_UP,
	}

	compareFields := map[string]interface{}{
		common.StateField: hostpb.HostState_HOST_STATE_UNKNOWN,
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(row, nil)

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetGoalStateMatchFail tests the failure case of comparing and
// setting host info fields of a host due to goal state field mismatch
func (s *hostInfoObjectTestSuite) TestCompareAndSetGoalStateMatchFail() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_DOWN",
		"current_pool": "stateless",
		"desired_pool": "stateless",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: hostpb.HostState_HOST_STATE_UP,
	}

	compareFields := map[string]interface{}{
		common.GoalStateField: hostpb.HostState_HOST_STATE_UP,
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(row, nil)

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetCurrentPoolMatchFail tests the failure case of comparing and
// setting host info fields of a host due to current pool field mismatch
func (s *hostInfoObjectTestSuite) TestCompareAndSetCurrentPoolMatchFail() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_DOWN",
		"current_pool": "stateless",
		"desired_pool": "stateless",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.CurrentPoolField: "stateless",
	}

	compareFields := map[string]interface{}{
		common.CurrentPoolField: "shared",
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(row, nil)

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetDesiredPoolMatchFail tests the failure case of comparing and
// setting host info fields of a host due to desired pool field mismatch
func (s *hostInfoObjectTestSuite) TestCompareAndSetDesiredPoolMatchFail() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_DOWN",
		"current_pool": "stateless",
		"desired_pool": "stateless",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.DesiredPoolField: "stateless",
	}

	compareFields := map[string]interface{}{
		common.DesiredPoolField: "shared",
	}

	s.mockOrmClient.EXPECT().
		Get(gomock.Any(), readHostInfoObject).
		Return(row, nil)

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetClientUpdateFailure tests the failure case of comparing and
// setting host info fields of a host due to orm client update error
func (s *hostInfoObjectTestSuite) TestCompareAndSetClientUpdateFailure() {
	hostname := "testhost"
	readHostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}

	row := map[string]interface{}{
		"hostname":     hostname,
		"ip":           "1.2.3.4",
		"state":        "HOST_STATE_UP",
		"goal_state":   "HOST_STATE_UP",
		"current_pool": "stateless",
		"desired_pool": "shared",
		"labels":       "",
		"update_time":  time.Now(),
	}

	hostInfoDiff := common.HostInfoDiff{
		common.CurrentPoolField: "shared",
	}

	compareFields := map[string]interface{}{
		common.DesiredPoolField: "shared",
	}

	s.mockOrmClient.EXPECT().Get(gomock.Any(), readHostInfoObject).Return(row, nil)
	s.mockOrmClient.EXPECT().
		Update(gomock.Any(), gomock.Any(), []string{"UpdateTime", "CurrentPool"}).
		Do(func(_ context.Context, h *HostInfoObject, fieldsToUpdate ...string) {
			s.Equal(hostname, h.Hostname.String())
			s.Equal(
				hostInfoDiff[common.CurrentPoolField].(string),
				h.CurrentPool,
			)
		}).Return(yarpcerrors.InternalErrorf("test error"))

	s.Error(
		s.hostInfoOps.CompareAndSet(
			context.Background(),
			hostname,
			hostInfoDiff,
			compareFields,
		),
	)
}

// TestCompareAndSetConcurrent tests concurrent calls to CompareAndSet
// This test simulates changes to host entry during a host move operation
func (s *hostInfoObjectTestSuite) TestCompareAndSetConcurrent() {
	db := &hostInfoOps{store: testStore}

	testHostInfos := []*hostpb.HostInfo{
		{
			Hostname:    "host1",
			Ip:          "1.2.3.4",
			State:       hostpb.HostState_HOST_STATE_UP,
			GoalState:   hostpb.HostState_HOST_STATE_UP,
			CurrentPool: "pool1",
			DesiredPool: "pool2",
		},
		{
			Hostname:    "host2",
			Ip:          "1.2.3.4",
			State:       hostpb.HostState_HOST_STATE_UP,
			GoalState:   hostpb.HostState_HOST_STATE_UP,
			CurrentPool: "pool1",
			DesiredPool: "pool2",
		},
	}

	for _, h := range testHostInfos {
		s.NoError(
			db.Create(
				context.Background(),
				h.GetHostname(),
				h.GetIp(),
				h.GetState(),
				h.GetGoalState(),
				nil,
				h.GetCurrentPool(),
				h.GetDesiredPool(),
			),
		)
	}

	ctx := context.Background()
	wg := sync.WaitGroup{}
	wg.Add(5 * len(testHostInfos))
	for _, h := range testHostInfos {
		// There is one go routine each for
		// 1. Set goal state to DOWN
		// 2. Set current state to DRAINING
		// 3. Set current state to DOWN
		// 4. Set current pool to desired pool and set goal state to UP.
		// 5. Set current state to UP
		go func(h *hostpb.HostInfo) {
			defer wg.Done()

			hostInfoDiff := common.HostInfoDiff{
				common.GoalStateField:   hostpb.HostState_HOST_STATE_UP,
				common.CurrentPoolField: "pool2",
			}

			compareFields := map[string]interface{}{
				common.StateField:       hostpb.HostState_HOST_STATE_DOWN,
				common.GoalStateField:   hostpb.HostState_HOST_STATE_DOWN,
				common.DesiredPoolField: "pool2",
			}

			for {
				err := db.CompareAndSet(ctx, h.GetHostname(), hostInfoDiff, compareFields)
				if err == nil {
					fmt.Println(h.GetHostname(), ": goalstate set to ",
						hostpb.HostState_HOST_STATE_UP.String(),
						" and current pool to pool2, ")
					break
				}

				time.Sleep(time.Second)
			}
		}(h)
		go func(h *hostpb.HostInfo) {
			defer wg.Done()

			hostInfoDiff := common.HostInfoDiff{
				common.StateField: hostpb.HostState_HOST_STATE_UP,
			}

			compareFields := map[string]interface{}{
				common.GoalStateField:   hostpb.HostState_HOST_STATE_UP,
				common.CurrentPoolField: "pool2",
				common.DesiredPoolField: "pool2",
			}

			for {
				err := db.CompareAndSet(ctx, h.GetHostname(), hostInfoDiff, compareFields)
				if err == nil {
					fmt.Println(h.GetHostname(), ": state set to ",
						hostpb.HostState_HOST_STATE_UP.String())
					break
				}

				time.Sleep(time.Second)
			}
		}(h)
		go func(h *hostpb.HostInfo) {
			defer wg.Done()

			hostInfoDiff := common.HostInfoDiff{
				common.GoalStateField: hostpb.HostState_HOST_STATE_DOWN,
			}

			compareFields := map[string]interface{}{
				common.GoalStateField:   hostpb.HostState_HOST_STATE_UP,
				common.CurrentPoolField: "pool1",
				common.DesiredPoolField: "pool2",
			}

			for {
				err := db.CompareAndSet(ctx, h.GetHostname(), hostInfoDiff, compareFields)
				if err == nil {
					fmt.Println(h.GetHostname(), ": goalstate set to ",
						hostpb.HostState_HOST_STATE_DOWN.String())
					break
				}

				time.Sleep(time.Second)
			}
		}(h)
		go func(h *hostpb.HostInfo) {
			defer wg.Done()

			hostInfoDiff := common.HostInfoDiff{
				common.StateField: hostpb.HostState_HOST_STATE_DRAINING,
			}

			compareFields := map[string]interface{}{
				common.GoalStateField:   hostpb.HostState_HOST_STATE_DOWN,
				common.CurrentPoolField: "pool1",
				common.DesiredPoolField: "pool2",
			}

			for {
				err := db.CompareAndSet(ctx, h.GetHostname(), hostInfoDiff, compareFields)
				if err == nil {
					fmt.Println(h.GetHostname(), ": state set to ",
						hostpb.HostState_HOST_STATE_DRAINING.String())
					break
				}

				time.Sleep(time.Second)
			}
		}(h)
		go func(h *hostpb.HostInfo) {
			defer wg.Done()

			hostInfoDiff := common.HostInfoDiff{
				common.StateField: hostpb.HostState_HOST_STATE_DOWN,
			}

			compareFields := map[string]interface{}{
				common.StateField:     hostpb.HostState_HOST_STATE_DRAINING,
				common.GoalStateField: hostpb.HostState_HOST_STATE_DOWN,
			}

			for {
				err := db.CompareAndSet(ctx, h.GetHostname(), hostInfoDiff, compareFields)
				if err == nil {
					fmt.Println(h.GetHostname(), ": state set to ",
						hostpb.HostState_HOST_STATE_DOWN.String())
					break
				}

				time.Sleep(time.Second)
			}
		}(h)
	}

	wg.Wait()

	expectedHostInfos := []*hostpb.HostInfo{
		{
			Hostname:    "host1",
			Ip:          "1.2.3.4",
			State:       hostpb.HostState_HOST_STATE_UP,
			GoalState:   hostpb.HostState_HOST_STATE_UP,
			CurrentPool: "pool2",
			DesiredPool: "pool2",
		},
		{
			Hostname:    "host2",
			Ip:          "1.2.3.4",
			State:       hostpb.HostState_HOST_STATE_UP,
			GoalState:   hostpb.HostState_HOST_STATE_UP,
			CurrentPool: "pool2",
			DesiredPool: "pool2",
		},
	}

	// Check the final state in DB is as expected
	for i, h := range testHostInfos {
		updateHostInfo, err := db.Get(ctx, h.GetHostname())
		s.NoError(err)

		s.Equal(expectedHostInfos[i], updateHostInfo)
	}
}
