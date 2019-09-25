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

package resizer

import (
	"fmt"
	"testing"
	"time"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	cqosmocks "github.com/uber/peloton/.gen/qos/v1alpha1/mocks"
	movermocks "github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover/mocks"
	poolmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_testHighScore   = 70
	_testLowScore    = 10
	_simulateHotPool = true
)

// ResizerTestSuite is test suite for host pool manager.
type ResizerTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	mockHostPoolMgr *poolmocks.MockHostPoolManager
	mockCQosClient  *cqosmocks.MockQoSAdvisorServiceYARPCClient
	mockHostMover   *movermocks.MockHostMover
}

// SetupTest is setup function for this suite.
func (suite *ResizerTestSuite) SetupSuite() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.mockCQosClient = cqosmocks.NewMockQoSAdvisorServiceYARPCClient(
		suite.ctrl,
	)
	suite.mockHostPoolMgr = poolmocks.NewMockHostPoolManager(suite.ctrl)
	suite.mockHostMover = movermocks.NewMockHostMover(suite.ctrl)
}

func (suite *ResizerTestSuite) getResizer() *resizer {
	r := NewResizer(
		suite.mockHostPoolMgr,
		suite.mockCQosClient,
		suite.mockHostMover,
		Config{
			ResizeInterval: 1 * time.Millisecond,
			PoolResizeRange: PoolResizeRange{
				Lower: 50,
				Upper: 75,
			},
			MoveBatchSize: 10,
		},
		tally.NoopScope,
	)

	// Set the minimum wait before resize to 0, to avoid random
	// sleep in the tests.
	r.config.MinWaitBeforeResize = 0
	return r
}

func getPools() (stateless, shared hostpool.HostPool) {
	stateless = hostpool.New(
		common.StatelessHostPoolID,
		tally.NoopScope,
	)
	shared = hostpool.New(
		common.SharedHostPoolID,
		tally.NoopScope,
	)

	numHosts := 100
	for i := 0; i < numHosts; i++ {
		hostname := fmt.Sprintf("host-%d", i)
		if i%2 == 0 {
			stateless.Add(hostname)
		} else {
			shared.Add(hostname)
		}
	}
	return
}

func generateHostMetrics(
	numHosts int,
	highScore bool,
) map[string]*cqos.Metrics {
	hosts := make(map[string]*cqos.Metrics)
	var score int32 = _testLowScore
	if highScore {
		score = _testHighScore
	}
	for i := 0; i < numHosts; i++ {
		hostname := fmt.Sprintf("host-%d", i)
		hosts[hostname] = &cqos.Metrics{
			Score: score,
		}
	}
	return hosts
}

func (suite *ResizerTestSuite) TearDownSuite() {
	suite.ctrl.Finish()
}

// TestResizerTestSuite runs ResizerTestSuite.
func TestResizerTestSuite(t *testing.T) {
	suite.Run(t, new(ResizerTestSuite))
}

// TestResizerStartStop tests start and stop of resizer instance.
func (suite *ResizerTestSuite) TestResizerStartStop() {
	r := suite.getResizer()
	r.Start()
	r.Stop()
}

// TestTryResizeHot resizes the pool if it is hot for a duration of time.
func (suite *ResizerTestSuite) TestTryResizeHot() {
	r := suite.getResizer()
	stateless, shared := getPools()
	hotHostMetrics := generateHostMetrics(100, _simulateHotPool)

	suite.mockCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any(),
		).Return(&cqos.GetHostMetricsResponse{
		Hosts: hotHostMetrics,
	}, nil).Times(3)

	suite.mockHostPoolMgr.EXPECT().
		GetPool(common.StatelessHostPoolID).
		Return(stateless, nil).Times(3)
	suite.mockHostPoolMgr.EXPECT().
		GetPool(common.SharedHostPoolID).
		Return(shared, nil).Times(3)

	// Resizer will mark the stateless pool as hot but not move hosts to it.
	// Resizer will wait for a min configured period and all subsequent runs,
	// till this period is met, will be noops.
	r.runOnce()

	// On the next runOnce, resizer will try to resize the pool. Hence,
	// a MoveHosts call is expected.
	suite.mockHostMover.EXPECT().MoveHosts(
		gomock.Any(),
		shared.ID(),
		int32(40),
		stateless.ID(),
		int32(60),
	).Return(nil)
	r.runOnce()

	// On the next runOnce call, resizer will again mark this pool as hot,
	// but not try to move hosts to it. So a MoveHosts call is not triggered.
	r.runOnce()
}

// TestTryResizeCold resizes the pool if it is cold for a duration of time.
func (suite *ResizerTestSuite) TestTryResizeCold() {
	r := suite.getResizer()
	stateless, shared := getPools()
	coldHostMetrics := generateHostMetrics(100, !_simulateHotPool)

	suite.mockCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any(),
		).Return(&cqos.GetHostMetricsResponse{
		Hosts: coldHostMetrics,
	}, nil).Times(3)

	suite.mockHostPoolMgr.EXPECT().
		GetPool(common.StatelessHostPoolID).
		Return(stateless, nil).Times(3)
	suite.mockHostPoolMgr.EXPECT().
		GetPool(common.SharedHostPoolID).
		Return(shared, nil).Times(3)

	// Resizer will mark the stateless pool as hot but not move hosts to it.
	// Resizer will wait for a min configured period and all subsequent runs,
	// till this period is met, will be noops.
	r.runOnce()

	// On the next runOnce, resizer will try to resize the pool. Hence,
	// a MoveHosts call is expected.
	suite.mockHostMover.EXPECT().MoveHosts(
		gomock.Any(),
		stateless.ID(),
		int32(40),
		shared.ID(),
		int32(60),
	).Return(nil)
	r.runOnce()

	// On the next runOnce call, resizer will again mark this pool as hot,
	// but not try to move hosts to it. So a MoveHosts call is not triggered.
	r.runOnce()
}

// TestPoolResizeMap tests if the poolResizeMap in the resizer is
// correctly populated on every `tryResize` call.
func (suite *ResizerTestSuite) TestPoolResizeMap() {
	r := suite.getResizer()
	stateless, shared := getPools()

	_, ok := r.poolResizeMap.Load(stateless.ID())
	suite.False(ok)

	err := r.tryResize(shared, stateless)
	suite.NoError(err)
	_, ok = r.poolResizeMap.Load(stateless.ID())
	suite.True(ok)

	suite.mockHostMover.EXPECT().MoveHosts(
		gomock.Any(),
		shared.ID(),
		int32(40),
		stateless.ID(),
		int32(60),
	).Return(nil)
	err = r.tryResize(shared, stateless)
	suite.NoError(err)
	_, ok = r.poolResizeMap.Load(stateless.ID())
	suite.False(ok)

	// Dummy src pool has no hosts, so the resize will now fail.
	dummySrcPool := hostpool.New("dummy", tally.NoopScope)
	// Force stateless pool to be in the resizer map.
	r.poolResizeMap.Store(stateless.ID(), time.Now())
	err = r.tryResize(dummySrcPool, stateless)
	suite.True(yarpcerrors.IsResourceExhausted(err))

	// Simulate a case where src pool has <= batch size hosts.
	// In this case, we should move whatever is available in
	// src pool.

	// Add one host to dummy src pool.
	dummySrcPool.Add("host1")
	// Force stateless pool to be in the resizer map.
	r.poolResizeMap.Store(stateless.ID(), time.Now())
	// Hence we will try to move, one host from dummy to stateless.
	suite.mockHostMover.EXPECT().MoveHosts(
		gomock.Any(),
		dummySrcPool.ID(),
		int32(0),
		stateless.ID(),
		int32(51),
	).Return(nil)
	err = r.tryResize(dummySrcPool, stateless)
	suite.NoError(err)
}
