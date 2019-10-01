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

package binpacking

import (
	"context"
	"testing"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	cqosmocks "github.com/uber/peloton/.gen/qos/v1alpha1/mocks"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type LoadAwareRankerTestSuite struct {
	suite.Suite
	ctx              context.Context
	loadAwareRanker  Ranker
	offerIndex       map[string]summary.HostSummary
	mockedCQosClient *cqosmocks.MockQoSAdvisorServiceYARPCClient
	metric           *metrics.Metrics
	mockCtrl         *gomock.Controller
	watchProcessor   *watchmocks.MockWatchProcessor
	cancelFunc       context.CancelFunc
}

func TestLoadAwareRankerTestSuite(t *testing.T) {
	suite.Run(t, new(LoadAwareRankerTestSuite))
}

func (suite *LoadAwareRankerTestSuite) SetupTest() {
	suite.ctx, suite.cancelFunc = context.WithTimeout(
		context.Background(),
		_rpcTimeout)
	//defer suite.cancelFunc()
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockedCQosClient = cqosmocks.NewMockQoSAdvisorServiceYARPCClient(suite.mockCtrl)
	suite.metric = metrics.NewMetrics(tally.NoopScope)
	suite.loadAwareRanker = NewLoadAwareRanker(suite.mockedCQosClient,
		suite.metric)
	suite.offerIndex = CreateOfferIndex(suite.watchProcessor)
}

func (suite *LoadAwareRankerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
	suite.cancelFunc()
}

func (suite *LoadAwareRankerTestSuite) TestName() {
	suite.EqualValues(suite.loadAwareRanker.Name(), LoadAware)
}

// TestGetRankedHostList return a sorted hostsummary by Load
func (suite *LoadAwareRankerTestSuite) TestGetRankedHostList() {
	suite.setupMocks()

	sortedList := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")
}

// TestGetRankedHostListCqosNotHost tests cQos doesn't return Load value for
// hostname5 but hostname5 is present in offerIndex
// hostname5 will be put into the buttom of the sorted list
// treating it like the heavy loaded.
func (suite *LoadAwareRankerTestSuite) TestGetRankedHostListCqosNoHost() {
	suite.setupMocks()
	// offer index provides 6 hosts from hostname0 to hostname5
	// Cqos only provided 5 hosts from hostname0 to hostname4
	AddHostToIndex(5, suite.offerIndex, suite.watchProcessor)
	sortedList := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")
	// hostname5 will be at the bottom of the list
	suite.Equal(sortedList[5].(summary.HostSummary).GetHostname(),
		"hostname5")
}

// TestHostExistCqosNotOfferIndex tests cQos return Load value for
// hostname5 but hostname5 is not present in offerIndex
// hostname5 will be ignored
func (suite *LoadAwareRankerTestSuite) TestHostExistCqosNotOfferIndex() {
	suite.mockedCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any()).Return(
		&cqos.GetHostMetricsResponse{
			Hosts: map[string]*cqos.Metrics{
				"hostname0": {Score: 0},
				"hostname1": {Score: 10},
				"hostname2": {Score: 80},
				"hostname3": {Score: 20},
				"hostname4": {Score: 100},
				"hostname5": {Score: 10},
			}}, nil)
	// Cqos provides 6 hosts from hostname0 to hostname5
	// offer index only provided 5 hosts from hostname0 to hostname4
	sortedList := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.Equal(len(sortedList), 5)
	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")
}

// TestGetCachedRankedHostListCqosDown tests verify if Cqos advisor is down
// we will use the sortedhostsummarylist from cache
func (suite *LoadAwareRankerTestSuite) TestGetCachedRankedHostListCqosDown() {
	suite.setupMocks()
	sortedList := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)

	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")

	// cqos connection error out
	for i := 0; i < 9; i++ {
		suite.mockedCQosClient.EXPECT().
			GetHostMetrics(
				gomock.Any(),
				gomock.Any()).Return(
			nil, yarpcerrors.UnavailableErrorf("test error"))
	}

	// return the list from cache
	for i := 0; i < 9; i++ {
		suite.loadAwareRanker.RefreshRanking(
			suite.ctx,
			suite.offerIndex,
		)
	}
	sortedList = suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")
}

// TestExpiredRankedHostListCqosDown tests verify if Cqos advisor is down
// for a long time,
// we gonna expire the cached summary list and fall back to first_fit ranker
// TODO: modify the test after making _maxTryTimeout configurable
func (suite *LoadAwareRankerTestSuite) TestExpiredRankedHostListCqosDown() {
	suite.setupMocks()
	sortedList := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)

	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")

	// cqos connection error out
	for i := 0; i < 11; i++ {
		suite.mockedCQosClient.EXPECT().
			GetHostMetrics(
				gomock.Any(),
				gomock.Any()).Return(
			nil, yarpcerrors.UnavailableErrorf("test error"))
	}

	// return the list from cache
	for i := 0; i < 11; i++ {
		suite.loadAwareRanker.RefreshRanking(
			suite.ctx,
			suite.offerIndex,
		)
	}
	sortedList = suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.EqualValues(len(sortedList), 5)
	hosts := []string{
		"hostname0",
		"hostname1",
		"hostname2",
		"hostname3",
		"hostname4"}
	for _, s := range sortedList {
		h := s.(summary.HostSummary).GetHostname()
		suite.Contains(hosts, h)
	}
}

func (suite *LoadAwareRankerTestSuite) TestGetRankedHostListWithRefresh() {
	suite.setupMocks()
	// Getting the sorted list based on first call
	sortedList := suite.loadAwareRanker.GetRankedHostList(suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedList), 5)
	suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
		"hostname4")
	AddHostToIndex(5, suite.offerIndex, suite.watchProcessor)
	suite.mockedCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any()).Return(
		&cqos.GetHostMetricsResponse{
			Hosts: map[string]*cqos.Metrics{
				"hostname0": {Score: 0},
				"hostname1": {Score: 10},
				"hostname2": {Score: 80},
				"hostname3": {Score: 20},
				"hostname4": {Score: 100},
				"hostname5": {Score: 70},
			}}, nil)
	// Refresh the ranker
	suite.loadAwareRanker.RefreshRanking(
		suite.ctx,
		suite.offerIndex,
	)
	// NOw it should get the new list
	sortedListNew := suite.loadAwareRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.EqualValues(len(sortedListNew), 6)
	suite.Equal(sortedListNew[0].(summary.HostSummary).GetHostname(),
		"hostname0")
	suite.Equal(sortedListNew[1].(summary.HostSummary).GetHostname(),
		"hostname1")
	suite.Equal(sortedListNew[2].(summary.HostSummary).GetHostname(),
		"hostname3")
	suite.Equal(sortedListNew[3].(summary.HostSummary).GetHostname(),
		"hostname5")
	suite.Equal(sortedListNew[4].(summary.HostSummary).GetHostname(),
		"hostname2")
	suite.Equal(sortedListNew[5].(summary.HostSummary).GetHostname(),
		"hostname4")
}

func (suite *LoadAwareRankerTestSuite) setupMocks() {
	suite.mockedCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any()).Return(
		&cqos.GetHostMetricsResponse{
			Hosts: map[string]*cqos.Metrics{
				"hostname0": {Score: 0},
				"hostname1": {Score: 10},
				"hostname2": {Score: 80},
				"hostname3": {Score: 20},
				"hostname4": {Score: 100},
			}}, nil)
}
