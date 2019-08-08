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

package offerpool

import (
	"context"
	"testing"
	"time"

	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	rankerutil "github.com/uber/peloton/pkg/hostmgr/binpacking"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
)

type RefreshTestSuite struct {
	suite.Suite
	ctx            context.Context
	defragRanker   binpacking.Ranker
	ctrl           *gomock.Controller
	offerIndex     map[string]summary.HostSummary
	pool           Pool
	watchProcessor *watchmocks.MockWatchProcessor
}

func TestRefreshTestSuite(t *testing.T) {
	binpacking.Init(nil, nil)
	suite.Run(t, new(RefreshTestSuite))
}

func (suite *RefreshTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.defragRanker = binpacking.GetRankerByName(binpacking.DeFrag)
	suite.ctrl = gomock.NewController(suite.T())
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.offerIndex = rankerutil.CreateOfferIndex(suite.watchProcessor)
	suite.pool = &offerPool{
		hostOfferIndex:   suite.offerIndex,
		offerHoldTime:    1 * time.Minute,
		metrics:          NewMetrics(tally.NoopScope),
		binPackingRanker: suite.defragRanker,
		watchProcessor:   suite.watchProcessor,
	}
	suite.defragRanker.RefreshRanking(suite.ctx, nil)
}

func (suite *RefreshTestSuite) TearDownTest() {
	suite.pool = nil
}

func (suite *RefreshTestSuite) TestRefresh() {
	refresher := NewRefresher(suite.pool)
	refresher.Refresh(nil)
	sortedList := suite.defragRanker.GetRankedHostList(suite.ctx,
		suite.offerIndex)
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedList[0].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedList[1].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedList[2].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedList[3].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedList[4].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})

	binpacking.AddHostToIndex(5, suite.offerIndex, suite.watchProcessor)
	sortedListNew := suite.defragRanker.GetRankedHostList(suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedListNew), 5)
	// Refresh the ranker
	refresher.Refresh(nil)
	sortedListNew = suite.defragRanker.GetRankedHostList(suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedListNew), 6)
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedListNew[5].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 5, Mem: 5, Disk: 5, GPU: 5})
}
