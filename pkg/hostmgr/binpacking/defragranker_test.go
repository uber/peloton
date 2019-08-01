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

	"github.com/golang/mock/gomock"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"

	"github.com/stretchr/testify/suite"
)

type DeFragRankerTestSuite struct {
	ctx context.Context
	suite.Suite
	defragRanker   Ranker
	offerIndex     map[string]summary.HostSummary
	ctrl           *gomock.Controller
	watchProcessor *watchmocks.MockWatchProcessor
}

func TestDeFragRankerTestSuite(t *testing.T) {
	suite.Run(t, new(DeFragRankerTestSuite))
}

func (suite *DeFragRankerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.defragRanker = NewDeFragRanker()
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.offerIndex = CreateOfferIndex(suite.watchProcessor)
}

func (suite *DeFragRankerTestSuite) TestName() {
	suite.EqualValues(suite.defragRanker.Name(), DeFrag)
}

func (suite *DeFragRankerTestSuite) TestGetRankedHostList() {
	sortedList := suite.defragRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
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
}

func (suite *DeFragRankerTestSuite) TestGetRankedHostListWithRefresh() {
	// Getting the sorted list based on first call
	sortedList := suite.defragRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.EqualValues(len(sortedList), 5)
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
	// Adding new host and check we still not get
	// the new list before we call refresh
	// Checking if we get the previous list
	AddHostToIndex(5, suite.offerIndex, suite.watchProcessor)
	sortedListNew := suite.defragRanker.GetRankedHostList(suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedListNew), 5)
	// Refresh the ranker
	suite.defragRanker.RefreshRanking(suite.ctx, suite.offerIndex)
	// NOw it should get the new list
	sortedListNew = suite.defragRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedListNew), 6)
	suite.EqualValues(hmutil.GetResourcesFromOffers(
		sortedListNew[5].(summary.HostSummary).GetOffers(summary.All)),
		scalar.Resources{CPU: 5, Mem: 5, Disk: 5, GPU: 5})
}
