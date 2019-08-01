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

type FirstFitRankerTestSuite struct {
	ctx context.Context
	suite.Suite
	firstfitRanker Ranker
	ctrl           *gomock.Controller
	offerIndex     map[string]summary.HostSummary
	watchProcessor *watchmocks.MockWatchProcessor
}

func TestFirstFitRankerTestSuite(t *testing.T) {
	suite.Run(t, new(FirstFitRankerTestSuite))
}

func (suite *FirstFitRankerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.firstfitRanker = NewFirstFitRanker()
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.offerIndex = CreateOfferIndex(suite.watchProcessor)
}

func (suite *FirstFitRankerTestSuite) TestName() {
	suite.EqualValues(suite.firstfitRanker.Name(), FirstFit)
}

func (suite *FirstFitRankerTestSuite) TestGetRankedHostList() {
	sortedList := suite.firstfitRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex)
	suite.EqualValues(len(sortedList), 5)
	for _, sum := range sortedList {
		switch sum.(summary.HostSummary).GetHostname() {
		case "hostname0":
			suite.EqualValues(hmutil.GetResourcesFromOffers(
				sum.(summary.HostSummary).GetOffers(summary.All)),
				scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
		case "hostname1":
			suite.EqualValues(hmutil.GetResourcesFromOffers(
				sum.(summary.HostSummary).GetOffers(summary.All)),
				scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})
		case "hostname2":
			suite.EqualValues(hmutil.GetResourcesFromOffers(
				sum.(summary.HostSummary).GetOffers(summary.All)),
				scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})
		case "hostname3":
			suite.EqualValues(hmutil.GetResourcesFromOffers(
				sum.(summary.HostSummary).GetOffers(summary.All)),
				scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
		case "hostname4":
			suite.EqualValues(hmutil.GetResourcesFromOffers(
				sum.(summary.HostSummary).GetOffers(summary.All)),
				scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
		}
	}
	// Checking by adding new host to ranker it does not effect any thing
	// for the firstfit ranker
	AddHostToIndex(5, suite.offerIndex, suite.watchProcessor)
	sortedListNew := suite.firstfitRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.EqualValues(len(sortedListNew), 6)
	suite.firstfitRanker.RefreshRanking(
		suite.ctx,
		suite.offerIndex)
	sortedListNew = suite.firstfitRanker.GetRankedHostList(
		suite.ctx,
		suite.offerIndex,
	)
	suite.EqualValues(len(sortedListNew), 6)
}
