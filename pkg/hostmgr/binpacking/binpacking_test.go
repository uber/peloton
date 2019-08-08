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
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"testing"

	cqosmocks "github.com/uber/peloton/.gen/qos/v1alpha1/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type BinPackingTestSuite struct {
	suite.Suite
	mockCtrl         *gomock.Controller
	mockedCQosClient *cqosmocks.MockQoSAdvisorServiceYARPCClient
	metric           *metrics.Metrics
}

func TestBinPackingTestSuiteTestSuite(t *testing.T) {
	suite.Run(t, new(BinPackingTestSuite))
}

func (suite *BinPackingTestSuite) SetupTest() {
	suite.mockedCQosClient = cqosmocks.NewMockQoSAdvisorServiceYARPCClient(suite.mockCtrl)
	suite.metric = metrics.NewMetrics(tally.NoopScope)
	Init(suite.mockedCQosClient, suite.metric)
}

// TestInit tests the Init() function
func (suite *BinPackingTestSuite) TestInit() {
	suite.Equal(3, len(rankers))
	suite.NotNil(rankers[DeFrag])
	suite.Equal(rankers[DeFrag].Name(), DeFrag)
	suite.NotNil(rankers[FirstFit])
	suite.Equal(rankers[FirstFit].Name(), FirstFit)
	suite.NotNil(rankers[LoadAware])
	suite.Equal(rankers[LoadAware].Name(), LoadAware)
}

// TestRegister tests the register() function
func (suite *BinPackingTestSuite) TestRegister() {
	rankers[DeFrag] = nil
	register(DeFrag, nil)
	suite.Nil(rankers[DeFrag])
	register(DeFrag, NewDeFragRanker)
	suite.Nil(rankers[DeFrag])
	delete(rankers, DeFrag)
	register(DeFrag, NewDeFragRanker)
	suite.NotNil(rankers[DeFrag])
}

// TestGetRankerByName tests the GetRankerByName() function
func (suite *BinPackingTestSuite) TestGetRankerByName() {
	ranker := GetRankerByName(DeFrag)
	suite.NotNil(ranker)
	suite.Equal(ranker.Name(), DeFrag)

	ranker = GetRankerByName("Not_existing")
	suite.Nil(ranker)
}

// TestGetRankers tests the GetRankers() function
func (suite *BinPackingTestSuite) TestGetRankers() {
	result := GetRankers()
	suite.Equal(3, len(result))
	expectedNames := []string{DeFrag, FirstFit, LoadAware}
	suite.Contains(expectedNames, result[0].Name())
	suite.Contains(expectedNames, result[1].Name())
	suite.Contains(expectedNames, result[2].Name())
}
