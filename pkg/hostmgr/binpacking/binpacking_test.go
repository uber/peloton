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
	"testing"

	"github.com/stretchr/testify/suite"
)

type BinPackingTestSuite struct {
	suite.Suite
}

func TestBinPackingTestSuiteTestSuite(t *testing.T) {
	suite.Run(t, new(BinPackingTestSuite))
}

func (suite *BinPackingTestSuite) SetupTest() {
	Init()
}

// TestInit tests the Init() function
func (suite *BinPackingTestSuite) TestInit() {
	suite.Equal(2, len(rankers))
	suite.NotNil(rankers[DeFrag])
	suite.Equal(rankers[DeFrag].Name(), DeFrag)
	suite.NotNil(rankers[FirstFit])
	suite.Equal(rankers[FirstFit].Name(), FirstFit)
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
	suite.Equal(2, len(result))
	expectedNames := []string{DeFrag, FirstFit}
	suite.Contains(expectedNames, result[0].Name())
	suite.Contains(expectedNames, result[1].Name())
}
