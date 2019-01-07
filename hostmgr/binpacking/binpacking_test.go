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

func (suite *BinPackingTestSuite) TestInit() {
	suite.EqualValues(rankers[DeFrag]().Name(), DeFrag)
	suite.EqualValues(rankers[FirstFit]().Name(), FirstFit)
}

func (suite *BinPackingTestSuite) TestRegister() {
	rankers[DeFrag] = nil
	Register(DeFrag, nil)
	suite.Nil(rankers[DeFrag])
	Register(DeFrag, NewDeFragRanker)
	suite.Nil(rankers[DeFrag])
	delete(rankers, DeFrag)
	Register(DeFrag, NewDeFragRanker)
	suite.NotNil(rankers[DeFrag])
}

func (suite *BinPackingTestSuite) TestCreateRanker() {
	ranker := CreateRanker(DeFrag)
	suite.EqualValues(ranker.Name(), DeFrag)
	ranker = CreateRanker("Not_existing")
	suite.Nil(ranker)
}
