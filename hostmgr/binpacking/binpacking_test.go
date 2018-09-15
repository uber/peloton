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
