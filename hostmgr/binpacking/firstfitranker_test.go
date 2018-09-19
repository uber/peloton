package binpacking

import (
	"testing"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	hmutil "code.uber.internal/infra/peloton/hostmgr/util"

	"github.com/stretchr/testify/suite"
)

type FirstFitRankerTestSuite struct {
	suite.Suite
	firstfitRanker Ranker
	offerIndex     map[string]summary.HostSummary
}

func TestFirstFitRankerTestSuite(t *testing.T) {
	suite.Run(t, new(FirstFitRankerTestSuite))
}

func (suite *FirstFitRankerTestSuite) SetupTest() {
	suite.firstfitRanker = NewFirstFitRanker()
	suite.offerIndex = CreateOfferIndex()
}

func (suite *FirstFitRankerTestSuite) TestName() {
	suite.EqualValues(suite.firstfitRanker.Name(), FirstFit)
}

func (suite *FirstFitRankerTestSuite) TestGetRankedHostList() {
	sortedList := suite.firstfitRanker.GetRankedHostList(suite.offerIndex)
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
}