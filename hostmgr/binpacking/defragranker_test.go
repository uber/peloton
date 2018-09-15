package binpacking

import (
	"context"
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	hmutil "code.uber.internal/infra/peloton/hostmgr/util"
	"code.uber.internal/infra/peloton/util"

	"github.com/stretchr/testify/suite"
)

type DeFragRankerTestSuite struct {
	suite.Suite
	defragRanker Ranker
	offerIndex   map[string]summary.HostSummary
}

func TestDeFragRankerTestSuite(t *testing.T) {
	suite.Run(t, new(DeFragRankerTestSuite))
}

func (suite *DeFragRankerTestSuite) SetupTest() {
	suite.defragRanker = NewDeFragRanker()
	suite.offerIndex = CreateOfferIndex()
}

func (suite *DeFragRankerTestSuite) TestName() {
	suite.EqualValues(suite.defragRanker.Name(), DeFrag)
}

func (suite *DeFragRankerTestSuite) TestGetRankedHostList() {
	sortedList := suite.defragRanker.GetRankedHostList(suite.offerIndex)
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

func CreateOfferIndex() map[string]summary.HostSummary {
	offerIndex := make(map[string]summary.HostSummary)
	hostName0 := "hostname0"
	offer0 := CreateOffer(hostName0, scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	summry0 := summary.New(nil, nil, hostName0, nil)
	summry0.AddMesosOffers(context.Background(), []*mesos.Offer{offer0})
	offerIndex[hostName0] = summry0

	hostName1 := "hostname1"
	offer1 := CreateOffer(hostName1, scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})
	summry1 := summary.New(nil, nil, hostName1, nil)
	summry1.AddMesosOffers(context.Background(), []*mesos.Offer{offer1})
	offerIndex[hostName1] = summry1

	hostName2 := "hostname2"
	offer2 := CreateOffer(hostName2, scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})
	summry2 := summary.New(nil, nil, hostName2, nil)
	summry2.AddMesosOffers(context.Background(), []*mesos.Offer{offer2})
	offerIndex[hostName2] = summry2

	hostName3 := "hostname3"
	offer3 := CreateOffer(hostName3, scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	summry3 := summary.New(nil, nil, hostName3, nil)
	summry3.AddMesosOffers(context.Background(), []*mesos.Offer{offer3})
	offerIndex[hostName3] = summry3

	hostName4 := "hostname4"
	offer4 := CreateOffer(hostName4, scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	summry4 := summary.New(nil, nil, hostName4, nil)
	summry4.AddMesosOffers(context.Background(), []*mesos.Offer{offer4})
	offerIndex[hostName4] = summry4
	return offerIndex
}

func CreateOffer(
	hostName string,
	resource scalar.Resources) *mesos.Offer {
	offerID := fmt.Sprintf("%s-%d", hostName, 1)
	agentID := fmt.Sprintf("%s-%d", hostName, 1)
	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &agentID,
		},
		Hostname: &hostName,
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName("cpus").
				WithValue(resource.CPU).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("mem").
				WithValue(resource.Mem).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(resource.Disk).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("gpus").
				WithValue(resource.GPU).
				Build(),
		},
	}
}
