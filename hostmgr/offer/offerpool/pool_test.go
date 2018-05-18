package offerpool

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/util"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	hostmgr_summary_mocks "code.uber.internal/infra/peloton/hostmgr/summary/mocks"

	hostmgr_mesos_mocks "code.uber.internal/infra/peloton/hostmgr/mesos/mocks"
	mpb_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	_perHostCPU     = 10.0
	_perHostMem     = 20.0
	pelotonRole     = "peloton"
	_testAgent      = "agent"
	_testAgent1     = "agent-1"
	_testAgent2     = "agent-2"
	_testAgent3     = "agent-3"
	_testAgent4     = "agent-4"
	_testOfferID    = "testOffer"
	_testKey        = "testKey"
	_testValue      = "testValue"
	_streamID       = "streamID"
	_dummyOfferID   = "dummyOfferID"
	_dummyTestAgent = "dummyTestAgent"
)

type mockJSONClient struct {
	rejectedOfferIds map[string]bool
}

type mockMesosStreamIDProvider struct{}

func getMesosOffer(hostName string, offerID string) *mesos.Offer {
	agentID := fmt.Sprintf("%s-%d", hostName, 1)
	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &agentID,
		},
		Hostname: &hostName,
	}
}

func (suite *OfferPoolTestSuite) GetTimedOfferLen() int {
	length := 0
	suite.pool.timedOffers.Range(func(key, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func (c *mockJSONClient) Call(mesosStreamID string, msg proto.Message) error {
	call := msg.(*sched.Call)
	for _, id := range call.Decline.OfferIds {
		c.rejectedOfferIds[*id.Value] = true
	}
	return nil
}

func (suite *OfferPoolTestSuite) createReservedMesosOffer(
	offerID string, hasPersistentVolume bool) *mesos.Offer {
	var _testKey, _testValue, _testAgent string
	_testKey = "key"
	_testValue = "value"
	_testAgent = "agent"
	reservation1 := &mesos.Resource_ReservationInfo{
		Labels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &offerID,
		},
	}
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("1").
			WithValue(1.0).
			WithRole(pelotonRole).
			WithReservation(reservation1).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("1").
			WithValue(2.0).
			WithReservation(reservation1).
			WithRole(pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("1").
			WithValue(5.0).
			Build(),
	}

	if hasPersistentVolume {
		rs = append(
			rs,
			util.NewMesosResourceBuilder().
				WithName("1").
				WithValue(3.0).
				WithRole(pelotonRole).
				WithReservation(reservation1).
				WithDisk(diskInfo).
				Build())
	}

	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: rs,
	}
}

func (suite *OfferPoolTestSuite) createReservedMesosOffers(count int, hasPersistentVolume bool) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < count; i++ {
		offers = append(offers, suite.createReservedMesosOffer("offer-id-"+strconv.Itoa(i), hasPersistentVolume))
	}
	return offers
}

type OfferPoolTestSuite struct {
	suite.Suite

	ctrl                 *gomock.Controller
	pool                 *offerPool
	schedulerClient      *mpb_mocks.MockSchedulerClient
	masterOperatorClient *mpb_mocks.MockMasterOperatorClient
	provider             *hostmgr_mesos_mocks.MockFrameworkInfoProvider
	agent1Offers         []*mesos.Offer
	agent2Offers         []*mesos.Offer
	agent3Offers         []*mesos.Offer
	agent4Offers         []*mesos.Offer
}

func (suite *OfferPoolTestSuite) SetupSuite() {
	for i := 1; i <= 4; i++ {
		var offers []*mesos.Offer
		for j := 1; j <= 10; j++ {
			offer := getMesosOffer(_testAgent+"-"+strconv.Itoa(i), _testAgent+"-"+strconv.Itoa(i)+_testOfferID+"-"+strconv.Itoa(j))
			offers = append(offers, offer)
		}
		if i == 1 {
			suite.agent1Offers = append(suite.agent1Offers, offers...)
		} else if i == 2 {
			suite.agent2Offers = append(suite.agent2Offers, offers...)
		} else if i == 3 {
			suite.agent3Offers = append(suite.agent3Offers, offers...)
		} else {
			suite.agent4Offers = append(suite.agent4Offers, offers...)
		}
	}
}

func (suite *OfferPoolTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	scope := tally.NewTestScope("", map[string]string{})

	suite.schedulerClient = mpb_mocks.NewMockSchedulerClient(suite.ctrl)
	suite.masterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.ctrl)
	suite.provider = hostmgr_mesos_mocks.NewMockFrameworkInfoProvider(suite.ctrl)

	suite.pool = &offerPool{
		hostOfferIndex:             make(map[string]summary.HostSummary),
		offerHoldTime:              1 * time.Minute,
		metrics:                    NewMetrics(scope),
		mSchedulerClient:           suite.schedulerClient,
		mesosFrameworkInfoProvider: suite.provider,
	}

	suite.pool.timedOffers.Range(func(key interface{}, value interface{}) bool {
		suite.pool.timedOffers.Delete(key)
		return true
	})
}

func (suite *OfferPoolTestSuite) TearDownTest() {
	suite.pool = nil
}

func (suite *OfferPoolTestSuite) TestClaimForLaunch() {
	// Launching tasks for host, which does not exist in the offer pool
	_, err := suite.pool.ClaimForLaunch(_dummyTestAgent, true)
	suite.Error(err)

	// Add reserved & unreserved offers, and do ClaimForPlace.
	offers := suite.createReservedMesosOffers(10, true)
	suite.pool.AddOffers(context.Background(), offers)
	suite.pool.AddOffers(context.Background(), suite.agent1Offers)
	suite.pool.AddOffers(context.Background(), suite.agent2Offers)
	suite.pool.AddOffers(context.Background(), suite.agent3Offers)
	suite.pool.AddOffers(context.Background(), suite.agent4Offers)
	suite.Equal(suite.GetTimedOfferLen(), 50)

	for i := 1; i <= len(suite.agent3Offers); i++ {
		suite.pool.timedOffers.Store(_testAgent3+_testOfferID+"-"+strconv.Itoa(i), &TimedOffer{
			Hostname:   _testAgent3,
			Expiration: time.Now().Add(-2 * time.Minute),
		})
	}

	takenHostOffers := map[string][]*mesos.Offer{}
	mutex := &sync.Mutex{}
	nClients := 4
	var limit uint32 = 1
	wg := sync.WaitGroup{}
	wg.Add(nClients)
	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: limit,
		},
	}
	for i := 0; i < nClients; i++ {
		go func(i int) {
			hostOffers, _, err := suite.pool.ClaimForPlace(filter)
			suite.NoError(err)
			suite.Equal(int(limit), len(hostOffers))
			mutex.Lock()
			defer mutex.Unlock()
			for hostname, offers := range hostOffers {
				suite.Equal(
					10,
					len(offers),
					"hostname %s has incorrect offer length",
					hostname)
				if _, ok := takenHostOffers[hostname]; ok {
					suite.Fail("Host %s is taken multiple times", hostname)
				}
				takenHostOffers[hostname] = offers
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	_, resultCount, _ := suite.pool.ClaimForPlace(filter)
	suite.Equal(len(resultCount), 2)

	// Launch Tasks for successful case.
	offerMap, err := suite.pool.ClaimForLaunch(_testAgent1, false)
	suite.NoError(err)
	suite.Equal(len(offerMap), 10)
	suite.Equal(suite.GetTimedOfferLen(), 40)

	// Launch Task for Expired Offers.
	suite.pool.RemoveExpiredOffers()
	suite.Equal(suite.GetTimedOfferLen(), 30)
	offerMap, err = suite.pool.ClaimForLaunch(_testAgent3, false)
	suite.Nil(offerMap)
	suite.Error(err)

	// Return unused offers for host, it will mark that host from Placing -> Ready.
	suite.pool.ReturnUnusedOffers(_testAgent2)
	offerMap, err = suite.pool.ClaimForLaunch(_testAgent2, false)
	suite.Nil(offerMap)
	suite.Error(err)

	_offerID := "agent-4testOffer-1"
	suite.pool.RescindOffer(&mesos.OfferID{Value: &_offerID})
	offerMap, err = suite.pool.ClaimForLaunch(_testAgent4, false)
	suite.Equal(len(offerMap), 9)
	suite.NoError(err)

	suite.pool.AddOffers(context.Background(), suite.agent3Offers)
	suite.pool.ClaimForPlace(filter)

	// Launch Task on Host, who are set from Placing -> Ready
	hostnames := suite.pool.ResetExpiredHostSummaries(time.Now().Add(2 * time.Hour))
	suite.Equal(len(hostnames), 1)
	offerMap, err = suite.pool.ClaimForLaunch(_testAgent3, false)
	suite.Nil(offerMap)
	suite.Error(err)

	// Launch Task with Reserved Resources.
	offerMap, err = suite.pool.ClaimForLaunch(_testAgent, true)
	suite.NoError(err)
	suite.Equal(len(offerMap), 10)
	suite.Equal(suite.GetTimedOfferLen(), 20)
}

func (suite *OfferPoolTestSuite) TestReservedOffers() {
	offers := suite.createReservedMesosOffers(10, true)

	suite.pool.AddOffers(context.Background(), offers)
	suite.Equal(suite.GetTimedOfferLen(), 10)

	testTable := []struct {
		offerType     summary.OfferType
		expectedCount int
		removeOffer   bool
		msg           string
	}{
		{
			offerType:     summary.Reserved,
			expectedCount: 10,
			removeOffer:   false,
			msg:           "number of reserved offers matches same as added",
		},
		{
			offerType:     summary.Unreserved,
			expectedCount: 0,
			removeOffer:   false,
			msg:           "number of unreserved offers are not present in offerpool",
		},
		{
			offerType:     summary.All,
			expectedCount: 10,
			removeOffer:   false,
			msg:           "number of all offers matches, all reserved offers added",
		},
		{
			offerType:     summary.Reserved,
			expectedCount: 10,
			removeOffer:   true,
			msg:           "number of reserved resources matches, even on removing dummy offer",
		},
	}

	for _, tt := range testTable {
		if tt.removeOffer {
			suite.pool.RemoveReservedOffer(_dummyTestAgent, _dummyOfferID)
		}

		poolOffers, _ := suite.pool.GetOffers(tt.offerType)
		suite.Equal(len(poolOffers[_testAgent]), tt.expectedCount)
	}

	// no-op, as all the offers are reserved.
	suite.pool.ReturnUnusedOffers(_dummyTestAgent)
	suite.pool.ReturnUnusedOffers(_testAgent)
	for _, offer := range offers {
		suite.pool.RemoveReservedOffer(_testAgent, offer.Id.GetValue())
	}
	suite.Equal(suite.GetTimedOfferLen(), 0)
}

func (suite *OfferPoolTestSuite) TestOffersWithUnavailability() {
	// Verify offer pool is empty
	suite.Equal(suite.GetTimedOfferLen(), 0)

	offer1 := suite.agent1Offers[0]
	offer2 := suite.agent1Offers[1]
	offer3 := suite.agent1Offers[2]
	unavailableOffer1 := suite.agent4Offers[4]
	unavailableOffer2 := suite.agent1Offers[4]

	startTime := int64(time.Now().Add(time.Duration(2) * time.Hour).UnixNano())
	unavailableOffer1.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime,
		},
	}

	// Accept the offer, as start time for maintenance is after 4 hours.
	startTime2 := int64(time.Now().Add(time.Duration(4) * time.Hour).UnixNano())
	unavailableOffer2.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime2,
		},
	}

	_frameworkID := "frameworkID"
	var frameworkID *mesos.FrameworkID
	frameworkID = &mesos.FrameworkID{
		Value: &_frameworkID,
	}

	callType := sched.Call_DECLINE
	msg := &sched.Call{
		FrameworkId: frameworkID,
		Type:        &callType,
		Decline: &sched.Call_Decline{
			OfferIds: []*mesos.OfferID{unavailableOffer1.Id},
		},
	}

	gomock.InOrder(
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(frameworkID),
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		suite.schedulerClient.EXPECT().Call(_streamID, msg).Return(nil),
	)

	// the offer with Unavailability shouldn't be considered
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{offer1, offer2, offer3, unavailableOffer1, unavailableOffer2})
	suite.Equal(suite.GetTimedOfferLen(), 4)

	// Clear all offers.
	suite.pool.Clear()
	suite.Equal(suite.GetTimedOfferLen(), 0)
	suite.Equal(len(suite.pool.hostOfferIndex), 0)

	// Add offers back to pool
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{offer1, offer2, offer3})
	suite.Equal(suite.GetTimedOfferLen(), 3)

	// resending an unavailable offer shouldn't break anything
	suite.pool.RescindOffer(unavailableOffer1.Id)
	suite.Equal(suite.GetTimedOfferLen(), 3)
}

func (suite *OfferPoolTestSuite) TestRemoveExpiredOffers() {
	suite.Equal(suite.GetTimedOfferLen(), 0)
	removed, valid := suite.pool.RemoveExpiredOffers()
	suite.Equal(len(removed), 0)
	suite.Equal(0, valid)

	offer1 := suite.agent1Offers[0]
	offer2 := suite.agent2Offers[1]
	offer3 := suite.agent1Offers[2]
	offer4 := suite.agent4Offers[3]

	// pool with offers within timeout
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{offer1, offer2, offer3, offer4})
	removed, valid = suite.pool.RemoveExpiredOffers()
	suite.Empty(removed)
	suite.Equal(4, valid)

	offerID1 := *offer1.Id.Value
	offerID4 := *offer4.Id.Value

	timedOffer1 := &TimedOffer{
		Hostname:   offer1.GetHostname(),
		Expiration: time.Now().Add(-2 * time.Minute),
	}
	timedOffer4 := &TimedOffer{
		Hostname:   offer4.GetHostname(),
		Expiration: time.Now().Add(-2 * time.Minute),
	}

	// adjust the time stamp
	suite.pool.timedOffers.Store(offerID1, timedOffer1)
	suite.pool.timedOffers.Store(offerID4, timedOffer4)

	expected := map[string]*TimedOffer{
		offerID1: timedOffer1,
		offerID4: timedOffer4,
	}

	removed, valid = suite.pool.RemoveExpiredOffers()
	suite.Exactly(expected, removed)
	suite.Equal(2, valid)
}

func (suite *OfferPoolTestSuite) TestAddGetRemoveOffers() {
	// Add offer concurrently
	nOffers := 10
	nAgents := 10
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	for i := 0; i < nOffers; i++ {
		go func(i int) {
			defer wg.Done()
			var offers []*mesos.Offer
			for j := 0; j < nAgents; j++ {
				hostName := fmt.Sprintf("agent-%d", j)
				offerID := fmt.Sprintf("%s-%d", hostName, i)
				offer := getMesosOffer(hostName, offerID)
				offers = append(offers, offer)
			}
			suite.pool.AddOffers(context.Background(), offers)
		}(i)
	}
	wg.Wait()

	suite.Equal(nOffers*nAgents, suite.GetTimedOfferLen())
	for i := 0; i < nOffers; i++ {
		for j := 0; j < nAgents; j++ {
			hostName := fmt.Sprintf("agent-%d", j)
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			value, _ := suite.pool.timedOffers.Load(offerID)
			if hostName == _testAgent2 {
				suite.pool.timedOffers.Store(offerID, &TimedOffer{
					Hostname:   hostName,
					Expiration: time.Now().Add(-2 * time.Minute),
				})
			}
			suite.Equal(value.(*TimedOffer).Hostname, hostName)
		}
	}
	for j := 0; j < nAgents; j++ {
		hostName := fmt.Sprintf("agent-%d", j)
		suite.True(suite.pool.hostOfferIndex[hostName].HasOffer())
	}

	// Get offer for placement
	takenHostOffers := map[string][]*mesos.Offer{}
	mutex := &sync.Mutex{}
	nClients := 5
	var limit uint32 = 2
	wg = sync.WaitGroup{}
	wg.Add(nClients)
	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: limit,
		},
	}
	for i := 0; i < nClients; i++ {
		go func(i int) {
			hostOffers, _, err := suite.pool.ClaimForPlace(filter)
			suite.NoError(err)
			suite.Equal(int(limit), len(hostOffers))
			mutex.Lock()
			defer mutex.Unlock()
			for hostname, offers := range hostOffers {
				suite.Equal(
					nOffers,
					len(offers),
					"hostname %s has incorrect offer length",
					hostname)
				if _, ok := takenHostOffers[hostname]; ok {
					suite.Fail("Host %s is taken multiple times", hostname)
				}
				takenHostOffers[hostname] = offers
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for hostname, offers := range takenHostOffers {
		s, ok := suite.pool.hostOfferIndex[hostname]
		suite.True(ok)
		suite.NotNil(s)

		for _, offer := range offers {
			offerID := offer.GetId().GetValue()
			// Check that all offers are still around.
			value, _ := suite.pool.timedOffers.Load(offerID)
			suite.NotNil(value)
		}
	}

	suite.Equal(nOffers*nAgents, suite.GetTimedOfferLen())
	suite.pool.RefreshGaugeMaps()

	// All the hosts are in PlacingOffer status, ClaimForPlace should return err.
	hostOffers, resultCount, _ := suite.pool.ClaimForPlace(filter)
	suite.Equal(len(hostOffers), 0)
	suite.Equal(resultCount["mismatch_status"], uint32(10))

	// Return unused offers for a host and let other task be placed on that host.
	suite.pool.ReturnUnusedOffers(_testAgent1)
	hostOffers, _, _ = suite.pool.ClaimForPlace(filter)
	suite.Equal(len(hostOffers), 1)

	// Remove Expired Offers,
	_, status := suite.pool.hostOfferIndex[_testAgent2].UnreservedAmount()
	suite.Equal(status, summary.PlacingOffer)
	suite.pool.RemoveExpiredOffers()
	suite.Equal(suite.pool.hostOfferIndex[_testAgent2].HasOffer(), false)

	// Rescind all offers.
	wg = sync.WaitGroup{}
	wg.Add(nOffers)
	for i := 0; i < nOffers; i++ {
		go func(i int) {
			for j := 0; j < nAgents; j++ {
				hostName := fmt.Sprintf("agent-%d", j)
				offerID := fmt.Sprintf("%s-%d", hostName, i)
				rFound := suite.pool.RescindOffer(&mesos.OfferID{Value: &offerID})
				suite.Equal(
					true,
					rFound,
					"Offer %s has inconsistent result when rescinding",
					offerID)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	suite.Equal(suite.GetTimedOfferLen(), 0)
}

func (suite *OfferPoolTestSuite) TestResetExpiredHostSummaries() {
	defer suite.ctrl.Finish()

	type mockHelper struct {
		mockResetExpiredPlacingOfferStatus bool
		hostname                           string
	}

	testTable := []struct {
		helpers                 []mockHelper
		expectedPrunedHostnames []string
		msg                     string
	}{
		{
			helpers:                 []mockHelper{},
			expectedPrunedHostnames: []string{},
			msg: "Pool with no host",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: false,
					hostname: "host0",
				},
			},
			expectedPrunedHostnames: []string{},
			msg: "Pool with 1 host, 0 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: false,
					hostname: "host0",
				},
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname: "host1",
				},
			},
			expectedPrunedHostnames: []string{"host1"},
			msg: "Pool with 2 hosts, 1 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname: "host0",
				},
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname: "host1",
				},
			},
			expectedPrunedHostnames: []string{"host0", "host1"},
			msg: "Pool with 2 hosts, 2 pruned",
		},
	}

	now := time.Now()
	scope := tally.NewTestScope("", map[string]string{})

	for _, tt := range testTable {
		hostOfferIndex := make(map[string]summary.HostSummary)
		for _, helper := range tt.helpers {
			mhs := hostmgr_summary_mocks.NewMockHostSummary(suite.ctrl)
			mhs.EXPECT().ResetExpiredPlacingOfferStatus(now).Return(helper.mockResetExpiredPlacingOfferStatus, scalar.Resources{})
			mhs.EXPECT().HasAnyOffer().Return(true)
			hostOfferIndex[helper.hostname] = mhs
		}
		pool := &offerPool{
			hostOfferIndex: hostOfferIndex,
			metrics:        NewMetrics(scope),
		}
		resetHostnames := pool.ResetExpiredHostSummaries(now)
		suite.Equal(len(tt.expectedPrunedHostnames), len(resetHostnames), tt.msg)
		for _, hostname := range resetHostnames {
			suite.Contains(tt.expectedPrunedHostnames, hostname)
		}
	}
}

func (suite *OfferPoolTestSuite) TestDeclineOffers() {
	// Verify offer pool is empty
	suite.Equal(suite.GetTimedOfferLen(), 0)

	offer1 := suite.agent1Offers[0]
	offer2 := suite.agent2Offers[1]
	offer3 := suite.agent1Offers[2]

	// the offer with Unavailability shouldn't be considered
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{offer1, offer2, offer3})
	suite.Equal(suite.GetTimedOfferLen(), 3)

	_frameworkID := "frameworkID"
	var frameworkID *mesos.FrameworkID
	frameworkID = &mesos.FrameworkID{
		Value: &_frameworkID,
	}

	callType := sched.Call_DECLINE
	msg := &sched.Call{
		FrameworkId: frameworkID,
		Type:        &callType,
		Decline: &sched.Call_Decline{
			OfferIds: []*mesos.OfferID{offer1.Id},
		},
	}

	gomock.InOrder(
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(frameworkID),
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		suite.schedulerClient.EXPECT().Call(_streamID, msg).Return(nil),
	)

	// Decline a valid and non-valid offer.
	suite.pool.DeclineOffers(context.Background(), []*mesos.OfferID{offer1.Id})
	suite.Equal(suite.GetTimedOfferLen(), 2)
}

func TestOfferPoolTestSuite(t *testing.T) {
	suite.Run(t, new(OfferPoolTestSuite))
}
