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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	cqosmocks "github.com/uber/peloton/.gen/qos/v1alpha1/mocks"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	hostmgr_mesos_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/mocks"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	hostmgr_summary_mocks "github.com/uber/peloton/pkg/hostmgr/summary/mocks"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/goleak"
)

const (
	pelotonRole     = "peloton"
	_testAgent      = "agent"
	_testAgent1     = "agent-1"
	_testAgent2     = "agent-2"
	_testAgent3     = "agent-3"
	_testAgent4     = "agent-4"
	_testOfferID    = "testOffer"
	_streamID       = "streamID"
	_dummyOfferID   = "dummyOfferID"
	_dummyTestAgent = "dummyTestAgent"
	_rpcTimeout     = 15 * time.Second
)

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

// Added custom TestReporter to avoid fatalf on the goroutines which makes the
// main goroutine to hang unexpectedly.
//https://github.com/golang/mock/issues/139

type goroutineReporter struct {
	T *testing.T
}

func (r goroutineReporter) Errorf(format string, args ...interface{}) {
	r.T.Errorf(format, args...)
}

func (r goroutineReporter) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type OfferPoolTestSuite struct {
	suite.Suite
	ctx context.Context

	ctrl                 *gomock.Controller
	pool                 *offerPool
	schedulerClient      *mpb_mocks.MockSchedulerClient
	masterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockedCQosClient     *cqosmocks.MockQoSAdvisorServiceYARPCClient
	provider             *hostmgr_mesos_mocks.MockFrameworkInfoProvider
	agent1Offers         []*mesos.Offer
	agent2Offers         []*mesos.Offer
	agent3Offers         []*mesos.Offer
	agent4Offers         []*mesos.Offer
	watchProcessor       *watchmocks.MockWatchProcessor
	cancelFunc           context.CancelFunc
	metric               *metrics.Metrics
}

func (suite *OfferPoolTestSuite) SetupSuite() {
	for i := 1; i <= 4; i++ {
		var offers []*mesos.Offer
		for j := 1; j <= 10; j++ {
			offer := getMesosOffer(
				_testAgent+"-"+strconv.Itoa(i),
				_testAgent+"-"+strconv.Itoa(i)+_testOfferID+"-"+strconv.Itoa(j))
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
	binpacking.Init(nil, nil)
}

func (suite *OfferPoolTestSuite) SetupTest() {
	suite.ctx, suite.cancelFunc = context.WithTimeout(
		context.Background(),
		_rpcTimeout)
	suite.ctrl = gomock.NewController(goroutineReporter{})
	suite.mockedCQosClient = cqosmocks.
		NewMockQoSAdvisorServiceYARPCClient(suite.ctrl)
	suite.schedulerClient = mpb_mocks.NewMockSchedulerClient(suite.ctrl)
	suite.masterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.ctrl)
	suite.provider = hostmgr_mesos_mocks.NewMockFrameworkInfoProvider(suite.ctrl)
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.metric = metrics.NewMetrics(tally.NoopScope)

	suite.pool = &offerPool{
		hostOfferIndex:             make(map[string]summary.HostSummary),
		offerHoldTime:              1 * time.Minute,
		metrics:                    NewMetrics(tally.NoopScope),
		mSchedulerClient:           suite.schedulerClient,
		mesosFrameworkInfoProvider: suite.provider,
		binPackingRanker:           binpacking.GetRankerByName(binpacking.DeFrag),
		watchProcessor:             suite.watchProcessor,
	}
	// reset the ranker state before use
	suite.pool.binPackingRanker.RefreshRanking(suite.ctx, nil)

	suite.pool.timedOffers.Range(func(key interface{}, value interface{}) bool {
		suite.pool.timedOffers.Delete(key)
		return true
	})
}

func (suite *OfferPoolTestSuite) TearDownTest() {
	suite.pool = nil
}

func (suite *OfferPoolTestSuite) TestSlackResourceTypes() {
	NewOfferPool(
		1*time.Minute,
		suite.schedulerClient,
		NewMetrics(tally.NoopScope),
		suite.provider,
		[]string{"GPU", "DUMMY"},
		[]string{common.MesosCPU, "DUMMY"},
		binpacking.GetRankerByName(binpacking.DeFrag),
		time.Duration(30*time.Second),
		suite.watchProcessor,
		nil,
	)
	suite.True(hmutil.IsSlackResourceType(
		common.MesosCPU,
		supportedSlackResourceTypes))
	suite.False(hmutil.IsSlackResourceType(
		common.MesosMem,
		supportedSlackResourceTypes))
}

func (suite *OfferPoolTestSuite) TestClaimForLaunch() {
	// Launching tasks for host, which does not exist in the offer pool
	_, err := suite.pool.ClaimForLaunch(
		_dummyTestAgent,
		"",
		nil)
	suite.Error(err)
	suite.EqualError(err, "cannot find input hostname dummyTestAgent")
	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	// Add unreserved offers, and do ClaimForPlace.
	suite.pool.AddOffers(context.Background(), suite.agent1Offers)
	suite.pool.AddOffers(context.Background(), suite.agent2Offers)
	suite.pool.AddOffers(context.Background(), suite.agent3Offers)
	suite.pool.AddOffers(context.Background(), suite.agent4Offers)
	suite.Equal(suite.GetTimedOfferLen(), 40)

	for i := 1; i <= len(suite.agent3Offers); i++ {
		suite.pool.timedOffers.Store(_testAgent3+_testOfferID+"-"+strconv.Itoa(i),
			&TimedOffer{
				Hostname:   _testAgent3,
				Expiration: time.Now().Add(-2 * time.Minute),
			})
	}

	takenHostOffers := map[string]*summary.Offer{}
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
			hostOffers, _, err := suite.pool.ClaimForPlace(suite.ctx, filter)
			suite.NoError(err)
			suite.Equal(int(limit), len(hostOffers))
			mutex.Lock()
			defer mutex.Unlock()
			for hostname, hostOffer := range hostOffers {
				suite.Equal(
					10,
					len(hostOffer.Offers),
					"hostname %s has incorrect offer length",
					hostname)
				if _, ok := takenHostOffers[hostname]; ok {
					suite.Fail(
						"Host %s is taken multiple times",
						hostname)
				}
				takenHostOffers[hostname] = hostOffer
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	_, resultCount, _ := suite.pool.ClaimForPlace(suite.ctx, filter)
	suite.Equal(len(resultCount), 1)

	// Launch Tasks for successful case.
	var launchableTasks []*hostsvc.LaunchableTask
	taskID1 := "af2e2ff1-1b93-4884-abca-bd3ec3ced972-1-1"
	launchableTasks = append(launchableTasks, &hostsvc.LaunchableTask{
		TaskId: &mesos.TaskID{
			Value: &taskID1,
		},
		Config: &task.TaskConfig{
			Name: "task1",
		},
	})
	offerMap, err := suite.pool.ClaimForLaunch(
		_testAgent1,
		takenHostOffers[_testAgent1].ID,
		launchableTasks,
	)
	suite.NoError(err)
	suite.Equal(len(offerMap), 10)
	suite.Equal(suite.GetTimedOfferLen(), 30)

	// Launch Task for Expired Offers.
	suite.pool.RemoveExpiredOffers()
	suite.Equal(suite.GetTimedOfferLen(), 20)
	offerMap, err = suite.pool.ClaimForLaunch(
		_testAgent3,
		takenHostOffers[_testAgent3].ID,
		launchableTasks)
	suite.Nil(offerMap)
	suite.Error(err)

	// Return unused offers for host, it will mark that host from Placing -> Ready.
	suite.pool.ReturnUnusedOffers(_testAgent2)
	offerMap, err = suite.pool.ClaimForLaunch(
		_testAgent2,
		takenHostOffers[_testAgent2].ID,
		launchableTasks)
	suite.Nil(offerMap)
	suite.Error(err)

	_offerID := "agent-4testOffer-1"
	suite.pool.RescindOffer(&mesos.OfferID{Value: &_offerID})
	offerMap, err = suite.pool.ClaimForLaunch(
		_testAgent4,
		takenHostOffers[_testAgent4].ID,
		launchableTasks)
	suite.Equal(len(offerMap), 9)
	suite.NoError(err)

	suite.pool.AddOffers(context.Background(), suite.agent3Offers)
	suite.pool.ClaimForPlace(suite.ctx, filter)

	// Launch Task on Host, who are set from Placing -> Ready
	hostnames := suite.pool.ResetExpiredPlacingHostSummaries(time.Now().Add(2 * time.Hour))
	suite.Equal(len(hostnames), 1)
	offerMap, err = suite.pool.ClaimForLaunch(
		_testAgent3,
		takenHostOffers[_testAgent3].ID,
		launchableTasks)
	suite.Nil(offerMap)
	suite.Error(err)
}

// TestGetAllOffers tests the GetAllOffers API
func (suite *OfferPoolTestSuite) TestGetOffers() {

	offer1 := suite.agent1Offers[0]

	// pool with offers within timeout
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{
		offer1,
	})

	offers, count := suite.pool.GetAllOffers()
	suite.Equal(1, len(offers))
	suite.Equal(1, count)
}

func (suite *OfferPoolTestSuite) TestOffersWithUnavailability() {
	// Verify offer pool is empty
	suite.Equal(suite.GetTimedOfferLen(), 0)

	offer1 := suite.agent1Offers[0]
	offer2 := suite.agent1Offers[1]
	offer3 := suite.agent1Offers[2]
	unavailableOffer1 := suite.agent4Offers[4]
	unavailableOffer2 := suite.agent1Offers[4]
	unavailableOffer3 := suite.agent2Offers[4]
	unavailableOffer4 := suite.agent3Offers[4]

	// Reject the offer, as start time of maintenance is less than 3 hour from current time.
	startTime := int64(time.Now().Add(time.Duration(2) * time.Hour).UnixNano())
	unavailableOffer1.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime,
		},
	}

	// Accept the offer, as start time for maintenance is after 4 hours of current time.
	startTime2 := int64(time.Now().Add(time.Duration(4) * time.Hour).UnixNano())
	unavailableOffer2.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime2,
		},
	}

	// Reject the offer, as current time is more than start time of maintenance.
	startTime3 := int64(time.Now().Add(time.Duration(-2) * time.Hour).UnixNano())
	unavailableOffer3.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime3,
		},
	}

	// Reject the offer, as current time is same as maintenance start time.
	startTime4 := int64(time.Now().UnixNano())
	unavailableOffer4.Unavailability = &mesos.Unavailability{
		Start: &mesos.TimeInfo{
			Nanoseconds: &startTime4,
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
			OfferIds: []*mesos.OfferID{
				unavailableOffer1.Id,
				unavailableOffer3.Id,
				unavailableOffer4.Id,
			},
		},
	}
	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	gomock.InOrder(
		suite.provider.
			EXPECT().
			GetFrameworkID(context.Background()).Return(frameworkID),
		suite.provider.
			EXPECT().
			GetMesosStreamID(context.Background()).Return(_streamID),
		suite.schedulerClient.EXPECT().Call(_streamID, msg).Return(nil),
	)

	// the offer with Unavailability shouldn't be considered
	suite.pool.AddOffers(
		context.Background(),
		[]*mesos.Offer{
			offer1,
			offer2,
			offer3,
			unavailableOffer1,
			unavailableOffer2,
			unavailableOffer3,
			unavailableOffer4},
	)
	suite.Equal(suite.GetTimedOfferLen(), 4)

	// Clear all offers.
	suite.pool.Clear()
	suite.Equal(suite.GetTimedOfferLen(), 0)
	suite.Equal(len(suite.pool.hostOfferIndex), 0)

	// Add offers back to pool
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{
		offer1,
		offer2,
		offer3,
	})
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
	suite.pool.AddOffers(context.Background(), []*mesos.Offer{
		offer1,
		offer2,
		offer3,
		offer4,
	})
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
	defer goleak.VerifyNoLeaks(suite.T())
	// Add offer concurrently
	nOffers := 10
	nAgents := 10
	wg := sync.WaitGroup{}
	wg.Add(nOffers)
	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()
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
			hostOffers, _, err := suite.pool.ClaimForPlace(suite.ctx, filter)
			suite.NoError(err)
			suite.Equal(int(limit), len(hostOffers))
			mutex.Lock()
			defer mutex.Unlock()
			for hostname, hostOffer := range hostOffers {
				suite.NotNil(hostOffer.ID)
				suite.Equal(
					nOffers,
					len(hostOffer.Offers),
					"hostname %s has incorrect offer length",
					hostname)
				if _, ok := takenHostOffers[hostname]; ok {
					suite.Fail("Host %s is taken multiple times", hostname)
				}
				takenHostOffers[hostname] = hostOffer.Offers
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
	hostOffers, resultCount, _ := suite.pool.ClaimForPlace(suite.ctx, filter)
	suite.Equal(len(hostOffers), 0)
	suite.Equal(resultCount["mismatch_status"], uint32(10))

	// Return unused offers for a host and let other task be placed on that host.
	suite.pool.ReturnUnusedOffers(_testAgent1)
	hostOffers, _, _ = suite.pool.ClaimForPlace(suite.ctx, filter)
	suite.Equal(len(hostOffers), 1)

	// Remove Expired Offers,
	_, _, status := suite.pool.hostOfferIndex[_testAgent2].UnreservedAmount()
	suite.Equal(status, summary.PlacingHost)
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

func (suite *OfferPoolTestSuite) TestResetExpiredPlacingHostSummaries() {
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
			msg:                     "Pool with no host",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: false,
					hostname:                           "host0",
				},
			},
			expectedPrunedHostnames: []string{},
			msg:                     "Pool with 1 host, 0 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: false,
					hostname:                           "host0",
				},
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname:                           "host1",
				},
			},
			expectedPrunedHostnames: []string{"host1"},
			msg:                     "Pool with 2 hosts, 1 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname:                           "host0",
				},
				{
					mockResetExpiredPlacingOfferStatus: true,
					hostname:                           "host1",
				},
			},
			expectedPrunedHostnames: []string{"host0", "host1"},
			msg:                     "Pool with 2 hosts, 2 pruned",
		},
	}

	now := time.Now()
	for _, tt := range testTable {
		hostOfferIndex := make(map[string]summary.HostSummary)
		for _, helper := range tt.helpers {
			mhs := hostmgr_summary_mocks.NewMockHostSummary(suite.ctrl)
			mhs.EXPECT().
				ResetExpiredPlacingOfferStatus(now).
				Return(
					helper.mockResetExpiredPlacingOfferStatus,
					scalar.Resources{},
					nil,
				)
			hostOfferIndex[helper.hostname] = mhs
		}
		pool := &offerPool{
			hostOfferIndex: hostOfferIndex,
			metrics:        NewMetrics(tally.NoopScope),
		}
		resetHostnames := pool.ResetExpiredPlacingHostSummaries(now)
		suite.Equal(len(tt.expectedPrunedHostnames), len(resetHostnames), tt.msg)
		for _, hostname := range resetHostnames {
			suite.Contains(tt.expectedPrunedHostnames, hostname)
		}
	}
}

func (suite *OfferPoolTestSuite) TestResetExpiredHeldHostSummaries() {
	defer suite.ctrl.Finish()

	type mockHelper struct {
		mockResetExpiredHeldOfferStatus bool
		hostname                        string
	}

	testTable := []struct {
		helpers                 []mockHelper
		expectedPrunedHostnames []string
		msg                     string
	}{
		{
			helpers:                 []mockHelper{},
			expectedPrunedHostnames: []string{},
			msg:                     "Pool with no host",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredHeldOfferStatus: false,
					hostname:                        "host0",
				},
			},
			expectedPrunedHostnames: []string{},
			msg:                     "Pool with 1 host, 0 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredHeldOfferStatus: false,
					hostname:                        "host0",
				},
				{
					mockResetExpiredHeldOfferStatus: true,
					hostname:                        "host1",
				},
			},
			expectedPrunedHostnames: []string{"host1"},
			msg:                     "Pool with 2 hosts, 1 pruned",
		}, {
			helpers: []mockHelper{
				{
					mockResetExpiredHeldOfferStatus: true,
					hostname:                        "host0",
				},
				{
					mockResetExpiredHeldOfferStatus: true,
					hostname:                        "host1",
				},
			},
			expectedPrunedHostnames: []string{"host0", "host1"},
			msg:                     "Pool with 2 hosts, 2 pruned",
		},
	}

	now := time.Now()
	for _, tt := range testTable {
		hostOfferIndex := make(map[string]summary.HostSummary)
		for _, helper := range tt.helpers {
			mhs := hostmgr_summary_mocks.NewMockHostSummary(suite.ctrl)
			mhs.EXPECT().
				ResetExpiredHostHeldStatus(now).
				Return(
					helper.mockResetExpiredHeldOfferStatus,
					scalar.Resources{},
					nil,
				)
			hostOfferIndex[helper.hostname] = mhs
		}
		pool := &offerPool{
			hostOfferIndex: hostOfferIndex,
			metrics:        NewMetrics(tally.NoopScope),
		}
		resetHostnames := pool.ResetExpiredHeldHostSummaries(now)
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

func (suite *OfferPoolTestSuite) TestOfferSorting() {
	binpacking.CleanUpRanker()
	binpacking.Init(suite.mockedCQosClient, suite.metric)
	// Verify offer pool is empty
	suite.Equal(suite.GetTimedOfferLen(), 0)

	hostName0 := "hostname0"
	offer0 := suite.createOffer(hostName0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})

	hostName1 := "hostname1"
	offer1 := suite.createOffer(hostName1,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})

	hostName2 := "hostname2"
	offer2 := suite.createOffer(hostName2,
		scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})

	hostName3 := "hostname3"
	offer3 := suite.createOffer(hostName3,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})

	hostName4 := "hostname4"
	offer4 := suite.createOffer(hostName4,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})

	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer2, offer3, offer1, offer0, offer4})

	rankHints := []hostsvc.FilterHint_Ranking{
		hostsvc.FilterHint_FILTER_HINT_RANKING_INVALID,
		hostsvc.FilterHint_FILTER_HINT_RANKING_LEAST_AVAILABLE_FIRST,
		hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM,
		hostsvc.FilterHint_FILTER_HINT_RANKING_LOAD_AWARE,
	}

	for _, rh := range rankHints {
		if rh == hostsvc.FilterHint_FILTER_HINT_RANKING_LOAD_AWARE {
			suite.mockedCQosClient.EXPECT().
				GetHostMetrics(
					gomock.Any(),
					gomock.Any()).Return(
				&cqos.GetHostMetricsResponse{
					Hosts: map[string]*cqos.Metrics{
						"hostname0": {Score: 0},
						"hostname1": {Score: 10},
						"hostname2": {Score: 80},
						"hostname3": {Score: 20},
						"hostname4": {Score: 100},
					}}, nil).Times(3)
		}
		sortedList := suite.pool.getRankedHostSummaryList(
			suite.ctx,
			rh,
			suite.pool.hostOfferIndex,
		)
		if rh == hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM {
			hosts := []string{
				hostName0,
				hostName1,
				hostName2,
				hostName3,
				hostName4}
			for _, s := range sortedList {
				h := s.(summary.HostSummary).GetHostname()
				suite.Contains(hosts, h)
			}
			continue
		}

		if rh == hostsvc.FilterHint_FILTER_HINT_RANKING_LOAD_AWARE {
			suite.Equal(sortedList[0].(summary.HostSummary).GetHostname(),
				"hostname0")
			suite.Equal(sortedList[1].(summary.HostSummary).GetHostname(),
				"hostname1")
			suite.Equal(sortedList[2].(summary.HostSummary).GetHostname(),
				"hostname3")
			suite.Equal(sortedList[3].(summary.HostSummary).GetHostname(),
				"hostname2")
			suite.Equal(sortedList[4].(summary.HostSummary).GetHostname(),
				"hostname4")
			continue
		}
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

}

func (suite *OfferPoolTestSuite) createOffer(
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

func (suite *OfferPoolTestSuite) TestGetHostSummary() {
	_, err := suite.pool.GetHostSummary(_dummyTestAgent)
	suite.Error(err)
	suite.Contains(err.Error(), "does not have any offers")
	suite.pool.AddOffers(context.Background(), suite.agent1Offers)
	_, err = suite.pool.GetHostSummary(_testAgent1)
	suite.NoError(err)
}

func (suite *OfferPoolTestSuite) TestHostToTasksMap() {
	taskID := "693ba665-9e79-4222-a87b-3a5c1d94945b-1-1"
	taskID2 := "693ba665-9e79-4222-a87b-3a5c1d949452-1-1"
	hostname1 := suite.agent1Offers[0].GetHostname()
	suite.pool.AddOffers(context.Background(), suite.agent1Offers)
	suite.pool.AddOffers(context.Background(), suite.agent2Offers)
	suite.pool.AddOffers(context.Background(), suite.agent3Offers)
	suite.pool.AddOffers(context.Background(), suite.agent4Offers)

	// No tasks are running on this host
	hs1, _ := suite.pool.GetHostSummary(hostname1)
	suite.Equal(0, len(hs1.GetTasks()))

	// Remove host to task map on terminal event at recovery
	suite.pool.UpdateTasksOnHost(taskID, task.TaskState_SUCCEEDED, &task.TaskInfo{
		Config: &task.TaskConfig{},
		Runtime: &task.RuntimeInfo{
			Host:  hostname1,
			State: task.TaskState_SUCCEEDED,
		},
	})
	suite.Equal(0, len(hs1.GetTasks()))

	// Populate host to task map with non-terminal event on recovery
	suite.pool.UpdateTasksOnHost(taskID, task.TaskState_RUNNING, &task.TaskInfo{
		Config: &task.TaskConfig{},
		Runtime: &task.RuntimeInfo{
			Host:  hostname1,
			State: task.TaskState_RUNNING,
		},
	})
	suite.Equal(1, len(hs1.GetTasks()))

	suite.pool.UpdateTasksOnHost(taskID2, task.TaskState_RUNNING, &task.TaskInfo{
		Config: &task.TaskConfig{},
		Runtime: &task.RuntimeInfo{
			Host:  hostname1,
			State: task.TaskState_RUNNING,
		},
	})
	suite.Equal(2, len(hs1.GetTasks()))

	// Populate host to task map with non-terminal event
	// with host placement assigned
	suite.pool.UpdateTasksOnHost(taskID, task.TaskState_RUNNING, nil)
	suite.Equal(2, len(hs1.GetTasks()))

	// Populate host to task map with terminal event
	// which deletes from the map.
	suite.pool.UpdateTasksOnHost(taskID, task.TaskState_SUCCEEDED, nil)
	suite.Equal(1, len(hs1.GetTasks()))
}

func (suite *OfferPoolTestSuite) TestGetHostSummaries() {
	// empty offer pool
	hostSummaries, err := suite.pool.GetHostSummaries([]string{})
	suite.NoError(err)
	suite.Equal(0, len(hostSummaries))

	// sample offer pool
	hostname0 := "hostname0"
	offer0 := suite.createOffer(hostname0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	hostname1 := "hostname1"
	offer1 := suite.createOffer(hostname1,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})
	hostname2 := "hostname2"
	offer2 := suite.createOffer(hostname2,
		scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})
	hostname3 := "hostname3"
	offer3 := suite.createOffer(hostname3,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	hostname4 := "hostname4"
	offer4 := suite.createOffer(hostname4,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})
	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer0, offer1, offer2, offer3, offer4})

	hostSummaries, err = suite.pool.GetHostSummaries([]string{})
	suite.NoError(err)
	suite.Equal(5, len(hostSummaries))
	hostSummary0, _ := suite.pool.GetHostSummary("hostname0")
	hostSummary1, _ := suite.pool.GetHostSummary("hostname1")
	hostSummary2, _ := suite.pool.GetHostSummary("hostname2")
	hostSummary3, _ := suite.pool.GetHostSummary("hostname3")
	hostSummary4, _ := suite.pool.GetHostSummary("hostname4")
	suite.Equal(hostSummary0, hostSummaries[hostname0])
	suite.Equal(hostSummary1, hostSummaries[hostname1])
	suite.Equal(hostSummary2, hostSummaries[hostname2])
	suite.Equal(hostSummary3, hostSummaries[hostname3])
	suite.Equal(hostSummary4, hostSummaries[hostname4])

	// filter hostname
	hostSummaries, err = suite.pool.GetHostSummaries([]string{"hostname0", "hostname3"})
	suite.NoError(err)
	suite.Equal(2, len(hostSummaries))
	suite.Equal(hostSummary0, hostSummaries[hostname0])
	suite.Equal(hostSummary3, hostSummaries[hostname3])
}

// TestGetHostHeldForTask tests the happy path of
// get host held for a task
func (suite *OfferPoolTestSuite) TestGetHostHeldForTask() {
	t1 := &peloton.TaskID{Value: "t1"}
	t2 := &peloton.TaskID{Value: "t2"}
	t3 := &peloton.TaskID{Value: "t3"}
	t4 := &peloton.TaskID{Value: "t4"}

	hostname0 := "hostname0"
	offer0 := suite.createOffer(hostname0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	hostname1 := "hostname1"
	offer1 := suite.createOffer(hostname1,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})

	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer0, offer1})

	hs0, err := suite.pool.GetHostSummary(hostname0)
	suite.NoError(err)
	suite.NoError(suite.pool.HoldForTasks(hostname0, []*peloton.TaskID{t1, t3}))

	hs1, err := suite.pool.GetHostSummary(hostname1)
	suite.NoError(err)
	suite.NoError(suite.pool.HoldForTasks(hostname1, []*peloton.TaskID{t2, t4}))

	suite.Equal(suite.pool.GetHostHeldForTask(t1), hs0.GetHostname())
	suite.Equal(suite.pool.GetHostHeldForTask(t2), hs1.GetHostname())
	suite.Equal(suite.pool.GetHostHeldForTask(t3), hs0.GetHostname())
	suite.Equal(suite.pool.GetHostHeldForTask(t4), hs1.GetHostname())

	suite.pool.ReleaseHoldForTasks(hostname0, []*peloton.TaskID{t1})
	suite.pool.ReleaseHoldForTasks(hostname0, []*peloton.TaskID{t2})

	suite.Empty(suite.pool.GetHostHeldForTask(t1))
	suite.Empty(suite.pool.GetHostHeldForTask(t2))
	suite.Equal(suite.pool.GetHostHeldForTask(t3), hs0.GetHostname())
	suite.Equal(suite.pool.GetHostHeldForTask(t4), hs1.GetHostname())
}

// TestGetHostHeldWhenTaskHeldOnMultipleHosts tests the case of
// a task is on multiple hosts. Last write should win
func (suite *OfferPoolTestSuite) TestGetHostHeldWhenTaskHeldOnMultipleHosts() {
	t1 := &peloton.TaskID{Value: "t1"}

	hostname0 := "hostname0"
	offer0 := suite.createOffer(hostname0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	hostname1 := "hostname1"
	offer1 := suite.createOffer(hostname1,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})

	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer0, offer1})

	suite.NoError(suite.pool.HoldForTasks(hostname0, []*peloton.TaskID{t1}))
	suite.NoError(suite.pool.HoldForTasks(hostname1, []*peloton.TaskID{t1}))

	suite.Equal(suite.pool.GetHostHeldForTask(t1), hostname1)
}

// TestClaimForPlaceWithFilterHint tests ClaimForPlace would
// honor filter hint when possible
func (suite *OfferPoolTestSuite) TestClaimForPlaceWithFilterHint() {
	hostname0 := "hostname0"
	offer0 := suite.createOffer(hostname0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})
	hostname1 := "hostname1"
	offer1 := suite.createOffer(hostname1,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})
	hostname2 := "hostname2"
	offer2 := suite.createOffer(hostname2,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 4})

	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer0, offer1, offer2})

	filter := &hostsvc.HostFilter{
		Hint:     &hostsvc.FilterHint{HostHint: []*hostsvc.FilterHint_Host{{Hostname: hostname2}}},
		Quantity: &hostsvc.QuantityControl{MaxHosts: 1},
	}
	result, _, err := suite.pool.ClaimForPlace(suite.ctx, filter)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.NotNil(result[hostname2])
}

// TestClaimForPlaceWithFilterHint tests ClaimForPlace would
// honor rank hint load aware
// hostname0 is the least loaded but not fit the resource constraint
// hostname1 will be picked
func (suite *OfferPoolTestSuite) TestClaimForPlaceWithRankHintLoadAware() {
	binpacking.CleanUpRanker()
	binpacking.Init(suite.mockedCQosClient, suite.metric)
	// Verify offer pool is empty
	suite.Equal(suite.GetTimedOfferLen(), 0)

	hostName0 := "hostname0"
	offer0 := suite.createOffer(hostName0,
		scalar.Resources{CPU: 1, Mem: 1, Disk: 1, GPU: 1})

	hostName1 := "hostname1"
	offer1 := suite.createOffer(hostName1,
		scalar.Resources{CPU: 2, Mem: 1, Disk: 1, GPU: 4})

	hostName2 := "hostname2"
	offer2 := suite.createOffer(hostName2,
		scalar.Resources{CPU: 2, Mem: 2, Disk: 2, GPU: 4})

	hostName3 := "hostname3"
	offer3 := suite.createOffer(hostName3,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})

	hostName4 := "hostname4"
	offer4 := suite.createOffer(hostName4,
		scalar.Resources{CPU: 3, Mem: 3, Disk: 3, GPU: 2})

	suite.pool.AddOffers(context.Background(),
		[]*mesos.Offer{offer2, offer3, offer1, offer0, offer4})

	suite.mockedCQosClient.EXPECT().
		GetHostMetrics(
			gomock.Any(),
			gomock.Any()).Return(
		&cqos.GetHostMetricsResponse{
			Hosts: map[string]*cqos.Metrics{
				"hostname0": {Score: 0},
				"hostname1": {Score: 10},
				"hostname2": {Score: 80},
				"hostname3": {Score: 20},
				"hostname4": {Score: 100},
			}}, nil).Times(3)

	suite.watchProcessor.EXPECT().NotifyEventChange(gomock.Any()).AnyTimes()

	filter := &hostsvc.HostFilter{
		Hint:     &hostsvc.FilterHint{RankHint: hostsvc.FilterHint_FILTER_HINT_RANKING_LOAD_AWARE},
		Quantity: &hostsvc.QuantityControl{MaxHosts: 1},
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit: 2,
			},
		},
	}
	result, _, err := suite.pool.ClaimForPlace(suite.ctx, filter)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.NotNil(result[hostName1])
}

func TestOfferPoolTestSuite(t *testing.T) {
	suite.Run(t, new(OfferPoolTestSuite))
}
