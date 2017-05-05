package offer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"

	"code.uber.internal/infra/peloton/hostmgr/summary"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type mockJSONClient struct {
	rejectedOfferIds map[string]bool
}

func (c *mockJSONClient) Call(mesosStreamID string, msg proto.Message) error {
	call := msg.(*sched.Call)
	for _, id := range call.Decline.OfferIds {
		c.rejectedOfferIds[*id.Value] = true
	}
	return nil
}

type mockMesosStreamIDProvider struct {
}

func (msp *mockMesosStreamIDProvider) GetMesosStreamID() string {
	return "stream"
}

func (msp *mockMesosStreamIDProvider) GetFrameworkID() *mesos.FrameworkID {
	return nil
}

func TestRemoveExpiredOffers(t *testing.T) {
	// empty offer pool
	scope := tally.NewTestScope("", map[string]string{})
	pool := &offerPool{
		timedOffers:     make(map[string]*TimedOffer),
		hostOfferIndex:  make(map[string]summary.HostSummary),
		timedOffersLock: &sync.Mutex{},
		offerHoldTime:   1 * time.Minute,
		metrics:         NewMetrics(scope),
	}
	result := pool.RemoveExpiredOffers()
	assert.Equal(t, len(result), 0)

	hostName1 := "agent1"
	offerID1 := "offer1"
	offer1 := getMesosOffer(hostName1, offerID1)

	hostName2 := "agent2"
	offerID2 := "offer2"
	offer2 := getMesosOffer(hostName2, offerID2)

	offerID3 := "offer3"
	offer3 := getMesosOffer(hostName1, offerID3)

	hostName4 := "agent4"
	offerID4 := "offer4"
	offer4 := getMesosOffer(hostName4, offerID4)

	// pool with offers within timeout
	pool.AddOffers([]*mesos.Offer{offer1, offer2, offer3, offer4})
	result = pool.RemoveExpiredOffers()
	assert.Empty(t, result)

	// adjust the time stamp
	pool.timedOffers[offerID1].Expiration = time.Now().Add(-2 * time.Minute)
	pool.timedOffers[offerID4].Expiration = time.Now().Add(-2 * time.Minute)

	expected := map[string]*TimedOffer{
		offerID1: pool.timedOffers[offerID1],
		offerID4: pool.timedOffers[offerID4],
	}

	result = pool.RemoveExpiredOffers()
	assert.Exactly(t, expected, result)

	/*
		assert.Equal(t, 1, len(pool.hostOfferIndex[hostName1].unreservedOffers))
		offer := pool.hostOfferIndex[hostName1].unreservedOffers[offerID3]
		assert.Equal(t, offerID3, *offer.Id.Value)
		assert.Empty(t, len(pool.hostOfferIndex[hostName4].unreservedOffers))
	*/
}

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

func TestAddGetRemoveOffers(t *testing.T) {
	scope := tally.NewTestScope("", map[string]string{})
	pool := &offerPool{
		timedOffers:     make(map[string]*TimedOffer),
		hostOfferIndex:  make(map[string]summary.HostSummary),
		timedOffersLock: &sync.Mutex{},
		metrics:         NewMetrics(scope),
	}
	// Add offer concurrently
	nOffers := 10
	nAgents := 10
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	for i := 0; i < nOffers; i++ {
		go func(i int) {
			var offers []*mesos.Offer
			for j := 0; j < nAgents; j++ {
				hostName := fmt.Sprintf("agent-%d", j)
				offerID := fmt.Sprintf("%s-%d", hostName, i)
				offer := getMesosOffer(hostName, offerID)
				offers = append(offers, offer)
			}
			pool.AddOffers(offers)
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, nOffers*nAgents, len(pool.timedOffers))
	for i := 0; i < nOffers; i++ {
		for j := 0; j < nAgents; j++ {
			hostName := fmt.Sprintf("agent-%d", j)
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			assert.Equal(t, pool.timedOffers[offerID].Hostname, hostName)
		}
	}
	for j := 0; j < nAgents; j++ {
		hostName := fmt.Sprintf("agent-%d", j)
		assert.True(t, pool.hostOfferIndex[hostName].HasOffer())
	}

	// Get offer for placement
	takenHostOffers := map[string][]*mesos.Offer{}
	mutex := &sync.Mutex{}
	nClients := 4
	var limit uint32 = 2
	wg = sync.WaitGroup{}
	wg.Add(nClients)
	for i := 0; i < nClients; i++ {
		go func(i int) {
			constraint := &hostsvc.Constraint{
				HostLimit: limit,
			}
			hostOffers, err := pool.ClaimForPlace(constraint)
			assert.NoError(t, err)
			assert.Equal(t, int(limit), len(hostOffers))
			mutex.Lock()
			defer mutex.Unlock()
			for hostname, offers := range hostOffers {
				assert.Equal(
					t,
					nOffers,
					len(offers),
					"hostname %s has incorrect offer length",
					hostname)
				if _, ok := takenHostOffers[hostname]; ok {
					assert.Fail(t, "Host %s is taken multiple times", hostname)
				}
				takenHostOffers[hostname] = offers
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for hostname, offers := range takenHostOffers {
		s, ok := pool.hostOfferIndex[hostname]
		assert.True(t, ok)
		assert.NotNil(t, s)

		for _, offer := range offers {
			offerID := offer.GetId().GetValue()
			// Check that all offers are still around.
			assert.NotNil(t, pool.timedOffers[offerID])
		}
	}

	assert.Equal(t, nOffers*nAgents, len(pool.timedOffers))

	// Rescind all offers.
	wg = sync.WaitGroup{}
	wg.Add(nOffers)
	for i := 0; i < nOffers; i++ {
		go func(i int) {
			for j := 0; j < nAgents; j++ {
				hostName := fmt.Sprintf("agent-%d", j)
				offerID := fmt.Sprintf("%s-%d", hostName, i)
				rFound := pool.RescindOffer(&mesos.OfferID{Value: &offerID})
				assert.Equal(
					t,
					true,
					rFound,
					"Offer %s has inconsistent result when rescinding",
					offerID)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, len(pool.timedOffers), 0)
}

// TODO: Add test case covering:
// - ready offer pruned;
// - ready offer rescinded;
// - ready offer claimed but never launch within expiration;
// - ready offer claimed then returnd unused;
// - ready offer claimed then launched;
// - launch w/ offer already expired/rescinded/pruned;
// - return offer already expired/rescinded/pruned.
