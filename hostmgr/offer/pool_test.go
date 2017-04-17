package offer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"peloton/private/hostmgr/hostsvc"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

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
		offers:         make(map[string]*TimedOffer),
		hostOfferIndex: make(map[string]*hostOfferSummary),
		offersLock:     &sync.Mutex{},
		offerHoldTime:  1 * time.Minute,
		metrics:        NewMetrics(scope),
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
	pool.offers[offerID1].Expiration = time.Now().Add(-2 * time.Minute)
	pool.offers[offerID4].Expiration = time.Now().Add(-2 * time.Minute)

	expected := map[string]*TimedOffer{
		offerID1: pool.offers[offerID1],
		offerID4: pool.offers[offerID4],
	}

	result = pool.RemoveExpiredOffers()
	assert.Exactly(t, expected, result)

	assert.Equal(t, 1, len(pool.hostOfferIndex[hostName1].unreservedOffers))
	offer := pool.hostOfferIndex[hostName1].unreservedOffers[offerID3]
	assert.Equal(t, offerID3, *offer.Id.Value)
	assert.Empty(t, len(pool.hostOfferIndex[hostName4].unreservedOffers))
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
		offers:         make(map[string]*TimedOffer),
		hostOfferIndex: make(map[string]*hostOfferSummary),
		offersLock:     &sync.Mutex{},
		metrics:        NewMetrics(scope),
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

	assert.Equal(t, nOffers*nAgents, len(pool.offers))
	for i := 0; i < nOffers; i++ {
		for j := 0; j < nAgents; j++ {
			hostName := fmt.Sprintf("agent-%d", j)
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			assert.Equal(t, *pool.offers[offerID].MesosOffer.Hostname, hostName)
		}
	}
	for j := 0; j < nAgents; j++ {
		hostName := fmt.Sprintf("agent-%d", j)
		assert.Equal(t, ReadyOffer, pool.hostOfferIndex[hostName].status)
		assert.Equal(t, nOffers, len(pool.hostOfferIndex[hostName].unreservedOffers))
		assert.True(t, pool.hostOfferIndex[hostName].hasOffer())
		for i := 0; i < nOffers; i++ {
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			offer := pool.hostOfferIndex[hostName].unreservedOffers[offerID]
			assert.Equal(t, hostName, offer.GetHostname())
		}
	}

	// Get offer for placement
	takenOffers := map[string]*mesos.Offer{}
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
				for _, offer := range offers {
					oid := offer.GetId().GetValue()
					if _, ok := takenOffers[oid]; ok {
						assert.Fail(t, "offer id %s already in takenOffer %v", oid, takenOffers)
					}
					takenOffers[oid] = offer
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, nClients*int(limit)*nOffers, len(takenOffers))
	for offerID, offer := range takenOffers {
		assert.Equal(t, offerID, *offer.Id.Value)
		// Check that all offers are still around.
		// TODO: Check their status
		assert.NotNil(t, pool.offers[offerID])
	}

	for _, summary := range pool.hostOfferIndex {
		assert.NotNil(t, summary)
		for oid := range summary.unreservedOffers {
			if _, ok := takenOffers[oid]; ok {
				// For a taken offer, host status should be PlacingOffer
				// offersTakenFromHost[hostname] = true
				assert.Equal(t, PlacingOffer, summary.status)
			} else {
				assert.Equal(t, ReadyOffer, summary.status)
			}
		}
	}

	assert.Equal(t, nOffers*nAgents, len(pool.offers))

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
	assert.Equal(t, len(pool.offers), 0)

	for hostname, summary := range pool.hostOfferIndex {
		assert.False(t, summary.hasOffer())
		for oid := range summary.unreservedOffers {
			if _, ok := takenOffers[oid]; ok {
				assert.Equal(t, PlacingOffer, summary.status)
			} else {
				assert.Equal(t, ReadyOffer, summary.status)
			}
		}

		assert.Equal(
			t,
			0,
			len(summary.unreservedOffers),
			"Incorrect leftover offer count on hostname %s, leftover: %v",
			hostname,
			summary.unreservedOffers)
	}
}

// TODO: Add test case covering:
// - ready offer pruned;
// - ready offer rescinded;
// - ready offer claimed but never launch within expiration;
// - ready offer claimed then returnd unused;
// - ready offer claimed then launched;
// - launch w/ offer already expired/rescinded/pruned;
// - return offer already expired/rescinded/pruned.
