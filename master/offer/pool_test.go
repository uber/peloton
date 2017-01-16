package offer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
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
	pool := &offerPool{
		offers:         make(map[string]*TimedOffer),
		hostOfferIndex: make(map[string]*hostOfferSummary),
		offersLock:     &sync.Mutex{},
		offerHoldTime:  1 * time.Minute,
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
	assert.Equal(t, len(result), 0)

	//adjust the time stamp
	pool.offers[offerID1].Timestamp = time.Now().Add(-2 * time.Minute)
	pool.offers[offerID4].Timestamp = time.Now().Add(-2 * time.Minute)

	expected := map[string]*TimedOffer{offerID1: pool.offers[offerID1], offerID4: pool.offers[offerID4]}

	result = pool.RemoveExpiredOffers()
	assert.Exactly(t, result, expected)

	assert.Equal(t, len(pool.hostOfferIndex[hostName1].offersOnHost), 1)
	assert.Equal(t, *pool.hostOfferIndex[hostName1].offersOnHost[offerID3].Id.Value, offerID3)

	assert.Equal(t, len(pool.hostOfferIndex[hostName4].offersOnHost), 0)

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
	pool := &offerPool{
		offers:         make(map[string]*TimedOffer),
		hostOfferIndex: make(map[string]*hostOfferSummary),
		offersLock:     &sync.Mutex{},
	}
	// Add offer concurrently
	nOffers := 5
	nAgents := 10
	wg := sync.WaitGroup{}
	for i := 0; i < nOffers; i++ {
		wg.Add(1)
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

	assert.Equal(t, len(pool.offers), nOffers*nAgents)
	for i := 0; i < nOffers; i++ {
		for j := 0; j < nAgents; j++ {
			hostName := fmt.Sprintf("agent-%d", j)
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			assert.Equal(t, *pool.offers[offerID].MesosOffer.Hostname, hostName)
		}
	}
	for j := 0; j < nAgents; j++ {
		hostName := fmt.Sprintf("agent-%d", j)
		assert.Equal(t, len(pool.hostOfferIndex[hostName].offersOnHost), nOffers)
		assert.True(t, pool.hostOfferIndex[hostName].hasOffer())
		for i := 0; i < nOffers; i++ {
			offerID := fmt.Sprintf("%s-%d", hostName, i)
			o := *pool.hostOfferIndex[hostName].offersOnHost[offerID]
			assert.Equal(t, *o.Hostname, hostName)
		}
	}

	// Get offer
	takenOffers := map[string]*mesos.Offer{}
	mutex := &sync.Mutex{}
	nClients := 6
	nBatchSize := 3
	wg = sync.WaitGroup{}
	for i := 0; i < nClients; i++ {
		wg.Add(1)
		go func() {
			offers := pool.getOffers(nBatchSize)
			mutex.Lock()
			defer mutex.Unlock()
			for _, offer := range offers {
				takenOffers[*offer.Id.Value] = offer
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, len(takenOffers), nClients*nBatchSize)
	for offerID, offer := range takenOffers {
		assert.Equal(t, offerID, *offer.Id.Value)
		assert.Nil(t, pool.offers[offerID])
	}
	assert.Equal(t, len(pool.offers), nOffers*nAgents-nClients*nBatchSize)

	//Rescind offer
	wg = sync.WaitGroup{}
	for i := 0; i < nOffers; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < nAgents; j++ {
				hostName := fmt.Sprintf("agent-%d", j)
				offerID := fmt.Sprintf("%s-%d", hostName, i)
				pool.RescindOffer(&mesos.OfferID{Value: &offerID})
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, len(pool.offers), 0)

	for _, agentOffers := range pool.hostOfferIndex {
		assert.False(t, agentOffers.hasOffer())
		assert.Equal(t, len(agentOffers.offersOnHost), 0)
	}

}
