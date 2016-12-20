package offer

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"testing"
	"time"
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
		offers:                     make(map[string]*Offer),
		offerHoldTime:              1 * time.Minute,
		agentOfferIndex:            make(map[string]*Offer),
		mesosFrameworkInfoProvider: &mockMesosStreamIDProvider{},
	}
	result := pool.RemoveExpiredOffers()
	assert.Equal(t, len(result), 0)

	// pool with offers within timeout
	offer1 := &Offer{Timestamp: time.Now()}
	offer2 := &Offer{Timestamp: time.Now()}
	pool = &offerPool{
		offers: map[string]*Offer{
			"offer1": offer1,
			"offer2": offer2,
		},
		offerHoldTime: 1 * time.Minute,
	}
	result = pool.RemoveExpiredOffers()
	assert.Equal(t, len(result), 0)

	// pool with some offers which time out
	offer1 = &Offer{Timestamp: time.Now()}
	then := time.Now().Add(-2 * time.Minute)
	offerID2 := "offer2"
	agentID1 := "agent1"
	offer2 = &Offer{Timestamp: then, MesosOffer: &mesos.Offer{Id: &mesos.OfferID{Value: &offerID2}, AgentId: &mesos.AgentID{Value: &agentID1}}}
	offer3 := &Offer{Timestamp: time.Now()}
	pool = &offerPool{
		offers: map[string]*Offer{
			"offer1": offer1,
			"offer2": offer2,
			"offer3": offer3,
		},
		offerHoldTime: 1 * time.Minute,
	}
	result = pool.RemoveExpiredOffers()
	assert.Exactly(t, result, map[string]*Offer{"offer2": offer2})
}

func TestOfferConsolidation(t *testing.T) {
	client := mockJSONClient{
		rejectedOfferIds: make(map[string]bool),
	}
	pool := &offerPool{
		offers:        make(map[string]*Offer),
		offerHoldTime: 1 * time.Minute,
		client:        &client,
		mesosFrameworkInfoProvider: &mockMesosStreamIDProvider{},
		agentOfferIndex:            make(map[string]*Offer),
	}
	offerID1 := "offerID1"
	agentID1 := "agent1"

	offerID2 := "offerID2"
	agentID2 := "agent2"

	offerID3 := "offerID3"
	agentID3 := "agent3"

	offerID4 := "offerID4"
	//agentID4 := "agent4"

	offer1 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerID1}, AgentId: &mesos.AgentID{Value: &agentID1}}
	offer2 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerID2}, AgentId: &mesos.AgentID{Value: &agentID2}}
	offer3 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerID3}, AgentId: &mesos.AgentID{Value: &agentID1}}
	offer4 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerID4}, AgentId: &mesos.AgentID{Value: &agentID2}}
	offer5 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerID3}, AgentId: &mesos.AgentID{Value: &agentID3}}

	pool.AddOffers([]*mesos.Offer{offer1, offer2})
	assert.Equal(t, len(pool.agentOfferIndex), 2)
	assert.NotNil(t, pool.agentOfferIndex[agentID1])
	assert.NotNil(t, pool.agentOfferIndex[agentID2])

	// offer1 and offer3 both have same agent id thus both should be rejected
	pool.AddOffers([]*mesos.Offer{offer3})
	assert.Equal(t, len(pool.agentOfferIndex), 1)
	assert.NotNil(t, pool.agentOfferIndex[agentID2])
	assert.Equal(t, len(client.rejectedOfferIds), 2)
	assert.True(t, client.rejectedOfferIds[offerID1])
	assert.True(t, client.rejectedOfferIds[offerID3])

	pool.AddOffers([]*mesos.Offer{offer3, offer1})
	assert.Equal(t, len(pool.agentOfferIndex), 1)
	assert.NotNil(t, pool.agentOfferIndex[agentID2])

	pool.AddOffers([]*mesos.Offer{offer3, offer4, offer5})
	assert.Equal(t, len(pool.agentOfferIndex), 2)
	assert.NotNil(t, pool.agentOfferIndex[agentID1])
	assert.NotNil(t, pool.agentOfferIndex[agentID3])
	assert.True(t, client.rejectedOfferIds[offerID2])

}
