package offer

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"testing"
	"time"
)

type mockJsonClient struct {
	rejectedOfferIds map[string]bool
}

func (c *mockJsonClient) Call(mesosStreamId string, msg proto.Message) error {
	call := msg.(*sched.Call)
	for _, id := range call.Decline.OfferIds {
		c.rejectedOfferIds[*id.Value] = true
	}
	return nil
}

type mockMesosStreamIdProvider struct {
}

func (msp *mockMesosStreamIdProvider) GetMesosStreamId() string {
	return "stream"
}

func (msp *mockMesosStreamIdProvider) GetFrameworkId() *mesos.FrameworkID {
	return nil
}

func TestRemoveExpiredOffers(t *testing.T) {
	// empty offer pool
	pool := &offerPool{
		offers:                     make(map[string]*Offer),
		offerHoldTime:              1 * time.Minute,
		agentOfferIndex:            make(map[string]*Offer),
		mesosFrameworkInfoProvider: &mockMesosStreamIdProvider{},
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
	offerId2 := "offer2"
	agentId1 := "agent1"
	offer2 = &Offer{Timestamp: then, MesosOffer: &mesos.Offer{Id: &mesos.OfferID{Value: &offerId2}, AgentId: &mesos.AgentID{Value: &agentId1}}}
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
	client := mockJsonClient{
		rejectedOfferIds: make(map[string]bool),
	}
	pool := &offerPool{
		offers:        make(map[string]*Offer),
		offerHoldTime: 1 * time.Minute,
		client:        &client,
		mesosFrameworkInfoProvider: &mockMesosStreamIdProvider{},
		agentOfferIndex:            make(map[string]*Offer),
	}
	offerId1 := "offerId1"
	agentId1 := "agent1"

	offerId2 := "offerId2"
	agentId2 := "agent2"

	offerId3 := "offerId3"
	agentId3 := "agent3"

	offerId4 := "offerId4"
	//agentId4 := "agent4"

	offer1 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerId1}, AgentId: &mesos.AgentID{Value: &agentId1}}
	offer2 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerId2}, AgentId: &mesos.AgentID{Value: &agentId2}}
	offer3 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerId3}, AgentId: &mesos.AgentID{Value: &agentId1}}
	offer4 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerId4}, AgentId: &mesos.AgentID{Value: &agentId2}}
	offer5 := &mesos.Offer{Id: &mesos.OfferID{Value: &offerId3}, AgentId: &mesos.AgentID{Value: &agentId3}}

	pool.AddOffers([]*mesos.Offer{offer1, offer2})
	assert.Equal(t, len(pool.agentOfferIndex), 2)
	assert.NotNil(t, pool.agentOfferIndex[agentId1])
	assert.NotNil(t, pool.agentOfferIndex[agentId2])

	// offer1 and offer3 both have same agent id thus both should be rejected
	pool.AddOffers([]*mesos.Offer{offer3})
	assert.Equal(t, len(pool.agentOfferIndex), 1)
	assert.NotNil(t, pool.agentOfferIndex[agentId2])
	assert.Equal(t, len(client.rejectedOfferIds), 2)
	assert.True(t, client.rejectedOfferIds[offerId1])
	assert.True(t, client.rejectedOfferIds[offerId3])

	pool.AddOffers([]*mesos.Offer{offer3, offer1})
	assert.Equal(t, len(pool.agentOfferIndex), 1)
	assert.NotNil(t, pool.agentOfferIndex[agentId2])

	pool.AddOffers([]*mesos.Offer{offer3, offer4, offer5})
	assert.Equal(t, len(pool.agentOfferIndex), 2)
	assert.NotNil(t, pool.agentOfferIndex[agentId1])
	assert.NotNil(t, pool.agentOfferIndex[agentId3])
	assert.True(t, client.rejectedOfferIds[offerId2])

}
