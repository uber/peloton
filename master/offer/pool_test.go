package offer

import (
	"github.com/stretchr/testify/assert"
	mesos "mesos/v1"
	"testing"
	"time"
)

func TestRemoveExpiredOffers(t *testing.T) {
	// empty offer pool
	pool := &offerPool{
		offers:        make(map[string]*Offer),
		offerHoldTime: 1 * time.Minute,
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
	offer2 = &Offer{Timestamp: then, MesosOffer: &mesos.Offer{Id: &mesos.OfferID{Value: &offerId2}}}
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
