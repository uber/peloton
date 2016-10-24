package offer

import (
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"golang.org/x/net/context"

	mesos "mesos/v1"
	"peloton/master/offerpool"
)

// OfferPool caches a set of offers received from Mesos master. It is
// currently only instantiated at the leader of Peloton masters.
type OfferPool interface {
	// Add offers to the pool
	AddOffers([]*mesos.Offer) error

	// Rescind a offer from the pool
	RescindOffer(*mesos.OfferID) error
}

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(d yarpc.Dispatcher) OfferPool {
	pool := &offerPool{
		offers: make(map[string]*mesos.Offer),
	}
	json.Register(d, json.Procedure("OfferPool.GetOffers", pool.GetOffers))

	return pool
}

type offerPool struct {
	sync.Mutex

	// Set of offers received from Mesos master
	offers map[string]*mesos.Offer
}

func (p *offerPool) GetOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *offerpool.GetOffersRequest) (
	*offerpool.GetOffersResponse, yarpc.ResMeta, error) {

	limit := body.Limit
	defer p.Unlock()
	p.Lock()

	count := uint32(0)
	offers := []*mesos.Offer{}
	for id, offer := range p.offers {
		offers = append(offers, offer)
		delete(p.offers, id)
		count++
		if count >= limit {
			break
		}
	}

	log.WithField("offers", offers).Debug("OfferPool: get offers")
	return &offerpool.GetOffersResponse{
		Offers: offers,
	}, nil, nil
}

func (p *offerPool) AddOffers(offers []*mesos.Offer) error {
	defer p.Unlock()
	p.Lock()

	for _, offer := range offers {
		id := *offer.Id.Value
		p.offers[id] = offer
	}

	log.WithField("offers", offers).Debug("OfferPool: added offers")

	// TODO: error handling for offer validation such as duplicate
	// offers for the same host etc.
	return nil
}

func (p *offerPool) RescindOffer(offerId *mesos.OfferID) error {
	defer p.Unlock()
	p.Lock()

	// No-op if offer does not exist
	delete(p.offers, *offerId.Value)

	log.Debugf("OfferPool: rescinded offer %v", *offerId.Value)
	return nil
}
