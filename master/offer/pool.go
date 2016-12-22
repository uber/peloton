package offer

import (
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"context"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	master_mesos "code.uber.internal/infra/peloton/master/mesos"
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/master/offerpool"
)

// Pool caches a set of offers received from Mesos master. It is
// currently only instantiated at the leader of Peloton masters.
type Pool interface {
	// Add offers to the pool
	AddOffers([]*mesos.Offer) error

	// Rescind a offer from the pool
	RescindOffer(*mesos.OfferID) error

	// Remove expired offers from the pool
	RemoveExpiredOffers() map[string]*Offer

	// Cleanup offers in the pool
	CleanupOffers()

	// Decline offers
	DeclineOffers(offers map[string]*Offer) error
}

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(d yarpc.Dispatcher, offerHoldTime time.Duration, client mpb.Client) Pool {
	pool := &offerPool{
		offers:                     make(map[string]*Offer),
		client:                     client,
		agentOfferIndex:            make(map[string]*Offer),
		mesosFrameworkInfoProvider: master_mesos.GetSchedulerDriver(),
	}
	json.Register(d, json.Procedure("OfferPool.GetOffers", pool.GetOffers))

	return pool
}

// Offer contains details of a Mesos Offer
type Offer struct {
	MesosOffer *mesos.Offer
	Timestamp  time.Time // This is needed for offer pruner, but not included in mesos.Offer
}

type offerPool struct {
	sync.Mutex

	// agentOfferIndex -- key: agentID, value: Offer
	agentOfferIndex map[string]*Offer

	// Set of offers received from Mesos master
	// key -- offerID, value: offer
	offers map[string]*Offer
	// Time to hold offer for
	offerHoldTime              time.Duration
	client                     mpb.Client
	mesosFrameworkInfoProvider master_mesos.FrameworkInfoProvider
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
	for agentID, agentOffer := range p.agentOfferIndex {
		delete(p.agentOfferIndex, agentID)
		offerID := *agentOffer.MesosOffer.Id.Value
		delete(p.offers, offerID)
		offers = append(offers, agentOffer.MesosOffer)
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

	var hostContainsOffersToReject = make(map[string]bool)
	var offersToReject = make(map[string]*Offer)
	for _, offer := range offers {
		offerID := *offer.Id.Value
		agentID := *offer.AgentId.Value
		o := Offer{MesosOffer: offer, Timestamp: time.Now()}
		// if on the host, there is offer that need to be rejected,
		// then reject current offer
		if hostContainsOffersToReject[agentID] {
			offersToReject[offerID] = &o
		} else if existingOffer, ok := p.agentOfferIndex[agentID]; !ok {
			// there is no offer under agentID in agentOfferIndex, add the incoming offer
			p.agentOfferIndex[agentID] = &o
			p.offers[offerID] = &o
		} else {
			// otherwise reject both
			hostContainsOffersToReject[agentID] = true
			offersToReject[offerID] = &o
			existingOfferID := *existingOffer.MesosOffer.Id.Value
			offersToReject[existingOfferID] = existingOffer

			delete(p.agentOfferIndex, agentID)
			delete(p.offers, offerID)
		}
	}
	if len(offersToReject) > 0 {
		err := p.DeclineOffers(offersToReject)
		if err != nil {
			log.Errorf("Failed to reject offers, err=%v", err)
		}
	}
	log.WithField("offers", offers).Debug("OfferPool: added offers")
	// TODO: error handling for offer validation such as duplicate
	// offers for the same host etc.
	return nil
}

func (p *offerPool) RescindOffer(offerID *mesos.OfferID) error {
	defer p.Unlock()
	p.Lock()

	// No-op if offer does not exist
	if offer, ok := p.offers[*offerID.Value]; ok {
		agentID := *offer.MesosOffer.AgentId.Value
		delete(p.agentOfferIndex, agentID)
	}
	delete(p.offers, *offerID.Value)

	log.Debugf("OfferPool: rescinded offer %v", *offerID.Value)
	return nil
}

// RemoveExpiredOffers removes offers which are over offerHoldTime from pool
// and return the list of removed mesos offer ids plus offer map
func (p *offerPool) RemoveExpiredOffers() map[string]*Offer {
	defer p.Unlock()
	p.Lock()

	offersToDecline := map[string]*Offer{}
	// TODO: fix and revive code path below once T628276 is done
	for offerID, offer := range p.offers {
		offerHoldTime := offer.Timestamp.Add(p.offerHoldTime)
		if time.Now().After(offerHoldTime) {
			log.Debugf("Offer %v has expired, removed from offer pool", offerID)
			// Save offer map so we can put offers back to pool to retry if mesos decline call fails
			offersToDecline[offerID] = offer
			delete(p.offers, offerID)
			agentID := *offer.MesosOffer.AgentId.Value
			delete(p.agentOfferIndex, agentID)
		}
	}
	return offersToDecline
}

// CleanupOffers remove all offers from pool
func (p *offerPool) CleanupOffers() {
	defer p.Unlock()
	p.Lock()

	log.Info("Clean up offers")
	p.offers = map[string]*Offer{}
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(offers map[string]*Offer) error {
	offerIDs := []*mesos.OfferID{}
	for _, offer := range offers {
		offerIDs = append(offerIDs, offer.MesosOffer.Id)
	}
	log.Debugf("OfferPool: decline offers %v", offerIDs)
	callType := sched.Call_DECLINE
	msg := &sched.Call{
		FrameworkId: p.mesosFrameworkInfoProvider.GetFrameworkID(),
		Type:        &callType,
		Decline: &sched.Call_Decline{
			OfferIds: offerIDs,
		},
	}
	msid := p.mesosFrameworkInfoProvider.GetMesosStreamID()
	err := p.client.Call(msid, msg)
	if err != nil {
		// Ideally, we assume that Mesos has offer_timeout configured, so in the event that
		// offer declining call fails, offers should eventually be invalidated by Mesos, but
		// just in case there is no offer timeout, here offers are put back into pool for pruner
		// to retry cleanup at the next run
		log.Warnf("Failed to decline offers, put offers back to pool, err=%v", err)

		defer p.Unlock()
		p.Lock()
		for id, offer := range offers {
			p.offers[id] = offer
			agentID := *offer.MesosOffer.AgentId.Value
			p.agentOfferIndex[agentID] = offer
		}
		return err
	}

	return nil
}
