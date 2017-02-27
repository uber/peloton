package offer

import (
	"context"
	"errors"
	"sync"
	"time"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	"peloton/private/hostmgr/hostsvc"
	"peloton/private/hostmgr/offerpool"
)

// Pool caches a set of offers received from Mesos master. It is
// currently only instantiated at the leader of Peloton masters.
type Pool interface {
	// Add offers to the pool
	AddOffers([]*mesos.Offer)

	// Rescind a offer from the pool.
	// Returns whether the offer is found in the pool.
	RescindOffer(*mesos.OfferID) bool

	// Remove expired offers from the pool
	RemoveExpiredOffers() map[string]*TimedOffer

	// Clear offers in the pool
	Clear()

	// Decline offers
	DeclineOffers(offers map[string]*TimedOffer) error

	// ClaimForPlace obtains offers from pool conforming to given constraints
	// for placement purposes. Results are grouped by hostname as key.
	ClaimForPlace(contraints []*Constraint) (map[string][]*mesos.Offer, error)

	// ClaimForLaunch finds offers previously for placement on given host.
	// The difference from ClaimForPlace is that offers claimed from this
	// function are consided used and sent back to Mesos master in a Launch or similar
	// operation, while result in `ClaimForPlace` are still considered part of
	// peloton apps.
	ClaimForLaunch(hostname string) (map[string]*mesos.Offer, error)

	// ReturnUnusedOffers returns previously placed offers on hostname back to
	// the current offer pool so they can be used by future launch actions.
	ReturnUnusedOffers(hostname string) error

	// TODO: Add following API for viewingin offers, and optionally expose this in a debugging endpoint.
	// View() (map[string][]*mesos.Offer, err)
}

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(d yarpc.Dispatcher, offerHoldTime time.Duration, client mpb.Client) Pool {
	pool := &offerPool{
		offers:                     make(map[string]*TimedOffer),
		client:                     client,
		hostOfferIndex:             make(map[string]*hostOfferSummary),
		mesosFrameworkInfoProvider: hostmgr_mesos.GetSchedulerDriver(),
		offersLock:                 &sync.Mutex{},
	}
	json.Register(d, json.Procedure("OfferPool.GetOffers", pool.GetOffers))

	return pool
}

// TimedOffer contains details of a Mesos Offer
type TimedOffer struct {
	MesosOffer *mesos.Offer
	Expiration time.Time
}

type offerPool struct {
	sync.RWMutex

	// hostOfferIndex -- key: hostname, value: Offer
	hostOfferIndex map[string]*hostOfferSummary

	// Map from id to offers received from Mesos master.
	// key -- offerID, value: offer.
	// Used when offer is rescinded or pruned.
	offers     map[string]*TimedOffer
	offersLock *sync.Mutex

	// Time to hold offer for
	offerHoldTime              time.Duration
	client                     mpb.Client
	mesosFrameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider
}

func (p *offerPool) GetOffers(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *offerpool.GetOffersRequest) (
	*offerpool.GetOffersResponse, yarpc.ResMeta, error) {

	// this deprecated semantic is equivalent to a single constraint with same limit.
	c := &Constraint{
		hostsvc.Constraint{
			Limit: body.Limit,
		},
	}
	constraints := []*Constraint{c}

	hostOffers, err := p.ClaimForPlace(constraints)
	if err != nil {
		log.WithField("error", err).Error("ClaimForPlace failed")
		return nil, nil, err
	}

	var offers []*mesos.Offer
	for _, ol := range hostOffers {
		offers = append(offers, ol...)
	}

	log.WithField("offers", offers).Debug("ClaimForPlace: claimed offers")
	return &offerpool.GetOffersResponse{
		Offers: offers,
	}, nil, nil
}

// ClaimForPlace obtains offers from pool conforming to given constraints.
// Results are grouped by hostname as key.
// This implements Pool.ClaimForPlace.
func (p *offerPool) ClaimForPlace(constraints []*Constraint) (map[string][]*mesos.Offer, error) {
	if len(constraints) <= 0 {
		return nil, errors.New("Empty constraints passed in!")
	}

	p.RLock()
	defer p.RUnlock()

	matcher := NewConstraintMatcher(constraints)

	for hostname, summary := range p.hostOfferIndex {
		matcher.tryMatch(hostname, summary)
		if matcher.HasEnoughOffers() {
			break
		}
	}

	if !matcher.HasEnoughOffers() {
		// Still proceed to return something.
		log.Warn("Not enough offers are matched to given constraint")
	}

	hostOffers := matcher.claimHostOffers()

	// NOTE: we should not clear the entries for the selected offers in p.offers
	// because we still need to visit corresponding offers, when these offers are returned
	// or used.
	return hostOffers, nil
}

// ClaimForLaunch takes offers from pool for launch.
func (p *offerPool) ClaimForLaunch(hostname string) (
	map[string]*mesos.Offer, error) {
	// TODO: This is very similar to RemoveExpiredOffers, maybe refactor it somehow.
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	offerMap := p.hostOfferIndex[hostname].removeOffersByStatus(PlacingOffer)

	for id := range offerMap {
		if _, ok := p.offers[id]; ok {
			delete(p.offers, id)
		} else {
			log.WithFields(log.Fields{
				"offer_id": id,
				"host":     hostname,
			}).Warn("Offer id is not in pool")
		}
	}

	return offerMap, nil
}

// tryAddOffer acquires read lock of the offerPool. If the offerPool does not
// have the hostName, return false. If yes, add offer with its expiration time to the agent.
func (p *offerPool) tryAddOffer(offer *mesos.Offer, expiration time.Time) bool {
	p.RLock()
	defer p.RUnlock()

	hostName := *offer.Hostname
	if _, ok := p.hostOfferIndex[hostName]; !ok {
		return false
	}
	p.hostOfferIndex[hostName].addMesosOffer(offer, expiration)
	return true
}

// addOffer acquires the write lock. It would guarantee that the hostName correspond to the offer
// is added, then add the offer to the agent.
func (p *offerPool) addOffer(offer *mesos.Offer, expiration time.Time) {
	p.Lock()
	defer p.Unlock()
	hostName := *offer.Hostname
	_, ok := p.hostOfferIndex[hostName]
	if !ok {
		p.hostOfferIndex[hostName] = &hostOfferSummary{
			mutex:        &sync.Mutex{},
			offersOnHost: make(map[string]*mesos.Offer),
		}
	}
	p.hostOfferIndex[hostName].addMesosOffer(offer, expiration)
}

func (p *offerPool) AddOffers(offers []*mesos.Offer) {
	expiration := time.Now().Add(p.offerHoldTime)
	for _, offer := range offers {
		result := p.tryAddOffer(offer, expiration)
		if !result {
			p.addOffer(offer, expiration)
		}
	}

	p.offersLock.Lock()
	defer p.offersLock.Unlock()
	for _, offer := range offers {
		offerID := *offer.Id.Value
		p.offers[offerID] = &TimedOffer{MesosOffer: offer, Expiration: expiration}
	}
}

func (p *offerPool) RescindOffer(offerID *mesos.OfferID) bool {
	id := offerID.GetValue()
	log.WithField("offer_id", id).Debug("RescindOffer Received")
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	oID := *offerID.Value
	offer, ok := p.offers[oID]
	if !ok {
		log.WithField("offer_id", offerID).Warn("OfferID not found in pool")
		return false
	}

	delete(p.offers, oID)

	// Remove offer from hostOffers
	hostName := offer.MesosOffer.GetHostname()
	hostOffers, ok := p.hostOfferIndex[hostName]
	if !ok {
		log.WithFields(log.Fields{
			"host":     hostName,
			"offer_id": id,
		}).Warn("host not found in hostOfferIndex")
		return false
	}

	hostOffers.removeMesosOffer(oID)
	return true
}

// RemoveExpiredOffers removes offers which are expired from pool
// and return the list of removed mesos offer ids plus offer map
func (p *offerPool) RemoveExpiredOffers() map[string]*TimedOffer {
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	offersToDecline := map[string]*TimedOffer{}

	// TODO: fix and revive code path below once T628276 is done
	for offerID, offer := range p.offers {
		if time.Now().After(offer.Expiration) {
			log.WithField("offer_id", offerID).Debug("Removing expired offer from pool")
			// Save offer map so we can put offers back to pool to retry if mesos decline call fails
			offersToDecline[offerID] = offer
			delete(p.offers, offerID)
		}
	}
	// Remove the expired offers from hostOfferIndex
	if len(offersToDecline) > 0 {
		for _, offer := range offersToDecline {
			hostName := *offer.MesosOffer.Hostname
			offerID := *offer.MesosOffer.Id.Value
			p.hostOfferIndex[hostName].removeMesosOffer(offerID)
		}
	}
	return offersToDecline
}

// Clear remove all offers from pool
func (p *offerPool) Clear() {
	log.Info("Clean up offers")
	p.Lock()
	defer p.Unlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	p.offers = map[string]*TimedOffer{}
	p.hostOfferIndex = map[string]*hostOfferSummary{}
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(offers map[string]*TimedOffer) error {
	offerIDs := []*mesos.OfferID{}
	for _, offer := range offers {
		offerIDs = append(offerIDs, offer.MesosOffer.Id)
	}
	log.WithField("offer_ids", offerIDs).Debug("Decline offers")
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
		// TODO: discuss the correct behavior. If mesos master has offer-timeout set (which it definitely should),
		// we can ignore the error.
		log.WithField("error", err).Warn("Failed to decline offers so put offers back to pool")

		defer p.RUnlock()
		p.RLock()
		defer p.offersLock.Lock()
		p.offersLock.Unlock()
		for id, offer := range offers {
			p.offers[id] = offer
			hostName := *offer.MesosOffer.Hostname
			p.hostOfferIndex[hostName].addMesosOffer(offer.MesosOffer, offer.Expiration)
		}
		return err
	}

	return nil
}

// ReturnUnusedOffers returns resources previously sent to placement engine
// back to ready state.
func (p *offerPool) ReturnUnusedOffers(hostname string) error {
	p.RLock()
	defer p.RUnlock()

	hostOffers, ok := p.hostOfferIndex[hostname]
	if !ok {
		log.WithFields(log.Fields{
			"host": hostname,
		}).Info("Offers returned to host pool but not found, maybe already pruned?")
		return nil
	}

	count := hostOffers.casStatus(PlacingOffer, ReadyOffer)
	log.WithFields(log.Fields{
		"host":  hostname,
		"count": count,
	}).Info("Returned offers to Ready state.")
	return nil
}
