package offer

import (
	"errors"
	"sync"
	"time"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
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
	DeclineOffers(offers map[string]*mesos.Offer) error

	// ClaimForPlace obtains offers from pool conforming to given constraints
	// for placement purposes. Results are grouped by hostname as key.
	ClaimForPlace(contraints []*Constraint) (map[string][]*mesos.Offer, error)

	// ClaimForLaunch finds offers previously for placement on given host.
	// The difference from ClaimForPlace is that offers claimed from this
	// function are consided used and sent back to Mesos master in a Launch
	// operation, while result in `ClaimForPlace` are still considered part of
	// peloton apps.
	ClaimForLaunch(hostname string) (map[string]*mesos.Offer, error)

	// ReturnUnusedOffers returns previously placed offers on hostname back to
	// the current offer pool so they can be used by future launch actions.
	ReturnUnusedOffers(hostname string) error

	// TODO: Add following API for viewing offers, and optionally expose this in
	// a debugging endpoint.
	// View() (map[string][]*mesos.Offer, err)
}

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(
	offerHoldTime time.Duration,
	client mpb.Client,
	metrics *Metrics,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
) Pool {
	p := &offerPool{
		hostOfferIndex: make(map[string]*hostOfferSummary),

		offers:        make(map[string]*TimedOffer),
		offersLock:    &sync.Mutex{},
		offerHoldTime: offerHoldTime,

		client: client,
		mesosFrameworkInfoProvider: frameworkInfoProvider,

		metrics: metrics,
	}

	// Initialize gauges.
	p.metrics.ready.Update(p.readyResources.Get())
	p.metrics.placing.Update(p.placingResources.Get())

	return p
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
	offerHoldTime time.Duration

	client                     mpb.Client
	mesosFrameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider

	metrics          *Metrics
	readyResources   scalar.AtomicResources
	placingResources scalar.AtomicResources
}

// ClaimForPlace obtains offers from pool conforming to given constraints.
// Results are grouped by hostname as key.
// This implements Pool.ClaimForPlace.
func (p *offerPool) ClaimForPlace(constraints []*Constraint) (
	map[string][]*mesos.Offer, error) {

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

	var delta scalar.Resources
	for _, offers := range hostOffers {
		for _, offer := range offers {
			tmp := scalar.FromOffer(offer)
			delta = *(delta.Add(&tmp))
		}
	}
	incQuantity(&p.placingResources, delta, p.metrics.placing)
	decQuantity(&p.readyResources, delta, p.metrics.ready)

	// NOTE: we should not clear the entries for the selected offers in p.offers
	// because we still need to visit corresponding offers, when these offers
	// are returned or used.
	return hostOffers, nil
}

// ClaimForLaunch takes offers from pool for launch.
func (p *offerPool) ClaimForLaunch(hostname string) (
	map[string]*mesos.Offer, error) {
	// TODO: This is very similar to RemoveExpiredOffers, maybe refactor it.
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	offerMap, err := p.hostOfferIndex[hostname].claimForLaunch()
	if err != nil {
		log.WithFields(log.Fields{
			"host":  hostname,
			"error": err,
		}).Error("ClaimForLaunch error")
		return nil, err
	}

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

	delta := scalar.FromOfferMap(offerMap)
	decQuantity(&p.placingResources, delta, p.metrics.placing)

	return offerMap, nil
}

// tryAddOffer acquires read lock of the offerPool.
// If the offerPool does not have the hostName, returns false; otherwise,
// add offer with its expiration time to the agent and returns true.
func (p *offerPool) tryAddOffer(offer *mesos.Offer, expiration time.Time) bool {
	p.RLock()
	defer p.RUnlock()

	hostName := *offer.Hostname
	if _, ok := p.hostOfferIndex[hostName]; !ok {
		return false
	}
	p.hostOfferIndex[hostName].addMesosOffer(offer, expiration)

	delta := scalar.FromOffer(offer)
	switch p.hostOfferIndex[hostName].status {
	case ReadyOffer:
		incQuantity(&p.readyResources, delta, p.metrics.ready)
	case PlacingOffer:
		incQuantity(&p.placingResources, delta, p.metrics.placing)
	default:
		log.WithField("status", p.hostOfferIndex[hostName].status).
			Error("Unknown CacheStatus")
	}
	return true
}

// addOffer acquires the write lock. It would guarantee that the hostName
//  correspond to the offer is added, then add the offer to the agent.
func (p *offerPool) addOffer(offer *mesos.Offer, expiration time.Time) {
	p.Lock()
	defer p.Unlock()
	hostName := *offer.Hostname
	_, ok := p.hostOfferIndex[hostName]
	if !ok {
		p.hostOfferIndex[hostName] = &hostOfferSummary{
			offersOnHost: make(map[string]*mesos.Offer),
			status:       ReadyOffer,
		}
	}
	p.hostOfferIndex[hostName].addMesosOffer(offer, expiration)

	delta := scalar.FromOffer(offer)
	switch p.hostOfferIndex[hostName].status {
	case ReadyOffer:
		incQuantity(&p.readyResources, delta, p.metrics.ready)
	case PlacingOffer:
		incQuantity(&p.placingResources, delta, p.metrics.placing)
	default:
		log.WithField("status", p.hostOfferIndex[hostName].status).
			Error("Unknown CacheStatus")
	}
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

	delta := scalar.FromOffer(offer.MesosOffer)
	switch p.hostOfferIndex[hostName].status {
	case ReadyOffer:
		decQuantity(&p.readyResources, delta, p.metrics.ready)
	case PlacingOffer:
		decQuantity(&p.placingResources, delta, p.metrics.placing)
	default:
		log.WithField("status", p.hostOfferIndex[hostName].status).
			Error("Unknown CacheStatus")
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

	for offerID, offer := range p.offers {
		if time.Now().After(offer.Expiration) {
			log.WithField("offer_id", offerID).
				Debug("Removing expired offer from pool")
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

			delta := scalar.FromOffer(offer.MesosOffer)
			switch p.hostOfferIndex[hostName].status {
			case ReadyOffer:
				decQuantity(&p.readyResources, delta, p.metrics.ready)
			case PlacingOffer:
				decQuantity(&p.placingResources, delta, p.metrics.placing)
			default:
				log.WithField("status", p.hostOfferIndex[hostName].status).
					Error("Unknown CacheStatus")
			}
		}
	}
	return offersToDecline
}

// Clear removes all offers from pool.
func (p *offerPool) Clear() {
	log.Info("Clean up offers")
	p.Lock()
	defer p.Unlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	p.offers = map[string]*TimedOffer{}
	p.hostOfferIndex = map[string]*hostOfferSummary{}
	p.readyResources = scalar.AtomicResources{}
	p.placingResources = scalar.AtomicResources{}
	p.metrics.ready.Update(p.readyResources.Get())
	p.metrics.placing.Update(p.readyResources.Get())
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(offers map[string]*mesos.Offer) error {
	offerIDs := []*mesos.OfferID{}
	for _, offer := range offers {
		offerIDs = append(offerIDs, offer.Id)
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
		// Ideally, we assume that Mesos has offer_timeout configured, so in the
		//  event that offer declining call fails, offers should eventually be
		// invalidated by Mesos, but just in case there is no offer timeout,
		// offers are put back into pool here for pruner to retry at the next run.
		// TODO: discuss the correct behavior. If mesos master has offer-timeout set
		// (which is possible to know in a GET_FLAGS call),
		// we can ignore the error.
		log.WithError(err).
			WithField("call", msg).
			Warn("Failed to decline offers so put offers back to pool")

		expiration := time.Now().Add(p.offerHoldTime)
		defer p.RUnlock()
		p.RLock()
		defer p.offersLock.Lock()
		p.offersLock.Unlock()
		for id, offer := range offers {
			p.offers[id] = &TimedOffer{
				MesosOffer: offer,
				Expiration: expiration,
			}
			hostName := *offer.Hostname
			p.hostOfferIndex[hostName].addMesosOffer(offer, expiration)
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

	delta := scalar.FromOfferMap(hostOffers.offersOnHost)
	decQuantity(&p.placingResources, delta, p.metrics.placing)
	incQuantity(&p.readyResources, delta, p.metrics.ready)

	return nil
}

func incQuantity(
	resources *scalar.AtomicResources,
	delta scalar.Resources,
	gaugeMaps scalar.GaugeMaps,
) {
	tmp := resources.Get()
	tmp = *(tmp.Add(&delta))
	resources.Set(tmp)
	gaugeMaps.Update(tmp)
}

func decQuantity(
	resources *scalar.AtomicResources,
	delta scalar.Resources,
	gaugeMaps scalar.GaugeMaps,
) {
	curr := resources.Get()
	tmp := curr.TrySubtract(&delta)
	if tmp == nil {
		log.WithFields(log.Fields{
			"resources": resources,
			"delta":     delta,
		}).Error("No enough resources to subtract delta")
	}
	resources.Set(*tmp)
	gaugeMaps.Update(*tmp)
}
