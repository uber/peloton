package offer

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
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
	RemoveExpiredOffers() (map[string]*TimedOffer, int)

	// Clear offers in the pool
	Clear()

	// Decline offers
	DeclineOffers(offerIds []*mesos.OfferID) error

	// ClaimForPlace obtains offers from pool conforming to given constraint
	// for placement purposes. Results are grouped by hostname as key.
	ClaimForPlace(constraint *hostsvc.Constraint) (map[string][]*mesos.Offer, error)

	// ClaimForLaunch finds offers previously for placement on given host.
	// The difference from ClaimForPlace is that offers claimed from this
	// function are considered used and sent back to Mesos master in a Launch
	// operation, while result in `ClaimForPlace` are still considered part
	// of peloton apps.
	ClaimForLaunch(hostname string, useReservedOffers bool) (map[string]*mesos.Offer, error)

	// ReturnUnusedOffers returns previously placed offers on hostname back
	// to current offer pool so they can be used by future launch actions.
	ReturnUnusedOffers(hostname string) error

	// TODO: Add following API for viewing offers, and optionally expose
	//  this in a debugging endpoint.
	// View() (map[string][]*mesos.Offer, err)
}

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(
	offerHoldTime time.Duration,
	schedulerClient mpb.SchedulerClient,
	metrics *Metrics,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	volumeStore storage.PersistentVolumeStore,
) Pool {
	p := &offerPool{
		hostOfferIndex: make(map[string]summary.HostSummary),

		timedOffers:     make(map[string]*TimedOffer),
		timedOffersLock: &sync.Mutex{},
		offerHoldTime:   offerHoldTime,

		mSchedulerClient:           schedulerClient,
		mesosFrameworkInfoProvider: frameworkInfoProvider,

		metrics: metrics,

		volumeStore: volumeStore,
	}

	// Initialize gauges.
	p.metrics.ready.Update(p.readyResources.Get())
	p.metrics.placing.Update(p.placingResources.Get())

	return p
}

// TimedOffer contains hostname and possible expiration time of an offer.
type TimedOffer struct {
	Hostname   string
	Expiration time.Time
}

type offerPool struct {
	sync.RWMutex

	// hostOfferIndex -- key: hostname, value: HostSummary
	hostOfferIndex map[string]summary.HostSummary

	// Map from id to hostname and expiration.
	// Used when offer is rescinded or pruned.
	timedOffers     map[string]*TimedOffer
	timedOffersLock *sync.Mutex

	// Time to hold offer for
	offerHoldTime time.Duration

	mSchedulerClient           mpb.SchedulerClient
	mesosFrameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider

	metrics          *Metrics
	readyResources   scalar.AtomicResources
	placingResources scalar.AtomicResources

	volumeStore storage.PersistentVolumeStore
}

// ClaimForPlace obtains offers from pool conforming to given constraints.
// Results are grouped by hostname as key.
// This implements Pool.ClaimForPlace.
func (p *offerPool) ClaimForPlace(constraint *hostsvc.Constraint) (
	map[string][]*mesos.Offer, error) {

	if constraint == nil {
		return nil, errors.New("empty constraints passed in")
	}

	p.RLock()
	defer p.RUnlock()

	matcher := NewMatcher(
		constraint,
		constraints.NewEvaluator(task.LabelConstraint_HOST))

	for hostname, summary := range p.hostOfferIndex {
		matcher.tryMatch(hostname, summary)
		if matcher.HasEnoughHosts() {
			break
		}
	}

	if !matcher.HasEnoughHosts() {
		// Still proceed to return something.
		log.Warn("Not enough offers are matched to given constraints")
	}

	hostOffers := matcher.getHostOffers()

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
func (p *offerPool) ClaimForLaunch(hostname string, useReservedOffers bool) (
	map[string]*mesos.Offer, error) {
	// TODO: This is very similar to RemoveExpiredOffers, maybe refactor it.
	p.RLock()
	defer p.RUnlock()
	p.timedOffersLock.Lock()
	defer p.timedOffersLock.Unlock()

	var offerMap map[string]*mesos.Offer
	var err error
	if useReservedOffers {
		offerMap, err = p.hostOfferIndex[hostname].ClaimReservedOffersForLaunch()
	} else {
		offerMap, err = p.hostOfferIndex[hostname].ClaimForLaunch()
	}
	if err != nil {
		log.WithFields(log.Fields{
			"host":              hostname,
			"error":             err,
			"useReservedOffers": useReservedOffers,
		}).Error("claim offers for launch error")
		return nil, err
	}

	for id := range offerMap {
		if _, ok := p.timedOffers[id]; ok {
			delete(p.timedOffers, id)
		} else {
			log.WithFields(log.Fields{
				"offer_id": id,
				"host":     hostname,
			}).Warn("Offer id is not in pool")
		}
	}

	if !useReservedOffers {
		delta := scalar.FromOfferMap(offerMap)
		decQuantity(&p.placingResources, delta, p.metrics.placing)
	}

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
	status := p.hostOfferIndex[hostName].AddMesosOffer(offer)

	delta := scalar.FromOffer(offer)
	switch status {
	case summary.ReadyOffer:
		incQuantity(&p.readyResources, delta, p.metrics.ready)
	case summary.PlacingOffer:
		incQuantity(&p.placingResources, delta, p.metrics.placing)
	default:
		log.WithField("status", status).
			Error("Unknown CacheStatus")
	}
	return true
}

// addOffer acquires the write lock. It would guarantee that the hostName
// correspond to the offer is added, then add the offer to the agent.
func (p *offerPool) addOffer(offer *mesos.Offer, expiration time.Time) {
	p.Lock()
	defer p.Unlock()

	hostName := *offer.Hostname
	_, ok := p.hostOfferIndex[hostName]
	if !ok {
		p.hostOfferIndex[hostName] = summary.New(p.volumeStore)
	}
	status := p.hostOfferIndex[hostName].AddMesosOffer(offer)

	delta := scalar.FromOffer(offer)
	switch status {
	case summary.ReadyOffer:
		incQuantity(&p.readyResources, delta, p.metrics.ready)
	case summary.PlacingOffer:
		incQuantity(&p.placingResources, delta, p.metrics.placing)
	default:
		log.WithField("status", status).
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

	p.timedOffersLock.Lock()
	defer p.timedOffersLock.Unlock()
	for _, offer := range offers {
		offerID := *offer.Id.Value
		p.timedOffers[offerID] = &TimedOffer{
			Hostname:   offer.GetHostname(),
			Expiration: expiration,
		}
	}
}

func (p *offerPool) RescindOffer(offerID *mesos.OfferID) bool {
	id := offerID.GetValue()
	log.WithField("offer_id", id).Debug("RescindOffer Received")
	p.RLock()
	defer p.RUnlock()
	p.timedOffersLock.Lock()
	defer p.timedOffersLock.Unlock()

	oID := *offerID.Value
	offer, ok := p.timedOffers[oID]
	if !ok {
		log.WithField("offer_id", offerID).Warn("OfferID not found in pool")
		return false
	}

	delete(p.timedOffers, oID)

	// Remove offer from hostOffers
	hostName := offer.Hostname
	hostOffers, ok := p.hostOfferIndex[hostName]
	if !ok {
		log.WithFields(log.Fields{
			"host":     hostName,
			"offer_id": id,
		}).Warn("host not found in hostOfferIndex")
		return false
	}

	status, removed := hostOffers.RemoveMesosOffer(oID)

	if removed != nil {
		delta := scalar.FromOffer(removed)
		switch status {
		case summary.ReadyOffer:
			decQuantity(&p.readyResources, delta, p.metrics.ready)
		case summary.PlacingOffer:
			decQuantity(&p.placingResources, delta, p.metrics.placing)
		default:
			log.WithField("status", status).
				Error("Unknown CacheStatus")
		}
	}

	return true
}

// RemoveExpiredOffers removes offers which are expired from pool
// and return the list of removed mesos offer ids and their location as well as
// how many valid offers are still left.
func (p *offerPool) RemoveExpiredOffers() (map[string]*TimedOffer, int) {
	p.RLock()
	defer p.RUnlock()
	p.timedOffersLock.Lock()
	defer p.timedOffersLock.Unlock()

	offersToDecline := map[string]*TimedOffer{}

	for offerID, timedOffer := range p.timedOffers {
		if time.Now().After(timedOffer.Expiration) {
			log.WithField("offer_id", offerID).
				Debug("Removing expired offer from pool")
			offersToDecline[offerID] = timedOffer
			delete(p.timedOffers, offerID)
		}
	}

	// Remove the expired offers from hostOfferIndex
	if len(offersToDecline) > 0 {
		for offerID, offer := range offersToDecline {
			hostName := offer.Hostname
			status, removed := p.hostOfferIndex[hostName].RemoveMesosOffer(offerID)
			if removed != nil {
				delta := scalar.FromOffer(removed)
				switch status {
				case summary.ReadyOffer:
					decQuantity(&p.readyResources, delta, p.metrics.ready)
				case summary.PlacingOffer:
					decQuantity(&p.placingResources, delta, p.metrics.placing)
				default:
					log.WithField("status", status).
						Error("Unknown CacheStatus")
				}
			}
		}
	}
	return offersToDecline, len(p.timedOffers)
}

// Clear removes all offers from pool.
func (p *offerPool) Clear() {
	log.Info("Clean up offers")
	p.Lock()
	defer p.Unlock()
	p.timedOffersLock.Lock()
	defer p.timedOffersLock.Unlock()

	p.timedOffers = map[string]*TimedOffer{}
	p.hostOfferIndex = map[string]summary.HostSummary{}
	p.readyResources = scalar.AtomicResources{}
	p.placingResources = scalar.AtomicResources{}
	p.metrics.ready.Update(p.readyResources.Get())
	p.metrics.placing.Update(p.readyResources.Get())
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(offerIDs []*mesos.OfferID) error {
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
	err := p.mSchedulerClient.Call(msid, msg)
	if err != nil {
		// Ideally, we assume that Mesos has offer_timeout configured,
		// so in the event that offer declining call fails, offers
		// should eventually be invalidated by Mesos.
		log.WithError(err).
			WithField("call", msg).
			Warn("Failed to decline offers")
		p.metrics.DeclineFail.Inc(1)
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
		}).Info("Offers returned to pool but not found, maybe pruned?")
		return nil
	}

	err := hostOffers.CasStatus(summary.PlacingOffer, summary.ReadyOffer)
	if err != nil {
		log.WithError(err).
			Error("Incorrect status in ReturnUnusedOffers")
		return err
	}

	log.WithFields(log.Fields{
		"host": hostname,
	}).Info("Returned offers to Ready state.")

	delta := hostOffers.UnreservedAmount()
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
	if !(&curr).Contains(&delta) {
		// NOTE: we still proceed from there, but logs an error which
		// we hope to recover from logging.
		// This could be triggered by either missed offer tracking in
		// pool, or float point precision problem.
		log.WithFields(log.Fields{
			"current": curr,
			"delta":   delta,
		}).Error("Not sufficient resource to subtract delta!")
	}
	tmp := *(curr.Subtract(&delta))
	resources.Set(tmp)
	gaugeMaps.Update(tmp)
}
