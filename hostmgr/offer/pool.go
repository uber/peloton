package offer

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	"peloton/private/hostmgr/offerpool"
)

// Pool caches a set of offers received from Mesos master. It is
// currently only instantiated at the leader of Peloton masters.
type Pool interface {
	// Add offers to the pool
	AddOffers([]*mesos.Offer)

	// Rescind a offer from the pool
	RescindOffer(*mesos.OfferID)

	// Remove expired offers from the pool
	RemoveExpiredOffers() map[string]*TimedOffer

	// Cleanup offers in the pool
	CleanupOffers()

	// Decline offers
	DeclineOffers(offers map[string]*TimedOffer) error

	// GetHostOffers obtains offers from pool, grouped by hostname as key.
	GetHostOffers(offerFilter *HostOfferFilter) map[string][]*mesos.Offer
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

// HostOfferFilter describes offers from which host should be returned in a GetOffers call.
type HostOfferFilter struct {
	// HostLimit is the maximum number of hosts to return. Any non-positive number means unlimited.
	HostLimit uint32
}

// TimedOffer contains details of a Mesos Offer
type TimedOffer struct {
	MesosOffer *mesos.Offer
	Timestamp  time.Time // This is needed for offer pruner, but not included in mesos.Offer
}

type hostOfferSummary struct {
	// offerID -> offer
	offersOnHost map[string]*mesos.Offer
	mutex        *sync.Mutex
	hasOfferFlag *int32
}

// A heuristic about if the agentResourceSummary has any offer.
func (a *hostOfferSummary) hasOffer() bool {
	return atomic.LoadInt32(a.hasOfferFlag) == int32(1)
}

// Returns all of offers
func (a *hostOfferSummary) getAllOffers() []*mesos.Offer {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	var result []*mesos.Offer
	for id, offer := range a.offersOnHost {
		result = append(result, offer)
		delete(a.offersOnHost, id)
	}
	atomic.StoreInt32(a.hasOfferFlag, 0)
	return result
}

func (a *hostOfferSummary) addMesosOffer(offer *mesos.Offer) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	offerID := *offer.Id.Value
	a.offersOnHost[offerID] = offer
	atomic.StoreInt32(a.hasOfferFlag, 1)
}

func (a *hostOfferSummary) removeMesosOffer(offerID string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.offersOnHost, offerID)
	if len(a.offersOnHost) == 0 {
		atomic.StoreInt32(a.hasOfferFlag, 0)
	}
}

type offerPool struct {
	sync.RWMutex

	// hostOfferIndex -- key: hostname, value: Offer
	hostOfferIndex map[string]*hostOfferSummary

	// Set of offers received from Mesos master
	// key -- offerID, value: offer. Used when offer is rescinded or pruned
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

	filter := HostOfferFilter{
		HostLimit: body.Limit,
	}

	hostOffers := p.GetHostOffers(&filter)

	var offers []*mesos.Offer
	for _, ol := range hostOffers {
		offers = append(offers, ol...)
	}

	log.WithField("offers", offers).Debug("OfferPool: get offers")
	return &offerpool.GetOffersResponse{
		Offers: offers,
	}, nil, nil
}

// GetHostOffers returns Mesos offers according to input filter.
func (p *offerPool) GetHostOffers(offerFilter *HostOfferFilter) map[string][]*mesos.Offer {
	defer p.RUnlock()
	p.RLock()

	var hostLimit uint32 = math.MaxUint32
	if offerFilter.HostLimit > 0 {
		hostLimit = offerFilter.HostLimit
	}

	var numHosts uint32

	hostOffers := make(map[string][]*mesos.Offer)

	for hostname, summary := range p.hostOfferIndex {
		if summary.hasOffer() {
			offersGot := summary.getAllOffers()
			if len(offersGot) > 0 {
				hostOffers[hostname] = offersGot
				numHosts++
				if numHosts >= hostLimit {
					break
				}
			}
		}
	}

	// Clear the entries for the selected offers in p.offers
	if len(hostOffers) > 0 {
		p.offersLock.Lock()
		defer p.offersLock.Unlock()
		for _, offers := range hostOffers {
			for _, o := range offers {
				delete(p.offers, *o.Id.Value)
			}
		}
	}
	return hostOffers
}

// tryAddOffer acquires read lock of the offerPool. If the offerPool does not
// have the hostName, return false. If yes, add offer to the agent.
func (p *offerPool) tryAddOffer(offer *mesos.Offer) bool {
	defer p.RUnlock()
	p.RLock()

	hostName := *offer.Hostname
	hostOffers, ok := p.hostOfferIndex[hostName]
	if !ok {
		return false
	}
	hostOffers.addMesosOffer(offer)
	return true
}

// addOffer acquires the write lock. It would guarantee that the hostName correspond to the offer
// is added, then add the offer to the agent.
func (p *offerPool) addOffer(offer *mesos.Offer) {
	defer p.Unlock()
	p.Lock()
	hostName := *offer.Hostname
	_, ok := p.hostOfferIndex[hostName]
	if !ok {
		flag := int32(1)
		p.hostOfferIndex[hostName] = &hostOfferSummary{
			mutex:        &sync.Mutex{},
			hasOfferFlag: &flag,
			offersOnHost: make(map[string]*mesos.Offer),
		}
	}
	p.hostOfferIndex[hostName].addMesosOffer(offer)
}

func (p *offerPool) AddOffers(offers []*mesos.Offer) {
	for _, offer := range offers {
		result := p.tryAddOffer(offer)
		if !result {
			p.addOffer(offer)
		}
	}

	p.offersLock.Lock()
	defer p.offersLock.Unlock()
	for _, offer := range offers {
		offerID := *offer.Id.Value
		p.offers[offerID] = &TimedOffer{MesosOffer: offer, Timestamp: time.Now()}
	}
}

func (p *offerPool) RescindOffer(offerID *mesos.OfferID) {
	id := offerID.GetValue()
	log.WithField("offer_id", id).Debug("RescindOffer Received")
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	var hostName string
	oID := *offerID.Value
	offer, ok := p.offers[oID]
	if ok {
		hostName = *offer.MesosOffer.Hostname
		delete(p.offers, oID)
	} else {
		log.WithField("offer_id", offerID).Warn("OfferID not found in pool")
		return
	}

	// Remove offer from hosttOffers
	hosttOffers, ok := p.hostOfferIndex[hostName]
	if ok {
		hosttOffers.removeMesosOffer(oID)
	} else {
		log.WithFields(log.Fields{
			"host":     hostName,
			"offer_id": id,
		}).Warn("host not found in hostOfferIndex")
	}
}

// RemoveExpiredOffers removes offers which are over offerHoldTime from pool
// and return the list of removed mesos offer ids plus offer map
func (p *offerPool) RemoveExpiredOffers() map[string]*TimedOffer {
	p.RLock()
	defer p.RUnlock()
	p.offersLock.Lock()
	defer p.offersLock.Unlock()

	offersToDecline := map[string]*TimedOffer{}
	// TODO: fix and revive code path below once T628276 is done
	for offerID, offer := range p.offers {
		offerHoldTime := offer.Timestamp.Add(p.offerHoldTime)
		if time.Now().After(offerHoldTime) {
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

// CleanupOffers remove all offers from pool
func (p *offerPool) CleanupOffers() {
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
			p.hostOfferIndex[hostName].addMesosOffer(offer.MesosOffer)
		}
		return err
	}

	return nil
}
