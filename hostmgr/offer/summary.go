package offer

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	mesos "mesos/v1"

	"github.com/uber-go/atomic"
)

// CacheStatus represents status of the offer in offer pool's cache.
type CacheStatus int

const (
	// ReadyOffer represents an offer ready to be used.
	ReadyOffer CacheStatus = iota
	// PlacingOffer represents an offer being used by placement engine.
	PlacingOffer
)

// hostOfferSummary is an internal data struct holding offers on a particular host.
type hostOfferSummary struct {
	// offerID -> offer
	offersOnHost map[string]*mesos.Offer
	mutex        *sync.Mutex
	status       CacheStatus
	readyCount   atomic.Int32
}

// A heuristic about if the hostOfferSummary has any offer.
// TODO(zhitao): Create micro-benchmark to prove this is useful, otherwise remove it!
func (a *hostOfferSummary) hasOffer() bool {
	return a.readyCount.Load() > 0
}

// tryMatch atomically tries to match offers from the current host with given constraint.
// If current hostOfferSummary can satisfy given constraint, the first return value is true and
// all offers used to satisfy given constraint are atomically released from this instance.
// If current instance cannot satisfy given constraint, return value will be (false, empty-slice)
// and no offers are released.
func (a *hostOfferSummary) tryMatch(c *Constraint) (bool, []*mesos.Offer) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if !a.hasOffer() || a.status != ReadyOffer {
		return false, nil
	}

	readyOffers := make(map[string]*mesos.Offer)
	for id, offer := range a.offersOnHost {
		readyOffers[id] = offer
	}

	if c.match(readyOffers) {
		var result []*mesos.Offer
		for _, offer := range readyOffers {
			result = append(result, offer)
		}
		a.status = PlacingOffer
		a.readyCount.Store(0)
		return true, result
	}

	return false, nil
}

func (a *hostOfferSummary) addMesosOffer(offer *mesos.Offer, expiration time.Time) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	offerID := *offer.Id.Value
	a.offersOnHost[offerID] = offer
	if a.status == ReadyOffer {
		a.readyCount.Inc()
	}
}

func (a *hostOfferSummary) removeOffersByStatus(status CacheStatus) map[string]*mesos.Offer {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.status != status {
		return nil
	}

	result := make(map[string]*mesos.Offer)
	for id, offer := range a.offersOnHost {
		result[id] = offer
		delete(a.offersOnHost, id)
	}
	return result
}

func (a *hostOfferSummary) removeMesosOffer(offerID string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	_, ok := a.offersOnHost[offerID]
	if !ok {
		return
	}

	switch a.status {
	case PlacingOffer:
		log.WithField("offer", offerID).Warn("Offer removed while being used for placement, this could trigger INVALID_OFFER error if available resources are reduced further.")
	case ReadyOffer:
		log.WithField("offer", offerID).Debug("Ready offer removed")
		a.readyCount.Dec()
	default:
		log.WithField("status", a.status).Error("Unknown offer status")
	}

	delete(a.offersOnHost, offerID)
}

// casStatus atomically sets the status to new value if current value is old value,
// otherwise returns error.
func (a *hostOfferSummary) casStatus(old, new CacheStatus) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.status != old {
		return errors.New("Invalid status")
	}
	a.status = new
	if a.status == ReadyOffer {
		a.readyCount.Store(int32(len(a.offersOnHost)))
	}
	return nil
}
