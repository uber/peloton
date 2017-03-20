package offer

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/atomic"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
)

// CacheStatus represents status of the offer in offer pool's cache.
type CacheStatus int

const (
	// ReadyOffer represents an offer ready to be used.
	ReadyOffer CacheStatus = iota + 1
	// PlacingOffer represents an offer being used by placement engine.
	PlacingOffer
)

// hostOfferSummary is an internal data struct holding offers on a particular host.
type hostOfferSummary struct {
	sync.Mutex

	// offerID -> offer
	offersOnHost map[string]*mesos.Offer
	readyCount   atomic.Int32

	// quantity of scalar resources.
	quantity scalar.Resources
	status   CacheStatus
}

// A heuristic about if the hostOfferSummary has any offer.
// TODO(zhitao): Create micro-benchmark to prove this is useful,
// otherwise remove it!
func (a *hostOfferSummary) hasOffer() bool {
	return a.readyCount.Load() > 0
}

// tryMatch atomically tries to match offers from the current host with given
// constraint.
// If current hostOfferSummary can satisfy given constraint, the first return
// value is true and all offers used to satisfy given constraint are atomically
// released from this instance.
// If current instance cannot satisfy given constraint, return value will be
// (false, empty-slice) and no offers are released.
func (a *hostOfferSummary) tryMatch(c *Constraint) (bool, []*mesos.Offer) {
	a.Lock()
	defer a.Unlock()

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

func (a *hostOfferSummary) addMesosOffer(
	offer *mesos.Offer, expiration time.Time) {

	a.Lock()
	defer a.Unlock()
	offerID := *offer.Id.Value
	a.offersOnHost[offerID] = offer
	if a.status == ReadyOffer {
		a.readyCount.Inc()
	}
}

func (a *hostOfferSummary) claimForLaunch() (map[string]*mesos.Offer, error) {
	a.Lock()
	defer a.Unlock()
	if a.status != PlacingOffer {
		return nil, errors.New("Host status is not Placing")
	}

	result := make(map[string]*mesos.Offer)
	for id, offer := range a.offersOnHost {
		result[id] = offer
		delete(a.offersOnHost, id)
	}

	// Reseting status to ready so any future offer on the host is considered
	// as ready.
	a.status = ReadyOffer
	a.readyCount.Store(0)
	return result, nil
}

func (a *hostOfferSummary) removeMesosOffer(offerID string) {
	a.Lock()
	defer a.Unlock()

	_, ok := a.offersOnHost[offerID]
	if !ok {
		return
	}

	switch a.status {
	case PlacingOffer:
		log.WithField("offer", offerID).
			Warn("Offer removed while being used for placement, this could trigger " +
				"INVALID_OFFER error if available resources are reduced further.")
	case ReadyOffer:
		log.WithField("offer", offerID).Debug("Ready offer removed")
		a.readyCount.Dec()
	default:
		log.WithField("status", a.status).Error("Unknown offer status")
	}

	delete(a.offersOnHost, offerID)
}

// casStatus atomically sets the status to new value if current value is old,
// otherwise returns error.
func (a *hostOfferSummary) casStatus(old, new CacheStatus) error {
	a.Lock()
	defer a.Unlock()
	if a.status != old {
		return errors.New("Invalid status")
	}
	a.status = new
	if a.status == ReadyOffer {
		a.readyCount.Store(int32(len(a.offersOnHost)))
	}
	return nil
}
