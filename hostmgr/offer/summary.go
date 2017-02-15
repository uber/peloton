package offer

import (
	"sync"
	"sync/atomic"

	mesos "mesos/v1"
)

// hostOfferSummary is an internal data struct holding offers on a particular host.
type hostOfferSummary struct {
	// offerID -> offer
	offersOnHost map[string]*mesos.Offer
	mutex        *sync.Mutex
	hasOfferFlag *int32
}

// A heuristic about if the agentResourceSummary has any offer.
// TODO(zhitao): Create micro-benchmark to prove this is useful, otherwise remove it!
func (a *hostOfferSummary) hasOffer() bool {
	return atomic.LoadInt32(a.hasOfferFlag) == int32(1)
}

// tryMatch atomically tries to match offers from the current host with given constraint.
// If current hostOfferSummary can satisfy given constraint, the first return value is true and
// all offers used to satisfy given constraint are atomically released from this instance.
// If current instance cannot satisfy given constraint, return value will be (false, empty-slice)
// and no offers are released.
func (a *hostOfferSummary) tryMatch(c *Constraint) (bool, []*mesos.Offer) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if !a.hasOffer() {
		return false, nil
	}

	// NOTE: We take all offers from a host to satisify a constraint right now, but it is
	// possible to perform this at a smaller granularity.
	if c.match(a.offersOnHost) {
		var result []*mesos.Offer
		for id, offer := range a.offersOnHost {
			result = append(result, offer)
			delete(a.offersOnHost, id)
		}
		atomic.StoreInt32(a.hasOfferFlag, 0)
		return true, result
	}

	return false, nil
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
