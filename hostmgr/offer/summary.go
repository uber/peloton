package offer

import (
	"errors"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/reservation"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"
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

	// offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
	status           CacheStatus
	readyCount       atomic.Int32

	// offerID -> reserved offer
	reservedOffers map[string]*mesos.Offer
	// reservedResources has reservationLabelID -> resources.
	reservedResources map[string]*reservation.ReservedResources
}

// A heuristic about if the hostOfferSummary has any offer.
// TODO(zhitao): Create micro-benchmark to prove this is useful,
// otherwise remove it!
func (a *hostOfferSummary) hasOffer() bool {
	return a.readyCount.Load() > 0
}

// matchConstraint determines whether given constraint matches
// the given map of offers.
func matchConstraint(
	offerMap map[string]*mesos.Offer,
	c *hostsvc.Constraint,
	evaluator constraints.Evaluator,
) MatchResult {

	if len(offerMap) == 0 {
		return MismatchStatus
	}

	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		scalarRes := scalar.FromOfferMap(offerMap)
		scalarMin := scalar.FromResourceConfig(min)
		if !scalarRes.Contains(&scalarMin) {
			return InsufficientResources
		}

		// Special handling for GPU: GPU hosts are only for GPU tasks.
		if scalarRes.HasGPU() != scalarMin.HasGPU() {
			return MismatchGPU
		}
	}

	// Match ports resources.
	numPorts := c.GetResourceConstraint().GetNumPorts()
	if numPorts > util.GetPortsNumFromOfferMap(offerMap) {
		return InsufficientResources
	}

	// Only try to get first offer in this host because all the offers have
	// the same host attributes.
	var firstOffer *mesos.Offer
	for _, offer := range offerMap {
		firstOffer = offer
		break
	}

	if hc := c.GetSchedulingConstraint(); hc != nil {
		hostname := firstOffer.GetHostname()
		lv := constraints.GetHostLabelValues(
			hostname,
			firstOffer.GetAttributes(),
		)
		result, err := evaluator.Evaluate(hc, lv)
		if err != nil {
			log.WithError(err).
				Error("Error when evaluating input constraint")
			return MismatchAttributes
		}
		switch result {
		case constraints.EvaluateResultMatch:
		case constraints.EvaluateResultNotApplicable:
			log.WithFields(log.Fields{
				"values":     lv,
				"hostname":   hostname,
				"constraint": hc,
			}).Debug("Attributes match constraint")
		default:
			log.WithFields(log.Fields{
				"values":     lv,
				"hostname":   hostname,
				"constraint": hc,
			}).Debug("Attributes do not match constraint")
			return MismatchAttributes
		}
	}

	return Matched
}

// tryMatch atomically tries to match offers from the current host with given
// constraint.
// If current hostOfferSummary can satisfy given constraints, the first return
// value is true and unreserved offer status this instance will be marked as
// `READY`, which will not be used by another placement engine until released.
// If current instance cannot satisfy given constraints, return value will be
// (false, empty-slice) and status will remain unchanged.
func (a *hostOfferSummary) tryMatch(
	c *hostsvc.Constraint,
	evaluator constraints.Evaluator,
) (MatchResult, []*mesos.Offer) {

	a.Lock()
	defer a.Unlock()

	if !a.hasOffer() || a.status != ReadyOffer {
		return MismatchStatus, nil
	}

	readyOffers := make(map[string]*mesos.Offer)
	for id, offer := range a.unreservedOffers {
		readyOffers[id] = offer
	}

	match := matchConstraint(readyOffers, c, evaluator)
	if match == Matched {
		var result []*mesos.Offer
		for _, offer := range readyOffers {
			result = append(result, offer)
		}

		// Setting status to `PlacingOffer`: this ensures proper state
		// tracking of resources on the host and also ensures offers on
		// this host will not be sent to another `AcquireHostOffers`
		// call before released.
		a.status = PlacingOffer
		a.readyCount.Store(0)
		return match, result
	}

	return match, nil
}

func (a *hostOfferSummary) addMesosOffer(offer *mesos.Offer) {
	a.Lock()
	defer a.Unlock()

	offerID := *offer.Id.Value
	if !reservation.HasLabeledReservedResources(offer) {
		a.unreservedOffers[offerID] = offer
		if a.status == ReadyOffer {
			a.readyCount.Inc()
		}
	} else {
		a.reservedOffers[offerID] = offer
		reservedOffers := []*mesos.Offer{}
		for _, offer := range a.reservedOffers {
			reservedOffers = append(reservedOffers, offer)
		}
		a.reservedResources = reservation.GetLabeledReservedResources(
			reservedOffers)
		log.WithFields(log.Fields{
			"offer":                    offer,
			"total_reserved_resources": a.reservedResources,
		}).Debug("Added reserved offer.")
	}
}

func (a *hostOfferSummary) claimForLaunch() (map[string]*mesos.Offer, error) {
	a.Lock()
	defer a.Unlock()
	if a.status != PlacingOffer {
		return nil, errors.New("Host status is not Placing")
	}

	result := make(map[string]*mesos.Offer)
	for id, offer := range a.unreservedOffers {
		result[id] = offer
		delete(a.unreservedOffers, id)
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

	_, ok := a.unreservedOffers[offerID]
	if !ok {
		if _, ok = a.reservedOffers[offerID]; !ok {
			log.WithField("offer", offerID).
				Warn("Remove non-exist reserved offer.")
			return
		}
		// Remove offer then calculate/update the reserved resource.
		delete(a.reservedOffers, offerID)
		reservedOffers := []*mesos.Offer{}
		for _, offer := range a.reservedOffers {
			reservedOffers = append(reservedOffers, offer)
		}
		a.reservedResources = reservation.GetLabeledReservedResources(
			reservedOffers)
		log.WithFields(log.Fields{
			"offerID":                  offerID,
			"total_reserved_resources": a.reservedResources,
		}).Debug("Removed reserved offer.")
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

	delete(a.unreservedOffers, offerID)
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
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	}
	return nil
}
