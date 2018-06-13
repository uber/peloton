package summary

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/reservation"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// InvalidCacheStatus is returned when expected status on a hostSummary
// does not match actual value.
type InvalidCacheStatus struct {
	status CacheStatus
}

// Error implements error.Error.
func (e InvalidCacheStatus) Error() string {
	return fmt.Sprintf("Invalid status %v", e.status)
}

// CacheStatus represents status (Ready/Placing) of the host in offer pool's cache (host -> offers).
type CacheStatus int

// OfferType represents the type of offer in the host summary such as reserved, unreserved, or All.
type OfferType int

const (
	// ReadyOffer represents an offer ready to be used.
	ReadyOffer CacheStatus = iota + 1
	// PlacingOffer represents an offer being used by placement engine.
	PlacingOffer

	// hostPlacingOfferStatusTimeout is a timeout for resetting
	// PlacingOffer status back to ReadOffer status.
	hostPlacingOfferStatusTimeout time.Duration = 5 * time.Minute
)

const (
	// Reserved offer type, represents an offer reserved for a particular Mesos Role.
	Reserved OfferType = iota + 1
	// Unreserved offer type, is not reserved for any Mesos role, and can be used to launch task for
	// any role if framework has opt-in MULTI_ROLE capability.
	Unreserved
	// All represents reserved and unreserved offers.
	All
)

// HostSummary is the core component of host manager's internal
// data structure. It keeps track of offers in various state,
// launching cycles and reservation information for a host.
type HostSummary interface {
	// HasOffer provides a quick heuristic about if HostSummary has any
	// unreserved READY offer.
	HasOffer() bool

	// HasAnyOffer returns true if host has any offer, including both reserved
	// and unreserved offer.
	HasAnyOffer() bool

	// TryMatch atomically tries to match offers from the current host with given
	// constraint.
	TryMatch(hostFilter *hostsvc.HostFilter,
		evaluator constraints.Evaluator) (hostsvc.HostFilterResult, []*mesos.Offer)

	// AddMesosOffer adds a Mesos offer to the current HostSummary.
	AddMesosOffer(ctx context.Context, offer *mesos.Offer) CacheStatus

	// AddMesosOffer adds a Mesos offers to the current HostSummary.
	AddMesosOffers(ctx context.Context, offer []*mesos.Offer) CacheStatus

	// RemoveMesosOffer removes the given Mesos offer by its id, and returns
	// CacheStatus and possibly removed offer for tracking purpose.
	RemoveMesosOffer(offerID, reason string) (CacheStatus, *mesos.Offer)

	// ClaimForLaunch releases unreserved offers for task launch.
	ClaimForLaunch() (map[string]*mesos.Offer, error)

	// ClaimReservedOffersForLaunch releases reserved offers for task launch.
	ClaimReservedOffersForLaunch() (map[string]*mesos.Offer, error)

	// CasStatus atomically sets the status to new value if current value is old,
	// otherwise returns error.
	CasStatus(old, new CacheStatus) error

	// UnreservedAmount tells us unreserved resources amount and status for
	// report purpose.
	UnreservedAmount() (scalar.Resources, CacheStatus)

	// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
	// to ReadyOffer if the PlacingOffer status has expired, and returns
	// whether the hostSummary got reset and resources amount for unreserved offers.
	ResetExpiredPlacingOfferStatus(now time.Time) (bool, scalar.Resources)

	// GetOffers returns offers and #offers present for this host, of type reserved, unreserved or all.
	// Returns map of offerid -> offer
	GetOffers(OfferType) map[string]*mesos.Offer
}

// hostSummary is a data struct holding offers on a particular host.
type hostSummary struct {
	sync.Mutex

	// offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
	// offerID -> reserved offer
	reservedOffers map[string]*mesos.Offer

	status                       CacheStatus
	statusPlacingOfferExpiration time.Time

	readyCount atomic.Int32

	// TODO: pass volumeStore in updatePersistentVolume function.
	volumeStore storage.PersistentVolumeStore
}

// New returns a zero initialized hostSummary
func New(volumeStore storage.PersistentVolumeStore) HostSummary {
	return &hostSummary{
		unreservedOffers: make(map[string]*mesos.Offer),
		reservedOffers:   make(map[string]*mesos.Offer),

		status: ReadyOffer,

		volumeStore: volumeStore,
	}
}

// HasOffer is a lock-free heuristic about if the HostSummary has any unreserved ReadyOffer status.
// TODO(zhitao): Create micro-benchmark to prove this is useful,
// otherwise remove it!
func (a *hostSummary) HasOffer() bool {
	return a.readyCount.Load() > 0
}

// HasAnyOffer returns true if host has any offer, including both reserved
// and unreserved offer.
func (a *hostSummary) HasAnyOffer() bool {
	a.Lock()
	defer a.Unlock()
	return len(a.unreservedOffers) > 0 || len(a.reservedOffers) > 0
}

// matchConstraint determines whether given HostFilter matches
// the given map of offers.
func matchHostFilter(
	offerMap map[string]*mesos.Offer,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator) hostsvc.HostFilterResult {

	if len(offerMap) == 0 {
		return hostsvc.HostFilterResult_NO_OFFER
	}

	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		scalarRes := scalar.FromOfferMap(offerMap)
		scalarMin := scalar.FromResourceConfig(min)
		if !scalarRes.Contains(scalarMin) {
			return hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES
		}

		// Special handling for GPU: GPU hosts are only for GPU tasks.
		if scalarRes.HasGPU() != scalarMin.HasGPU() {
			return hostsvc.HostFilterResult_MISMATCH_GPU
		}
	}

	// Match ports resources.
	numPorts := c.GetResourceConstraint().GetNumPorts()
	if numPorts > util.GetPortsNumFromOfferMap(offerMap) {
		return hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES
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
			return hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS
		}

		switch result {
		case constraints.EvaluateResultMatch:
			fallthrough
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
			return hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS
		}
	}

	return hostsvc.HostFilterResult_MATCH
}

// TryMatch atomically tries to match offers from the current host with given
// HostFilter.
// If current hostSummary is matched by given HostFilter, the first return
// value is true and unreserved offer status for this host will be marked as
// `PLACING`, which will not be used by another placement engine until released.
// If current instance is not matched by given HostFilter, return value will be
// (actual reason, empty-slice) and status will remain unchanged.
func (a *hostSummary) TryMatch(
	filter *hostsvc.HostFilter,
	evaluator constraints.Evaluator) (hostsvc.HostFilterResult, []*mesos.Offer) {
	a.Lock()
	defer a.Unlock()

	if a.status != ReadyOffer {
		return hostsvc.HostFilterResult_MISMATCH_STATUS, nil
	}

	if !a.HasOffer() {
		return hostsvc.HostFilterResult_NO_OFFER, nil
	}

	match := matchHostFilter(a.unreservedOffers, filter, evaluator)
	if match == hostsvc.HostFilterResult_MATCH {
		var result []*mesos.Offer
		for _, offer := range a.unreservedOffers {
			result = append(result, offer)
		}

		// Setting status to `PlacingOffer`: this ensures proper state
		// tracking of resources on the host and also ensures offers on
		// this host will not be sent to another `AcquireHostOffers`
		// call before released.
		a.status = PlacingOffer
		a.statusPlacingOfferExpiration = time.Now().Add(hostPlacingOfferStatusTimeout)
		a.readyCount.Store(0)
		return match, result
	}

	return match, nil
}

// addMesosOffer helper method to add an offer to hostsummary.
func (a *hostSummary) addMesosOffer(offer *mesos.Offer) {
	offerID := offer.GetId().GetValue()
	if !reservation.HasLabeledReservedResources(offer) {
		a.unreservedOffers[offerID] = offer
		if a.status == ReadyOffer {
			a.readyCount.Inc()
		}
	} else {
		a.reservedOffers[offerID] = offer
	}
}

// AddMesosOffer adds a Mesos offer to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffer(ctx context.Context, offer *mesos.Offer) CacheStatus {
	a.Lock()
	defer a.Unlock()

	a.addMesosOffer(offer)

	return a.status
}

// AddMesosOffers adds a Mesos offers to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffers(ctx context.Context, offers []*mesos.Offer) CacheStatus {
	a.Lock()
	defer a.Unlock()

	for _, offer := range offers {
		a.addMesosOffer(offer)
	}

	return a.status
}

// ClaimForLaunch atomically check that current hostSummary is in Placing
// status, release offers so caller can use them to launch tasks, and reset
// status to ready.
func (a *hostSummary) ClaimForLaunch() (map[string]*mesos.Offer, error) {
	a.Lock()
	defer a.Unlock()

	if a.status != PlacingOffer {
		return nil, errors.New("Host status is not Placing")
	}

	result := make(map[string]*mesos.Offer)
	result, a.unreservedOffers = a.unreservedOffers, result

	// Reset status to ready so any future offer on the host is considered
	// as ready.
	a.status = ReadyOffer
	a.readyCount.Store(0)
	return result, nil
}

// ClaimReservedOffersForLaunch atomically releases and returns reserved offers
// on current host.
func (a *hostSummary) ClaimReservedOffersForLaunch() (map[string]*mesos.Offer, error) {
	a.Lock()
	defer a.Unlock()

	result := make(map[string]*mesos.Offer)
	result, a.reservedOffers = a.reservedOffers, result

	return result, nil
}

// RemoveMesosOffer removes the given Mesos offer by its id, and returns
// CacheStatus and possibly removed offer for tracking purpose.
func (a *hostSummary) RemoveMesosOffer(offerID, reason string) (CacheStatus, *mesos.Offer) {
	a.Lock()
	defer a.Unlock()

	offer, ok := a.unreservedOffers[offerID]
	if !ok {
		offer, ok = a.reservedOffers[offerID]
		if !ok {
			log.WithFields(log.Fields{
				"offer":  offerID,
				"reason": reason,
			}).Warn("Remove non-existing reserved offer.")
			return a.status, offer
		}
		delete(a.reservedOffers, offerID)
	} else {
		delete(a.unreservedOffers, offerID)
		a.readyCount.Dec()
	}

	switch a.status {
	case ReadyOffer:
		log.WithFields(log.Fields{
			"offer":  offerID,
			"reason": reason,
		}).Debug("Ready offer removed")
	default:
		// This could trigger INVALID_OFFER error later.
		log.WithFields(log.Fields{
			"offer":             offer,
			"unreserved_offers": a.unreservedOffers,
			"reserved_offers":   a.reservedOffers,
			"status":            a.status,
			"reason":            reason,
		}).Warn("Offer removed while not in ready status")
	}

	return a.status, offer
}

// CasStatus atomically sets the status to new value if current value is old,
// otherwise returns error.
func (a *hostSummary) CasStatus(old, new CacheStatus) error {
	a.Lock()
	defer a.Unlock()
	return a.casStatusLockFree(old, new)
}

// casStatus atomically and lock-freely sets the status to new value
// if current value is old, otherwise returns error. This should wrapped
// around locking
func (a *hostSummary) casStatusLockFree(old, new CacheStatus) error {
	if a.status != old {
		return InvalidCacheStatus{a.status}
	}
	a.status = new
	if a.status == ReadyOffer {
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	}

	return nil
}

// UnreservedAmount returns the amount of unreserved resources.
func (a *hostSummary) UnreservedAmount() (scalar.Resources, CacheStatus) {
	a.Lock()
	defer a.Unlock()

	return scalar.FromOfferMap(a.unreservedOffers), a.status
}

// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
// to ReadyOffer if the PlacingOffer status has expired, and returns
// whether the hostSummary got reset
func (a *hostSummary) ResetExpiredPlacingOfferStatus(now time.Time) (bool, scalar.Resources) {
	a.Lock()
	defer a.Unlock()

	if !a.HasOffer() &&
		a.status == PlacingOffer &&
		now.After(a.statusPlacingOfferExpiration) {

		var offers []*mesos.Offer
		for _, o := range a.unreservedOffers {
			offers = append(offers, o)
		}
		log.WithFields(log.Fields{
			"time":        now,
			"curr_status": a.status,
			"ready_count": a.readyCount.Load(),
			"offers":      offers,
		}).Warn("reset host from placing to ready after timeout")

		a.casStatusLockFree(PlacingOffer, ReadyOffer)
		return true, scalar.FromOfferMap(a.unreservedOffers)
	}
	return false, scalar.Resources{}
}

// GetOffers returns offers, and #offers present for this host, of type reserved, unreserved or all.
// Returns map of offerid -> offer
func (a *hostSummary) GetOffers(offertype OfferType) map[string]*mesos.Offer {
	offers := make(map[string]*mesos.Offer)
	switch offertype {
	case Reserved:
		for offerID, offer := range a.reservedOffers {
			offers[offerID] = proto.Clone(offer).(*mesos.Offer)
		}
		break
	case Unreserved:
		for offerID, offer := range a.unreservedOffers {
			offers[offerID] = proto.Clone(offer).(*mesos.Offer)
		}
		break
	case All:
		fallthrough
	default:
		offers = a.GetOffers(Unreserved)
		reservedOffers := a.GetOffers(Reserved)
		for key, value := range reservedOffers {
			offers[key] = value
		}
		break
	}
	return offers
}
