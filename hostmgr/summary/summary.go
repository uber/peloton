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
	"code.uber.internal/infra/peloton/hostmgr/host"
	"code.uber.internal/infra/peloton/hostmgr/reservation"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// InvalidHostStatus is returned when expected status on a hostSummary
// does not match actual value.
type InvalidHostStatus struct {
	status HostStatus
}

// Error implements error.Error.
func (e InvalidHostStatus) Error() string {
	return fmt.Sprintf("Invalid status %v", e.status)
}

// HostStatus represents status (Ready/Placing/Reserved) of the host in offer pool's cache (host -> offers).
type HostStatus int

// OfferType represents the type of offer in the host summary such as reserved, unreserved, or All.
type OfferType int

const (
	// ReadyHost represents an host ready to be used.
	ReadyHost HostStatus = iota + 1
	// PlacingHost represents an host being used by placement engine.
	PlacingHost
	// ReservedHost represents an host is reserved for tasks
	ReservedHost

	// hostPlacingOfferStatusTimeout is a timeout for resetting
	// PlacingHost status back to ReadHost status.
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
	TryMatch(
		hostFilter *hostsvc.HostFilter,
		evaluator constraints.Evaluator) (hostsvc.HostFilterResult, []*mesos.Offer)

	// AddMesosOffer adds a Mesos offer to the current HostSummary.
	AddMesosOffer(ctx context.Context, offer *mesos.Offer) HostStatus

	// AddMesosOffer adds a Mesos offers to the current HostSummary.
	AddMesosOffers(ctx context.Context, offer []*mesos.Offer) HostStatus

	// RemoveMesosOffer removes the given Mesos offer by its id, and returns
	// CacheStatus and possibly removed offer for tracking purpose.
	RemoveMesosOffer(offerID, reason string) (HostStatus, *mesos.Offer)

	// ClaimForLaunch releases unreserved offers for task launch.
	ClaimForLaunch() (map[string]*mesos.Offer, error)

	// ClaimReservedOffersForLaunch releases reserved offers for task launch.
	ClaimReservedOffersForLaunch() (map[string]*mesos.Offer, error)

	// CasStatus atomically sets the status to new value if current value is old,
	// otherwise returns error.
	CasStatus(old, new HostStatus) error

	// UnreservedAmount tells us unreserved resources amount and status for
	// report purpose.
	UnreservedAmount() (scalar.Resources, HostStatus)

	// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
	// to ReadyOffer if the PlacingOffer status has expired, and returns
	// whether the hostSummary got reset and resources amount for unreserved offers.
	ResetExpiredPlacingOfferStatus(now time.Time) (bool, scalar.Resources)

	// GetOffers returns offers and #offers present for this host, of type reserved, unreserved or all.
	// Returns map of offerid -> offer
	GetOffers(OfferType) map[string]*mesos.Offer

	// GetHostname returns the hostname of the host
	GetHostname() string

	// GetHostStatus returns the HostStatus of the host
	GetHostStatus() HostStatus
}

// hostSummary is a data struct holding offers on a particular host.
type hostSummary struct {
	sync.Mutex

	// hostname of the host
	hostname string

	// scarceResourceTypes are resources, which are exclusively reserved for specific task requirements,
	// and to prevent every task to schedule on those hosts such as GPU.
	scarceResourceTypes []string

	// offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
	// offerID -> reserved offer
	reservedOffers map[string]*mesos.Offer

	status                       HostStatus
	statusPlacingOfferExpiration time.Time

	readyCount atomic.Int32

	// TODO: pass volumeStore in updatePersistentVolume function.
	volumeStore storage.PersistentVolumeStore
}

// New returns a zero initialized hostSummary
func New(
	volumeStore storage.PersistentVolumeStore,
	scarceResourceTypes []string,
	hostname string) HostSummary {
	return &hostSummary{
		unreservedOffers: make(map[string]*mesos.Offer),
		reservedOffers:   make(map[string]*mesos.Offer),

		scarceResourceTypes: scarceResourceTypes,

		status: ReadyHost,

		volumeStore: volumeStore,

		hostname: hostname,
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
	evaluator constraints.Evaluator,
	scalarAgentRes scalar.Resources,
	scarceResourceTypes []string) hostsvc.HostFilterResult {

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

		// Validates iff requested resource types are present on current host.
		// It prevents a task to be launched on agent which has superset of requested resource types.
		// As of now, supported scarce resource type is GPU.
		for _, resourceType := range scarceResourceTypes {
			if scalar.HasResourceType(scalarAgentRes, scalarMin, resourceType) {
				return hostsvc.HostFilterResult_SCARCE_RESOURCES
			}
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

	if a.status != ReadyHost {
		return hostsvc.HostFilterResult_MISMATCH_STATUS, nil
	}

	if !a.HasOffer() {
		return hostsvc.HostFilterResult_NO_OFFER, nil
	}

	match := matchHostFilter(
		a.unreservedOffers,
		filter,
		evaluator,
		scalar.FromMesosResources(host.GetAgentInfo(a.GetHostname()).GetResources()),
		a.scarceResourceTypes)
	if match == hostsvc.HostFilterResult_MATCH {
		var result []*mesos.Offer
		for _, offer := range a.unreservedOffers {
			result = append(result, offer)
		}
		// Setting status to `PlacingHost`: this ensures proper state
		// tracking of resources on the host and also ensures offers on
		// this host will not be sent to another `AcquireHostOffers`
		// call before released.
		err := a.casStatusLockFree(ReadyHost, PlacingHost)
		if err != nil {
			return hostsvc.HostFilterResult_NO_OFFER, nil
		}
		return match, result
	}
	return match, nil
}

// addMesosOffer helper method to add an offer to hostsummary.
func (a *hostSummary) addMesosOffer(offer *mesos.Offer) {
	offerID := offer.GetId().GetValue()
	if !reservation.HasLabeledReservedResources(offer) {
		a.unreservedOffers[offerID] = offer
		if a.status == ReadyHost {
			a.readyCount.Inc()
		}
	} else {
		a.reservedOffers[offerID] = offer
	}
}

// AddMesosOffer adds a Mesos offer to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffer(ctx context.Context, offer *mesos.Offer) HostStatus {
	a.Lock()
	defer a.Unlock()

	a.addMesosOffer(offer)

	return a.status
}

// AddMesosOffers adds a Mesos offers to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffers(ctx context.Context, offers []*mesos.Offer) HostStatus {
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

	if a.status != PlacingHost {
		return nil, errors.New("Host status is not Placing")
	}

	result := make(map[string]*mesos.Offer)
	result, a.unreservedOffers = a.unreservedOffers, result

	// Reset status to ready so any future offer on the host is considered
	// as ready.
	a.status = ReadyHost
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
func (a *hostSummary) RemoveMesosOffer(offerID, reason string) (HostStatus, *mesos.Offer) {
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
	case ReadyHost:
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
func (a *hostSummary) CasStatus(old, new HostStatus) error {
	a.Lock()
	defer a.Unlock()
	return a.casStatusLockFree(old, new)
}

// casStatus atomically and lock-freely sets the status to new value
// if current value is old, otherwise returns error. This should wrapped
// around locking
func (a *hostSummary) casStatusLockFree(old, new HostStatus) error {
	if a.status != old {
		return InvalidHostStatus{a.status}
	}
	a.status = new

	switch a.status {
	case ReadyHost:
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	case PlacingHost:
		a.statusPlacingOfferExpiration = time.Now().Add(hostPlacingOfferStatusTimeout)
		a.readyCount.Store(0)
	}
	return nil
}

// UnreservedAmount returns the amount of unreserved resources.
func (a *hostSummary) UnreservedAmount() (scalar.Resources, HostStatus) {
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
		a.status == PlacingHost &&
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

		a.casStatusLockFree(PlacingHost, ReadyHost)
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

// GetHostname returns the hostname of the host
func (a *hostSummary) GetHostname() string {
	return a.hostname
}

// GetHostStatus returns the HostStatus of the host
func (a *hostSummary) GetHostStatus() HostStatus {
	a.Lock()
	defer a.Unlock()
	return a.status
}
