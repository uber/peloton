package summary

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/reservation"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// InvalidCacheStatus is returned when expected status on a hostSummary
// does not actual value.
type InvalidCacheStatus struct {
	status CacheStatus
}

// Error implements error.Error.
func (e InvalidCacheStatus) Error() string {
	return fmt.Sprintf("Invalid status %v", e.status)
}

// MatchResult is an enum type describing why a constraint is not matched.
type MatchResult int

const (
	// Matched indicates that offers on a host is matched
	// to given constraint and will be used.
	Matched MatchResult = iota + 1
	// InsufficientResources due to insufficient scalar resources.
	InsufficientResources
	// MismatchAttributes indicates attributes mismatches
	// to task-host scheduling constraint.
	MismatchAttributes
	// MismatchGPU indicates host is reserved for GPU tasks while task not
	// using GPU.
	MismatchGPU
	// MismatchStatus indicates that host is not in ready status.
	MismatchStatus
	// HostLimitExceeded indicates host offers matched so far already
	// exceeded host limit.
	HostLimitExceeded
)

// CacheStatus represents status of the offer in offer pool's cache.
type CacheStatus int

const (
	// ReadyOffer represents an offer ready to be used.
	ReadyOffer CacheStatus = iota + 1
	// PlacingOffer represents an offer being used by placement engine.
	PlacingOffer
)

// HostSummary is the core component of host manager's internal
// data structure. It keeps track of offers in various state,
// launching cycles and reservation information.
type HostSummary interface {
	// HasOffer provides a quick heuristic about if HostSummary has any offer.
	HasOffer() bool

	// TryMatch atomically tries to match offers from the current host with given
	// constraint.
	TryMatch(
		c *hostsvc.Constraint,
		evaluator constraints.Evaluator,
	) (MatchResult, []*mesos.Offer)

	// AddMesosOffer adds a Mesos offer to the current HostSummary.
	AddMesosOffer(ctx context.Context, offer *mesos.Offer) CacheStatus

	// RemoveMesosOffer removes the given Mesos offer by its id, and returns
	// CacheStatus and possibly removed offer for tracking purpose.
	RemoveMesosOffer(offerID string) (CacheStatus, *mesos.Offer)

	// ClaimForLaunch releases offers for task launch.
	ClaimForLaunch() (map[string]*mesos.Offer, error)

	// ClaimReservedOffersForLaunch release reserved offers for task launch.
	ClaimReservedOffersForLaunch() (map[string]*mesos.Offer, error)

	// CasStatus atomically sets the status to new value if current value is old,
	// otherwise returns error.
	CasStatus(old, new CacheStatus) error

	UnreservedAmount() scalar.Resources
}

// hostSummary is a data struct holding offers on a particular host.
type hostSummary struct {
	sync.Mutex

	// offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
	status           CacheStatus
	readyCount       atomic.Int32

	// offerID -> reserved offer
	reservedOffers map[string]*mesos.Offer
	// reservedResources has reservationLabelID -> resources.
	reservedResources map[string]*reservation.ReservedResources

	volumeStore storage.PersistentVolumeStore
}

// New returns a zero initialized hostSummary
func New(
	volumeStore storage.PersistentVolumeStore) HostSummary {
	return &hostSummary{
		unreservedOffers: make(map[string]*mesos.Offer),

		status: ReadyOffer,

		reservedOffers: make(map[string]*mesos.Offer),
		reservedResources: make(
			map[string]*reservation.ReservedResources),
		volumeStore: volumeStore,
	}
}

// HasOffer is a lock-free heuristic about if the hostOfferSummary has any offer.
// TODO(zhitao): Create micro-benchmark to prove this is useful,
// otherwise remove it!
func (a *hostSummary) HasOffer() bool {
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

// TryMatch atomically tries to match offers from the current host with given
// constraint.
// If current hostSummary can satisfy given constraints, the first return
// value is true and unreserved offer status this instance will be marked as
// `READY`, which will not be used by another placement engine until released.
// If current instance cannot satisfy given constraints, return value will be
// (false, empty-slice) and status will remain unchanged.
func (a *hostSummary) TryMatch(
	c *hostsvc.Constraint,
	evaluator constraints.Evaluator,
) (MatchResult, []*mesos.Offer) {

	a.Lock()
	defer a.Unlock()

	if !a.HasOffer() || a.status != ReadyOffer {
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

// AddMesosOffer adds a Mesos offer to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffer(ctx context.Context, offer *mesos.Offer) CacheStatus {
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
		a.updatePersistentVolumes(ctx)
		log.WithFields(log.Fields{
			"offer":                    offer,
			"total_reserved_resources": a.reservedResources,
		}).Debug("Added reserved offer.")
	}

	return a.status
}

// storePersistentVolumes iterates reserved resources and write volume info into
// the db if not exist.
func (a *hostSummary) updatePersistentVolumes(ctx context.Context) error {
	for labels, res := range a.reservedResources {
		// TODO(mu): unreserve resources without persistent volume.
		if len(res.Volumes) == 0 {
			continue
		}

		if len(res.Volumes) != 1 {
			log.WithField("reserved_resource", res).
				WithField("labels", labels).
				Warn("more than one volume reserved for same label")
		}

		// TODO(mu): Add cache for created volumes to avoid repeated db read/write.
		for _, v := range res.Volumes {
			pv, err := a.volumeStore.GetPersistentVolume(ctx, v)
			if err != nil || pv == nil {
				log.WithFields(
					log.Fields{
						"reserved_resource": res,
						"labels":            labels,
						"volume_id":         v}).
					WithError(err).
					Error("volume contained in reserved resources but not found in db")
				continue
			}

			// TODO(mu): destory/unreserve volume/resource if goalstate is DELETED.
			if pv.GetState() == volume.VolumeState_CREATED {
				// Skip update volume if state already created.
				continue
			}

			// TODO: compare volume info with result from db.
			log.WithFields(
				log.Fields{
					"reserved_resource": res,
					"labels":            labels,
					"volume":            pv}).
				Info("updating persistent volume table")
			err = a.volumeStore.UpdatePersistentVolume(ctx, v, volume.VolumeState_CREATED)
			if err != nil {
				log.WithFields(
					log.Fields{
						"reserved_resource": res,
						"labels":            labels,
						"volume":            pv}).
					WithError(err).
					Error("volume state update failed")
			}
		}
	}

	return nil
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
	for id, offer := range a.unreservedOffers {
		result[id] = offer
		delete(a.unreservedOffers, id)
	}

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
	for id, offer := range a.reservedOffers {
		result[id] = offer
		delete(a.reservedOffers, id)
	}

	return result, nil
}

// RemoveMesosOffer removes the given Mesos offer by its id, and returns
// CacheStatus and possibly removed offer for tracking purpose.
func (a *hostSummary) RemoveMesosOffer(offerID string) (CacheStatus, *mesos.Offer) {
	a.Lock()
	defer a.Unlock()

	unreserved, ok := a.unreservedOffers[offerID]
	if !ok {
		reserved, ok2 := a.reservedOffers[offerID]
		if !ok2 {
			log.WithField("offer", offerID).
				Warn("Remove non-exist reserved offer.")
			return a.status, reserved
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
		return a.status, reserved
	}

	switch a.status {
	case ReadyOffer:
		log.WithField("offer", offerID).Debug("Ready offer removed")
		a.readyCount.Dec()
	default:
		// This could trigger INVALID_OFFER error later.
		log.WithField("offer", offerID).
			WithField("status", a.status).
			Warn("Offer removed while not in ready status")
	}

	delete(a.unreservedOffers, offerID)
	return a.status, unreserved
}

// CasStatus atomically sets the status to new value if current value is old,
// otherwise returns error.
func (a *hostSummary) CasStatus(old, new CacheStatus) error {
	a.Lock()
	defer a.Unlock()
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
func (a *hostSummary) UnreservedAmount() scalar.Resources {
	a.Lock()
	defer a.Unlock()

	return scalar.FromOfferMap(a.unreservedOffers)
}
