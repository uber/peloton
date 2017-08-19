package summary

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

// CacheStatus represents status of the offer in offer pool's cache.
type CacheStatus int

const (
	// ReadyOffer represents an offer ready to be used.
	ReadyOffer CacheStatus = iota + 1
	// PlacingOffer represents an offer being used by placement engine.
	PlacingOffer

	// hostPlacingOfferStatusTimeout is a timeout for resetting
	// PlacingOffer status
	hostPlacingOfferStatusTimeout time.Duration = 5 * time.Minute
)

// HostSummary is the core component of host manager's internal
// data structure. It keeps track of offers in various state,
// launching cycles and reservation information.
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
		evaluator constraints.Evaluator,
	) (hostsvc.HostFilterResult, []*mesos.Offer)

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

	// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
	// to ReadyOffer if the PlacingOffer status has expired, and returns
	// wether the hostSummary got reset
	ResetExpiredPlacingOfferStatus(now time.Time) bool
}

// hostSummary is a data struct holding offers on a particular host.
type hostSummary struct {
	sync.Mutex

	// offerID -> unreserved offer
	unreservedOffers             map[string]*mesos.Offer
	status                       CacheStatus
	statusPlacingOfferExpiration time.Time
	readyCount                   atomic.Int32

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

// HasAnyOffer returns true if host has any offer.
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
) hostsvc.HostFilterResult {

	if len(offerMap) == 0 {
		return hostsvc.HostFilterResult_MISMATCH_STATUS
	}

	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		scalarRes := scalar.FromOfferMap(offerMap)
		scalarMin := scalar.FromResourceConfig(min)
		if !scalarRes.Contains(&scalarMin) {
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
// value is true and unreserved offer status this instance will be marked as
// `READY`, which will not be used by another placement engine until released.
// If current instance is not matched by given HostFilter, return value will be
// (actual reason, empty-slice) and status will remain unchanged.
func (a *hostSummary) TryMatch(
	filter *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
) (hostsvc.HostFilterResult, []*mesos.Offer) {

	a.Lock()
	defer a.Unlock()

	if !a.HasOffer() || a.status != ReadyOffer {
		return hostsvc.HostFilterResult_MISMATCH_STATUS, nil
	}

	readyOffers := make(map[string]*mesos.Offer)
	for id, offer := range a.unreservedOffers {
		readyOffers[id] = offer
	}

	match := matchHostFilter(readyOffers, filter, evaluator)
	if match == hostsvc.HostFilterResult_MATCH {
		var result []*mesos.Offer
		for _, offer := range readyOffers {
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

// AddMesosOffer adds a Mesos offer to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffer(ctx context.Context, offer *mesos.Offer) CacheStatus {
	a.Lock()
	defer a.Unlock()

	offerID := offer.GetId().GetValue()
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
		log.WithFields(log.Fields{
			"offer":              unreserved,
			"all_offers_noindex": a.unreservedOffers,
			"status":             a.status,
		}).Warn("Offer removed while not in ready status")
	}

	delete(a.unreservedOffers, offerID)
	return a.status, unreserved
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
func (a *hostSummary) UnreservedAmount() scalar.Resources {
	a.Lock()
	defer a.Unlock()

	return scalar.FromOfferMap(a.unreservedOffers)
}

// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
// to ReadyOffer if the PlacingOffer status has expired, and returns
// wether the hostSummary got reset
func (a *hostSummary) ResetExpiredPlacingOfferStatus(now time.Time) bool {
	if !a.HasOffer() {
		a.Lock()
		defer a.Unlock()
		if a.status == PlacingOffer {
			if now.After(a.statusPlacingOfferExpiration) {
				var offers []*mesos.Offer
				for _, o := range a.unreservedOffers {
					offers = append(offers, o)
				}
				log.WithFields(log.Fields{
					"time":        now,
					"curr_status": a.status,
					"ready_count": a.readyCount.Load(),
					"offers":      offers,
				}).Warn("pruning hostoffer")
				a.casStatusLockFree(PlacingOffer, ReadyOffer)
				return true
			}
		}
	}
	return false
}
