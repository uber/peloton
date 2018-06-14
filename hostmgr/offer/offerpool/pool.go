package offerpool

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
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
	// Add offers to the pool. Filters out non-viable offers (offers with non-nil Unavailability)
	// and returns slice of acceptable offers.
	AddOffers(context.Context, []*mesos.Offer) []*mesos.Offer

	// Rescind a offer from the pool, on Mesos Master --offer-timeout
	// Returns whether the offer is found in the pool.
	RescindOffer(*mesos.OfferID) bool

	// RemoveExpiredOffers, prunes offers from the pool, when offer-hold-time is expired.
	RemoveExpiredOffers() (map[string]*TimedOffer, int)

	// Clear all offers in the pool
	Clear()

	// Decline offers, sends Mesos Master decline call and removes from offer pool.
	DeclineOffers(ctx context.Context, offerIds []*mesos.OfferID) error

	// ClaimForPlace obtains offers from pool conforming to given HostFilter
	// for placement purposes.
	// First return value is returned offers, grouped by hostname as key,
	// Second return value is a map from hostsvc.HostFilterResult to count.
	ClaimForPlace(constraint *hostsvc.HostFilter) (map[string][]*mesos.Offer, map[string]uint32, error)

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

	// ResetExpiredHostSummaries resets the status of each hostSummary of the offerPool
	// from PlacingOffer to ReadyOffer if the PlacingOffer status has expired
	// and returns the hostnames which got reset
	ResetExpiredHostSummaries(now time.Time) []string

	// GetOffers returns hostname -> map(offerid -> offer) & #offers for reserved, unreserved or all offer type.
	GetOffers(offerType summary.OfferType) (map[string]map[string]*mesos.Offer, int)

	// RemoveReservedOffers removes given offerID from given host.
	RemoveReservedOffer(hostname string, offerID string)

	// RefreshGaugeMaps refreshes ready/placing metrics from all hosts.
	RefreshGaugeMaps()
}

const (
	_defaultContextTimeout = 10 * time.Second

	// Reject offers from unavailable/maintenance host only before 3 hour of starting window.
	// Mesos Master sets unix nano seconds for unavailability start time.
	_defaultRejectUnavailableOffer = int64(10800000000000)
)

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(
	offerHoldTime time.Duration,
	schedulerClient mpb.SchedulerClient,
	metrics *Metrics,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	volumeStore storage.PersistentVolumeStore) Pool {
	p := &offerPool{
		hostOfferIndex: make(map[string]summary.HostSummary),

		offerHoldTime: offerHoldTime,

		mSchedulerClient:           schedulerClient,
		mesosFrameworkInfoProvider: frameworkInfoProvider,

		metrics: metrics,

		volumeStore: volumeStore,
	}

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

	// number of hosts that has any offers, i.e. both reserved and unreserved
	// offers. It includes both READY and PLACING state hosts.
	availableHosts atomic.Uint32

	// Map from offer id to hostname and offer expiration time.
	// Used when offer is rescinded or pruned.
	timedOffers sync.Map

	// Time to hold offer in offer pool
	offerHoldTime time.Duration

	mSchedulerClient           mpb.SchedulerClient
	mesosFrameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider

	metrics          *Metrics
	readyResources   scalar.AtomicResources
	placingResources scalar.AtomicResources

	volumeStore storage.PersistentVolumeStore
}

// ClaimForPlace obtains offers from pool conforming to given constraints.
// Results are grouped by hostname as key. Here, offers are not cleared from from the offer pool.
// First return value is returned offers, grouped by hostname as key,
// Second return value is a map from hostsvc.HostFilterResult to count.
func (p *offerPool) ClaimForPlace(hostFilter *hostsvc.HostFilter) (map[string][]*mesos.Offer, map[string]uint32, error) {
	p.RLock()
	defer p.RUnlock()

	matcher := NewMatcher(
		hostFilter,
		constraints.NewEvaluator(task.LabelConstraint_HOST))

	for hostname, summary := range p.hostOfferIndex {
		matcher.tryMatch(hostname, summary)
		if matcher.HasEnoughHosts() {
			break
		}
	}

	hasEnoughHosts := matcher.HasEnoughHosts()
	hostOffers, resultCount := matcher.getHostOffers()

	if !hasEnoughHosts {
		// Still proceed to return something.
		log.WithFields(log.Fields{
			"host_filter":                 hostFilter,
			"matched_host_offers_noindex": hostOffers,
			"match_result_counts":         resultCount,
		}).Debug("Not enough offers are matched to given constraints")
	}
	// NOTE: we should not clear the entries for the selected offers in p.offers
	// because we still need to visit corresponding offers, when these offers
	// are returned or used.
	return hostOffers, resultCount, nil
}

// ClaimForLaunch takes offers from pool (removes from hostsummary) for launch.
func (p *offerPool) ClaimForLaunch(hostname string, useReservedOffers bool) (map[string]*mesos.Offer, error) {
	p.RLock()
	defer p.RUnlock()

	var offerMap map[string]*mesos.Offer
	var err error

	hs, ok := p.hostOfferIndex[hostname]
	if !ok {
		return nil, errors.New("cannot find input hostname " + hostname)
	}

	if useReservedOffers {
		offerMap, err = hs.ClaimReservedOffersForLaunch()
	} else {
		offerMap, err = hs.ClaimForLaunch()
	}

	if err != nil {
		return nil, err
	}

	if len(offerMap) == 0 {
		return nil, errors.New("No offer found to launch task on " + hostname)
	}

	for id := range offerMap {
		if _, ok := p.timedOffers.Load(id); ok {
			// Remove offer from the offerid -> hostname map.
			p.timedOffers.Delete(id)
		} else {
			log.WithFields(log.Fields{
				"offer_id": id,
				"host":     hostname,
			}).Warn("ClaimForLaunch: OfferID not found in pool.")
		}
	}

	if !hs.HasAnyOffer() {
		p.metrics.AvailableHosts.Update(float64(p.availableHosts.Dec()))
	}

	return offerMap, nil
}

// validateOfferUnavailablity for incoming offer.
// Reject an offer if maintenance start time is less than current time.
// Reject an offer if current time is less than 3 hours to maintenance start time.
// Accept an offer if current time is more than 3 hours to maintenance start time.
func validateOfferUnavailablity(offer *mesos.Offer) bool {
	if offer.GetUnavailability() != nil {
		currentTime := time.Now().UnixNano()
		unavailabilityStartTime := offer.Unavailability.Start.GetNanoseconds()
		if (unavailabilityStartTime-currentTime > 0 &&
			unavailabilityStartTime-currentTime < _defaultRejectUnavailableOffer) ||
			(currentTime-unavailabilityStartTime >= 0) {
			return true
		}
	}
	return false
}

// AddOffers is a callback event when Mesos Master sends offers.
func (p *offerPool) AddOffers(ctx context.Context, offers []*mesos.Offer) []*mesos.Offer {
	var acceptableOffers []*mesos.Offer
	var unavailableOffers []*mesos.OfferID
	hostnameToOffers := make(map[string][]*mesos.Offer)

	for _, offer := range offers {
		if validateOfferUnavailablity(offer) {
			unavailableOffers = append(unavailableOffers, offer.Id)
			continue
		}
		p.timedOffers.Store(offer.Id.GetValue(), &TimedOffer{
			Hostname:   offer.GetHostname(),
			Expiration: time.Now().Add(p.offerHoldTime),
		})

		oldOffers := hostnameToOffers[offer.GetHostname()]
		hostnameToOffers[offer.GetHostname()] = append(oldOffers, offer)
		acceptableOffers = append(acceptableOffers, offer)
	}

	// Decline unavailable offers.
	if len(unavailableOffers) > 0 {
		log.WithField("unavailable_offers", unavailableOffers).Debug("Offer unavailable due to maintenance on these hosts.")
		p.DeclineOffers(ctx, unavailableOffers)
	}
	log.WithField("acceptable_offers", acceptableOffers).Debug("Acceptable offers.")

	p.Lock()
	defer p.Unlock()

	for hostname, offers := range hostnameToOffers {
		_, ok := p.hostOfferIndex[hostname]

		if !ok {
			p.hostOfferIndex[hostname] = summary.New(p.volumeStore)
		}

		if !p.hostOfferIndex[hostname].HasAnyOffer() {
			p.metrics.AvailableHosts.Update(float64(p.availableHosts.Inc()))
		}

		p.hostOfferIndex[hostname].AddMesosOffers(ctx, offers)
	}

	return acceptableOffers
}

// removeOffer is a helper method to remove an offer from timedOffers and hostSummary.
func (p *offerPool) removeOffer(offerID, reason string) {
	offer, ok := p.timedOffers.Load(offerID)
	if !ok {
		log.WithField("offer_id", offerID).Info("removeOffer: OfferID not found in pool.")
		return
	}
	p.timedOffers.Delete(offerID)

	// Remove offer from hostOffers
	hostName := offer.(*TimedOffer).Hostname
	hostOffers, ok := p.hostOfferIndex[hostName]
	if !ok {
		log.WithFields(log.Fields{
			"host":     hostName,
			"offer_id": offerID,
		}).Warn("host not found in hostOfferIndex")
	} else {
		hostOffers.RemoveMesosOffer(offerID, reason)

		// If host summary (host -> offers), does not have any offer corresponding to a host,
		// decrement metric for available hosts.
		if !p.hostOfferIndex[hostName].HasAnyOffer() {
			p.metrics.AvailableHosts.Update(float64(p.availableHosts.Dec()))
		}
	}
}

// RescindOffer is a callback event when Mesos Master rescinds a offer.
// Reasons for offer rescind can be lost agent, offer_timeout and more.
func (p *offerPool) RescindOffer(offerID *mesos.OfferID) bool {
	p.RLock()
	defer p.RUnlock()

	oID := *offerID.Value
	log.WithField("offer_id", oID).Info("RescindOffer Received")

	p.removeOffer(oID, "Offer is rescinded.")
	return true
}

// RemoveExpiredOffers removes offers which are expired from pool
// and return the list of removed mesos offer ids and their location as well as
// how many valid offers are still left.
func (p *offerPool) RemoveExpiredOffers() (map[string]*TimedOffer, int) {
	p.RLock()
	defer p.RUnlock()

	offersToDecline := map[string]*TimedOffer{}
	p.timedOffers.Range(func(offerID, timedOffer interface{}) bool {
		if time.Now().After(timedOffer.(*TimedOffer).Expiration) {
			log.WithField("offer_id", offerID).Info("Removing expired offer from pool.")
			offersToDecline[offerID.(string)] = timedOffer.(*TimedOffer)
		}
		return true
	})

	// Remove the expired offers from hostOfferIndex
	if len(offersToDecline) > 0 {
		for offerID := range offersToDecline {
			p.removeOffer(offerID, "Offer is expired.")
		}
	}

	length := 0
	p.timedOffers.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	return offersToDecline, length
}

// Clear removes all offers from pool.
func (p *offerPool) Clear() {
	p.RLock()
	defer p.RUnlock()

	log.Info("Clean up offerpool.")
	p.timedOffers.Range(func(key interface{}, value interface{}) bool {
		p.timedOffers.Delete(key)
		return true
	})
	p.hostOfferIndex = map[string]summary.HostSummary{}
	p.availableHosts.Store(0)
	p.metrics.AvailableHosts.Update(float64(p.availableHosts.Load()))
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(ctx context.Context, offerIDs []*mesos.OfferID) error {
	p.RLock()
	defer p.RUnlock()

	log.WithField("offer_ids", offerIDs).Info("Decline offer.")

	callType := sched.Call_DECLINE
	msg := &sched.Call{
		FrameworkId: p.mesosFrameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Decline: &sched.Call_Decline{
			OfferIds: offerIDs,
		},
	}
	msid := p.mesosFrameworkInfoProvider.GetMesosStreamID(ctx)
	err := p.mSchedulerClient.Call(msid, msg)
	if err != nil {
		// Ideally, we assume that Mesos has offer_timeout configured,
		// so in the event that offer declining call fails, offers
		// should eventually be invalidated by Mesos.
		log.WithError(err).
			WithField("call", msg).
			Warn("Failed to decline offers.")
		p.metrics.DeclineFail.Inc(1)
		return err
	}

	for _, offerID := range offerIDs {
		p.removeOffer(*offerID.Value, "Offer is declined.")
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
		log.WithField("host", hostname).
			Warn("Offers returned to pool but not found, maybe pruned?")
		return nil
	}

	err := hostOffers.CasStatus(summary.PlacingOffer, summary.ReadyOffer)
	if err != nil {
		return err
	}

	delta, _ := hostOffers.UnreservedAmount()

	log.WithFields(log.Fields{
		"host":              hostname,
		"delta":             delta,
		"placing_resources": p.placingResources.Get(),
		"ready_resources":   p.readyResources.Get(),
	}).Debug("Returned offers to Ready state for this host.")

	return nil
}

// ResetExpiredHostSummaries resets the status of each hostSummary of the offerPool
// from PlacingOffer to ReadyOffer if the PlacingOffer status has expired
// and returns the hostnames which got reset
func (p *offerPool) ResetExpiredHostSummaries(now time.Time) []string {
	p.RLock()
	defer p.RUnlock()
	var resetHostnames []string
	for hostname, summary := range p.hostOfferIndex {
		if reset, res := summary.ResetExpiredPlacingOfferStatus(now); reset {
			resetHostnames = append(resetHostnames, hostname)
			log.WithFields(log.Fields{
				"host":    hostname,
				"summary": summary,
				"delta":   res,
			}).Info("reset expired host summaries.")
		}
		if !summary.HasAnyOffer() {
			p.metrics.AvailableHosts.Update(float64(p.availableHosts.Dec()))
		}
	}
	return resetHostnames
}

// RemoveReservedOffer removes an reserved offer from hostsummary.
func (p *offerPool) RemoveReservedOffer(hostname string, offerID string) {
	p.RLock()
	defer p.RUnlock()

	log.WithFields(log.Fields{
		"offer_id": offerID,
		"hostname": hostname}).Info("Remove reserved offer.")
	p.removeOffer(offerID, "Reserved Offer is removed from the pool.")
}

// getOffersByType is a helper method to get hostoffers by offer type
func (p *offerPool) getOffersByType(
	offerType summary.OfferType) (map[string]map[string]*mesos.Offer, int) {
	hostOffers := make(map[string]map[string]*mesos.Offer)
	var offersCount int
	for hostname, summary := range p.hostOfferIndex {
		offers := summary.GetOffers(offerType)
		hostOffers[hostname] = offers
		offersCount += len(offers)
	}

	return hostOffers, offersCount
}

// GetOffers returns hostname -> map(offerid -> offer)
// and #offers for reserved, unreserved or all offer types.
func (p *offerPool) GetOffers(
	offerType summary.OfferType) (map[string]map[string]*mesos.Offer, int) {
	p.RLock()
	defer p.RUnlock()

	var hostOffers map[string]map[string]*mesos.Offer
	var offersCount int

	switch offerType {
	case summary.Reserved:
		hostOffers, offersCount = p.getOffersByType(summary.Reserved)
		break
	case summary.Unreserved:
		hostOffers, offersCount = p.getOffersByType(summary.Unreserved)
		break
	case summary.All:
		fallthrough
	default:
		hostOffers, offersCount = p.getOffersByType(summary.All)
		break
	}
	return hostOffers, offersCount
}

// RefreshGaugeMaps refreshes the metrics for hosts in ready and placing state.
func (p *offerPool) RefreshGaugeMaps() {
	log.Debug("starting offer pool usage metrics refresh")

	p.RLock()
	defer p.RUnlock()

	stopWatch := p.metrics.refreshTimer.Start()

	ready := scalar.Resources{}
	placing := scalar.Resources{}

	for _, h := range p.hostOfferIndex {
		amount, status := h.UnreservedAmount()
		switch status {
		case summary.ReadyOffer:
			ready = ready.Add(amount)
		case summary.PlacingOffer:
			placing = placing.Add(amount)
		}
	}

	p.metrics.ready.Update(ready)
	p.metrics.placing.Update(placing)

	log.WithFields(log.Fields{
		"placing_resources": placing,
		"ready_resources":   ready,
	}).Debug("offer pool usage metrics refreshed")

	stopWatch.Stop()
}
