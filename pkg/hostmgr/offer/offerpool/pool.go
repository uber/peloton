// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package offerpool

import (
	"context"
	"reflect"
	"sync"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

// Pool caches a set of offers received from Mesos master. It is
// currently only instantiated at the leader of Peloton masters.
type Pool interface {
	// Add offers to the pool. Filters out non-viable offers (offers with
	// non-nil Unavailability) and returns slice of acceptable offers.
	AddOffers(context.Context, []*mesos.Offer) []*mesos.Offer

	// Rescind a offer from the pool, on Mesos Master --offer-timeout
	// Returns whether the offer is found in the pool.
	RescindOffer(*mesos.OfferID) bool

	// UpdateTasksOnHost updates the task to host map for host summary.
	UpdateTasksOnHost(taskID string, taskState task.TaskState, taskInfo *task.TaskInfo)

	// RemoveExpiredOffers, prunes offers from the pool, when offer-hold-time
	// is expired.
	RemoveExpiredOffers() (map[string]*TimedOffer, int)

	// Clear all offers in the pool
	Clear()

	// Decline offers, sends Mesos Master decline call and removes from offer
	// pool.
	DeclineOffers(ctx context.Context, offerIds []*mesos.OfferID) error

	// ClaimForPlace obtains offers from pool conforming to given HostFilter
	// for placement purposes.
	// First return value is returned offers, grouped by hostname as key,
	// Second return value is a map from hostsvc.HostFilterResult to count.
	ClaimForPlace(
		ctx context.Context,
		constraint *hostsvc.HostFilter) (
		map[string]*summary.Offer,
		map[string]uint32, error)

	// ClaimForLaunch finds offers previously for placement on given host.
	// The difference from ClaimForPlace is that offers claimed from this
	// function are considered used and sent back to Mesos master in a Launch
	// operation, while result in `ClaimForPlace` are still considered part
	// of peloton apps.
	// An optional list of task ids is provided if the host is held for
	// the tasks
	ClaimForLaunch(
		hostname string,
		hostOfferID string,
		launchableTasks []*hostsvc.LaunchableTask,
		taskIDs ...*peloton.TaskID) (map[string]*mesos.Offer, error)

	// ReturnUnusedOffers returns previously placed offers on hostname back
	// to current offer pool so they can be used by future launch actions.
	ReturnUnusedOffers(hostname string) error

	// TODO: Add following API for viewing offers, and optionally expose
	//  this in a debugging endpoint.
	// View() (map[string][]*mesos.Offer, err)

	// ResetExpiredPlacingHostSummaries resets the status of each hostSummary of the
	// offerPool from PlacingOffer to ReadyOffer if the PlacingOffer status has
	// expired and returns the hostnames which got reset
	ResetExpiredPlacingHostSummaries(now time.Time) []string

	// ResetExpiredHeldHostSummaries resets the status of each hostSummary of the
	// offerPool from HeldHost to ReadyHost if the HeldHost status has
	// expired and returns the hostnames which got reset
	ResetExpiredHeldHostSummaries(now time.Time) []string

	// GetAllOffers returns hostOffers : map[hostname] -> map(offerid -> offers
	// & #offers for reserved, unreserved or all offer type.
	GetAllOffers() (map[string]map[string]*mesos.Offer, int)

	// RefreshGaugeMaps refreshes ready/placing metrics from all hosts.
	RefreshGaugeMaps()

	// GetHostSummary returns the host summary object for the given host name
	GetHostSummary(hostName string) (summary.HostSummary, error)

	// GetBinPackingRanker returns the associated ranker with the offer pool
	GetBinPackingRanker() binpacking.Ranker

	// GetHostOfferIndex returns the host to host summary mapping
	// it makes the copy and returns the new map
	GetHostOfferIndex() map[string]summary.HostSummary

	// GetHostSummaries returns a map of hostname to host summary object
	GetHostSummaries(hostnames []string) (map[string]summary.HostSummary, error)

	// GetHostHeldForTask returns the host that is held for the task
	GetHostHeldForTask(taskID *peloton.TaskID) string

	// HoldForTasks holds the host for the tasks specified
	HoldForTasks(hostname string, taskIDs []*peloton.TaskID) error

	// ReleaseHoldForTasks release the hold of host for the tasks specified
	ReleaseHoldForTasks(hostname string, taskIDs []*peloton.TaskID) error

	// SetHostPoolManager set host pool manager in the offer pool.
	SetHostPoolManager(manager manager.HostPoolManager)
}

const (
	// Reject offers from unavailable/maintenance host only before 3 hour of
	// starting window.
	// Mesos Master sets unix nano seconds for unavailability start time.
	_defaultRejectUnavailableOffer = int64(10800000000000)
)

var (
	// supportedScarceResourceTypes are resources types supported to launch
	// scarce resource tasks, exclusively on scarce resource type hosts.
	//ToDo: move to lower case (match mesos resource type string)
	supportedScarceResourceTypes = []string{"GPU"}

	// supportedSlackResourceTypes are slack resource types supported by Peloton.
	supportedSlackResourceTypes = []string{common.MesosCPU}
)

// NewOfferPool creates a offerPool object and registers the
// corresponding YARPC procedures.
func NewOfferPool(
	offerHoldTime time.Duration,
	schedulerClient mpb.SchedulerClient,
	metrics *Metrics,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	scarceResourceTypes []string,
	slackResourceTypes []string,
	binPackingRanker binpacking.Ranker,
	hostPlacingOfferStatusTimeout time.Duration,
	processor watchevent.WatchProcessor,
	hostPoolManager manager.HostPoolManager) Pool {

	// GPU is only supported scarce resource type.
	if !reflect.DeepEqual(supportedScarceResourceTypes, scarceResourceTypes) {
		log.WithFields(log.Fields{
			"supported_scarce_resource_types": supportedScarceResourceTypes,
			"scarce_resource_types":           scarceResourceTypes,
		}).Error("input scarce resources types are not supported")
	}

	// cpus is only supported scarce resource type.
	if !reflect.DeepEqual(supportedSlackResourceTypes, slackResourceTypes) {
		log.WithFields(log.Fields{
			"supported_slack_resource_types": supportedSlackResourceTypes,
			"slack_resource_types":           slackResourceTypes,
		}).Error("input slack resources types are not supported")
	}

	p := &offerPool{
		hostOfferIndex: make(map[string]summary.HostSummary),

		scarceResourceTypes: scarceResourceTypes,
		slackResourceTypes:  slackResourceTypes,

		offerHoldTime:                 offerHoldTime,
		hostPlacingOfferStatusTimeout: hostPlacingOfferStatusTimeout,

		mSchedulerClient:           schedulerClient,
		mesosFrameworkInfoProvider: frameworkInfoProvider,

		metrics: metrics,

		binPackingRanker: binPackingRanker,

		watchProcessor: processor,

		hostPoolManager: hostPoolManager,
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

	// Inverse index from task to hostname. This map is required
	// because event stream does not have hostname.
	// key: taskID, value: hostname
	taskToHostMap sync.Map

	// scarce resource types, such as GPU.
	scarceResourceTypes []string

	// slack resource types, represents usage slace.
	// cpus is only supported slack resource.
	slackResourceTypes []string

	// Map from offer id to hostname and offer expiration time.
	// Used when offer is rescinded or pruned.
	timedOffers sync.Map

	// Time to hold offer in offer pool
	offerHoldTime time.Duration

	// Time to hold host in PLACING state
	hostPlacingOfferStatusTimeout time.Duration

	mSchedulerClient           mpb.SchedulerClient
	mesosFrameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider
	metrics                    *Metrics

	// The default ranker for hosts during filtering
	binPackingRanker binpacking.Ranker

	// taskHeldIndex --- key: task id,
	// value: host held for the task
	taskHeldIndex sync.Map

	watchProcessor watchevent.WatchProcessor

	hostPoolManager manager.HostPoolManager
}

// ClaimForPlace obtains offers from pool conforming to given constraints.
// Results are grouped by hostname as key. Here, offers are not cleared from
// the offer pool. First return value is returned offers, grouped by hostname
// as key, Second return value is a map from hostsvc.HostFilterResult to count.
func (p *offerPool) ClaimForPlace(
	ctx context.Context,
	hostFilter *hostsvc.HostFilter,
) (
	map[string]*summary.Offer,
	map[string]uint32,
	error) {
	p.RLock()
	defer p.RUnlock()

	matcher := NewMatcher(
		hostFilter,
		constraints.NewEvaluator(task.LabelConstraint_HOST),
		p.hostPoolManager)

	// if host hint is provided, try to return the hosts in hints first
	for _, filterHints := range hostFilter.GetHint().GetHostHint() {
		if hs, ok := p.hostOfferIndex[filterHints.GetHostname()]; ok {
			if result := matcher.tryMatch(hs); result != hostsvc.HostFilterResult_MATCH {
				log.WithField("task_id", filterHints.GetTaskID().GetValue()).
					WithField("hostname", hs.GetHostname()).
					WithField("match_result", result.String()).
					Info("failed to match task with desired host")
			}
			if matcher.HasEnoughHosts() {
				break
			}
		}
	}

	// We might want to consider making it a aynchronous process if
	// this becomes bottleneck, but that might increase the defragmentation
	// in the cluster, will start with this approach and monitor it based
	// on the results we will optimize this.
	var sortedSummaryList []interface{}
	if !matcher.HasEnoughHosts() {
		sortedSummaryList = p.getRankedHostSummaryList(
			ctx,
			hostFilter.GetHint().GetRankHint(),
			p.hostOfferIndex,
		)
	}

	for _, s := range sortedSummaryList {
		// if case the ordered list contains nil val
		if s != nil {
			matcher.tryMatch(s.(summary.HostSummary))
			if matcher.HasEnoughHosts() {
				break
			}
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

func (p *offerPool) getRankedHostSummaryList(
	ctx context.Context,
	rankHint hostsvc.FilterHint_Ranking,
	offerIndex map[string]summary.HostSummary,
) []interface{} {

	ranker := p.binPackingRanker
	switch rankHint {
	case hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM:
		// FirstFit ranker iterates over the offerIndex map and returns the
		// summary for each entry. We rely on Golang's randomized map
		// iteration order to get a random sequence of host summaries.
		ranker = binpacking.GetRankerByName(binpacking.FirstFit)

	case hostsvc.FilterHint_FILTER_HINT_RANKING_LEAST_AVAILABLE_FIRST:
		// DeFrag orders hosts from lowest offered resources to most
		// offered resources
		ranker = binpacking.GetRankerByName(binpacking.DeFrag)

	case hostsvc.FilterHint_FILTER_HINT_RANKING_LOAD_AWARE:
		// Load aware orders hosts from lowest loaded to highest loaded
		ranker = binpacking.GetRankerByName(binpacking.LoadAware)
	}
	return ranker.GetRankedHostList(ctx, offerIndex)
}

// ClaimForLaunch takes offers from pool (removes from hostsummary) for launch.
func (p *offerPool) ClaimForLaunch(
	hostname string,
	hostOfferID string,
	launchableTasks []*hostsvc.LaunchableTask,
	taskIDs ...*peloton.TaskID,
) (map[string]*mesos.Offer, error) {
	p.RLock()
	defer p.RUnlock()

	var offerMap map[string]*mesos.Offer
	var err error

	hs, ok := p.hostOfferIndex[hostname]
	if !ok {
		return nil, errors.New("cannot find input hostname " + hostname)
	}

	offerMap, err = hs.ClaimForLaunch(hostOfferID, launchableTasks, taskIDs...)

	if err != nil {
		return nil, err
	}

	if len(offerMap) == 0 {
		return nil, errors.New("no offer found to launch task on " + hostname)
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

	for _, taskID := range taskIDs {
		p.removeTaskHold(hostname, taskID)
	}

	for _, launchableTask := range launchableTasks {
		p.addTaskToHost(launchableTask.GetTaskId().GetValue(), hostname)
	}

	return offerMap, nil
}

// validateOfferUnavailability for incoming offer.
// Reject an offer if maintenance start time is less than current time.
// Reject an offer if current time is less than 3 hours to maintenance start time.
// Accept an offer if current time is more than 3 hours to maintenance start time.
func validateOfferUnavailability(offer *mesos.Offer) bool {
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
func (p *offerPool) AddOffers(
	ctx context.Context,
	offers []*mesos.Offer) []*mesos.Offer {
	var acceptableOffers []*mesos.Offer
	var unavailableOffers []*mesos.OfferID
	hostnameToOffers := make(map[string][]*mesos.Offer)

	for _, offer := range offers {
		if validateOfferUnavailability(offer) {
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
	p.metrics.UnavailableOffers.Inc(int64(len(unavailableOffers)))
	p.metrics.AcceptableOffers.Inc(int64(len(acceptableOffers)))

	// Decline unavailable offers.
	if len(unavailableOffers) > 0 {
		log.
			WithField("unavailable_offers", unavailableOffers).
			Debug("Offer unavailable due to maintenance on these hosts.")
		p.DeclineOffers(ctx, unavailableOffers)
	}
	log.
		WithField("acceptable_offers", acceptableOffers).
		Debug("Acceptable offers.")

	p.Lock()
	for hostname := range hostnameToOffers {
		p.addHostSummary(hostname)
	}
	p.Unlock()

	var wg sync.WaitGroup
	for hostname, offers := range hostnameToOffers {
		wg.Add(1)

		go func(hostname string, offers []*mesos.Offer) {
			defer wg.Done()

			p.RLock()
			p.hostOfferIndex[hostname].AddMesosOffers(ctx, offers)
			p.RUnlock()
		}(hostname, offers)
	}
	wg.Wait()

	return acceptableOffers
}

// addHostSummary is helper function to create HostSummary for
// provided hostname if not exists.
func (p *offerPool) addHostSummary(hostname string) {
	_, ok := p.hostOfferIndex[hostname]
	if !ok {
		hs := summary.New(
			p.scarceResourceTypes,
			hostname,
			p.slackResourceTypes,
			p.hostPlacingOfferStatusTimeout,
			p.watchProcessor)
		p.hostOfferIndex[hostname] = hs
	}
}

// UpdateTasksOnHost updates tasks assigned or running on a host.
// Task is assigned first time on recovery or placement.
// On receiving non-terminal event, task state is updated and on
// receiving terminal event, task is removed from the host.
func (p *offerPool) UpdateTasksOnHost(
	taskID string,
	taskState task.TaskState,
	taskInfo *task.TaskInfo) {
	p.Lock()
	defer p.Unlock()

	var hostname string
	// on recovery, taskinfo is non-nil
	if taskInfo != nil {
		hostname = taskInfo.GetRuntime().GetHost()
		if len(hostname) == 0 || util.IsPelotonStateTerminal(taskState) {
			return
		}

		p.addHostSummary(hostname)
		p.addTaskToHost(taskID, hostname)
	} else {
		// on receving mesos task status update event else is evaluated.
		value, ok := p.taskToHostMap.Load(taskID)
		if !ok {
			return
		}
		hostname = value.(string)
	}

	p.hostOfferIndex[hostname].UpdateTasksOnHost(taskID, taskState, taskInfo)
	if util.IsPelotonStateTerminal(taskState) {
		p.removeTaskToHost(taskID)
	}
}

// removeOffer is a helper method to remove an offer from timedOffers and
// hostSummary.
func (p *offerPool) removeOffer(offerID, reason string) {
	offer, ok := p.timedOffers.Load(offerID)
	if !ok {
		log.
			WithField("offer_id", offerID).
			Info("offer not found in pool.")
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
	}
}

// RescindOffer is a callback event when Mesos Master rescinds a offer.
// Reasons for offer rescind can be lost agent, offer_timeout and more.
func (p *offerPool) RescindOffer(offerID *mesos.OfferID) bool {
	p.RLock()
	defer p.RUnlock()

	oID := *offerID.Value
	p.metrics.RescindEvents.Inc(1)
	p.removeOffer(oID, "offer is rescinded.")
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
			log.
				WithField("offer_id", offerID).
				Info("Removing expired offer from pool.")
			offersToDecline[offerID.(string)] = timedOffer.(*TimedOffer)
		}
		return true
	})

	// Remove the expired offers from hostOfferIndex
	if len(offersToDecline) > 0 {
		p.metrics.ExpiredOffers.Inc(int64(len(offersToDecline)))
		for offerID := range offersToDecline {
			p.removeOffer(offerID, "offer is expired.")
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
}

// DeclineOffers calls mesos master to decline list of offers
func (p *offerPool) DeclineOffers(
	ctx context.Context,
	offerIDs []*mesos.OfferID) error {
	p.RLock()
	defer p.RUnlock()

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

	p.metrics.Decline.Inc(int64(len(offerIDs)))
	for _, offerID := range offerIDs {
		p.removeOffer(*offerID.Value, "offer is declined")
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

	err := hostOffers.ReturnPlacingHost()
	if err != nil {
		return err
	}
	p.metrics.ReturnUnusedHosts.Inc(1)

	return nil
}

// ResetExpiredPlacingHostSummaries resets the status of each hostSummary of the
// offerPool from PlacingOffer to ReadyOffer if the PlacingOffer status has
// expired and returns the hostnames which got reset
func (p *offerPool) ResetExpiredPlacingHostSummaries(now time.Time) []string {
	p.RLock()
	defer p.RUnlock()
	var resetHostnames []string
	for hostname, summ := range p.hostOfferIndex {
		if reset, res, taskExpired := summ.ResetExpiredPlacingOfferStatus(now); reset {
			resetHostnames = append(resetHostnames, hostname)
			for _, task := range taskExpired {
				p.removeTaskHold(hostname, task)
			}
			p.metrics.ResetExpiredPlacingHosts.Inc(1)
			log.WithFields(log.Fields{
				"host":    hostname,
				"summary": summ,
				"delta":   res,
			}).Info("reset expired host summaries in PLACING state.")
		}
	}
	return resetHostnames
}

// ResetExpiredHeldHostSummaries resets the status of each hostSummary of the
// offerPool from HeldHost to ReadyOffer if the PlacingOffer status has
// expired and returns the hostnames which got reset
func (p *offerPool) ResetExpiredHeldHostSummaries(now time.Time) []string {
	p.RLock()
	defer p.RUnlock()
	var resetHostnames []string
	for hostname, summ := range p.hostOfferIndex {
		reset, res, taskExpired := summ.ResetExpiredHostHeldStatus(now)
		if reset {
			resetHostnames = append(resetHostnames, hostname)
			p.metrics.ResetExpiredHeldHosts.Inc(1)
			log.WithFields(log.Fields{
				"host":    hostname,
				"summary": summ,
				"delta":   res,
			}).Info("reset expired host summaries in HELD state.")
		}
		for _, task := range taskExpired {
			p.removeTaskHold(hostname, task)
		}
	}
	return resetHostnames
}

// GetAllOffers returns all hostOffers in the pool as:
// map[hostname] -> map(offerid -> offers
// and #offers for reserved, unreserved or all offer types.
func (p *offerPool) GetAllOffers() (map[string]map[string]*mesos.Offer, int) {
	p.RLock()
	defer p.RUnlock()

	hostOffers := make(map[string]map[string]*mesos.Offer)
	var offersCount int
	for hostname, s := range p.hostOfferIndex {
		offers := s.GetOffers(summary.All)
		hostOffers[hostname] = offers
		offersCount += len(offers)
	}

	return hostOffers, offersCount
}

// RefreshGaugeMaps refreshes the metrics for hosts in ready and placing state.
func (p *offerPool) RefreshGaugeMaps() {
	p.RLock()
	defer p.RUnlock()

	ready := scalar.Resources{}
	readyRevocable := scalar.Resources{}
	readyHosts := float64(0)

	placing := scalar.Resources{}
	placingRevocable := scalar.Resources{}
	placingHosts := float64(0)

	for _, h := range p.hostOfferIndex {
		nonRevocableAmount, revocableAmount, status := h.UnreservedAmount()
		switch status {
		case summary.ReadyHost:
			ready = ready.Add(nonRevocableAmount)
			readyRevocable = readyRevocable.Add(revocableAmount)
			readyHosts++
		case summary.PlacingHost:
			placing = placing.Add(nonRevocableAmount)
			placingRevocable = placingRevocable.Add(revocableAmount)
			placingHosts++
		}
	}

	p.metrics.Ready.Update(ready)
	p.metrics.ReadyRevocable.Update(readyRevocable)
	p.metrics.ReadyHosts.Update(readyHosts)

	p.metrics.Placing.Update(placing)
	p.metrics.PlacingRevocable.Update(placingRevocable)
	p.metrics.PlacingHosts.Update(placingHosts)

	p.metrics.AvailableHosts.Update(readyHosts + placingHosts)
}

// GetHostSummary returns the host summary object for the given host name
func (p *offerPool) GetHostSummary(hostname string) (summary.HostSummary, error) {
	p.RLock()
	defer p.RUnlock()
	if _, ok := p.hostOfferIndex[hostname]; !ok {
		return nil, errors.Errorf("hostname %s does not have any offers",
			hostname)
	}
	return p.hostOfferIndex[hostname], nil
}

// GetBinPackingRanker returns the associated ranker with the offer pool
func (p *offerPool) GetBinPackingRanker() binpacking.Ranker {
	return p.binPackingRanker
}

// GetHostOfferIndex returns the host to host summary mapping
// it makes the copy and returns the new map
func (p *offerPool) GetHostOfferIndex() map[string]summary.HostSummary {
	p.RLock()
	defer p.RUnlock()
	dest := make(map[string]summary.HostSummary)
	for k, v := range p.hostOfferIndex {
		dest[k] = v
	}
	return dest
}

// GetHostSummaries returns a map of hostname to host summary object
func (p *offerPool) GetHostSummaries(
	hostnames []string) (map[string]summary.HostSummary, error) {
	p.RLock()
	defer p.RUnlock()

	hostSummaries := make(map[string]summary.HostSummary)

	if len(hostnames) > 0 {
		for _, hostname := range hostnames {
			if offerSummary, ok := p.hostOfferIndex[hostname]; ok {
				hostSummaries[hostname] = offerSummary
			}
		}
	} else {
		for hostname, offerSummary := range p.hostOfferIndex {
			hostSummaries[hostname] = offerSummary
		}
	}

	return hostSummaries, nil
}

// GetHostHeldForTask returns the host that is held for the task
func (p *offerPool) GetHostHeldForTask(taskID *peloton.TaskID) string {
	val, ok := p.taskHeldIndex.Load(taskID.GetValue())
	if !ok {
		return ""
	}

	return val.(string)
}

// HoldForTasks holds the host for the task specified
func (p *offerPool) HoldForTasks(hostname string, taskIDs []*peloton.TaskID) error {
	hs, err := p.GetHostSummary(hostname)
	if err != nil {
		return err
	}

	var errs []error
	for _, taskID := range taskIDs {
		if err := hs.HoldForTask(taskID); err != nil {
			errs = append(errs, err)
		} else {
			p.addTaskHold(hostname, taskID)
		}
	}

	if len(errs) != 0 {
		return multierr.Combine(errs...)
	}

	return nil
}

// ReleaseHoldForTasks release the hold of host for the tasks specified
func (p *offerPool) ReleaseHoldForTasks(hostname string, taskIDs []*peloton.TaskID) error {
	hs, err := p.GetHostSummary(hostname)
	if err != nil {
		return err
	}

	var errs []error
	for _, taskID := range taskIDs {
		if err := hs.ReleaseHoldForTask(taskID); err != nil {
			errs = append(errs, err)
		} else {
			p.removeTaskHold(hostname, taskID)
		}
	}

	if len(errs) != 0 {
		return multierr.Combine(errs...)
	}

	return nil
}

// SetHostPoolManager set host pool manager in the offer pool.
func (p *offerPool) SetHostPoolManager(manager manager.HostPoolManager) {
	p.hostPoolManager = manager
}

// addTaskHold update the index when a host is held for a task
func (p *offerPool) addTaskHold(hostname string, id *peloton.TaskID) {
	oldHost, loaded := p.taskHeldIndex.LoadOrStore(id.GetValue(), hostname)
	if loaded && oldHost != hostname {
		log.WithFields(log.Fields{
			"new_host": hostname,
			"old_host": oldHost.(string),
			"task_id":  id.GetValue(),
		}).Warn("task is held by multiple hosts")
		p.taskHeldIndex.Store(id.GetValue(), hostname)
	}
}

// removeTaskHold update the index when a host is removed from
// held for a task
func (p *offerPool) removeTaskHold(hostname string, id *peloton.TaskID) {
	p.taskHeldIndex.Delete(id.GetValue())
}

func (p *offerPool) addTaskToHost(taskID, hostname string) {
	p.taskToHostMap.LoadOrStore(taskID, hostname)
}

func (p *offerPool) removeTaskToHost(taskID string) {
	p.taskToHostMap.Delete(taskID)
}
