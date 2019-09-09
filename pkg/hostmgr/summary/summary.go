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

package summary

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	halphapb "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

const (
	unreservedRole = "*"
)

// Offer represents an offer sent from the host summary when the host is
// moved to PLACING state.
//
// Evey offer sent has a unique ID which is used to to
// claim the offer for launching a task.
// The ID is reset when the host moves to READY state.
//
// The ID is not persisted and is reset when a hostmanager restarts,
// remaining in flight tasks will have to placed again and generate new ID
// for the offers.
type Offer struct {
	ID     string
	Offers []*mesos.Offer
}

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

const (
	// ReadyHost represents an host ready to be used.
	ReadyHost HostStatus = iota + 1
	// PlacingHost represents an host being used by placement engine.
	PlacingHost
	// ReservedHost represents an host is reserved for tasks
	ReservedHost
	// HeldHost represents a host is held for tasks, which is used for in-place update
	HeldHost
)

// OfferType represents the type of offer in the host summary such as reserved, unreserved, or All.
type OfferType int

const (
	// Reserved offer type, represents an offer reserved for a particular Mesos Role.
	Reserved OfferType = iota + 1
	// Unreserved offer type, is not reserved for any Mesos role, and can be used to launch task for
	// any role if framework has opt-in MULTI_ROLE capability.
	Unreserved
	// All represents reserved and unreserved offers.
	All
)

const (
	// hostHeldHostStatusTimeout is a timeout for resetting
	// HeldHost status back to ReadyHost status.
	hostHeldStatusTimeout = 3 * time.Minute
	// emptyOfferID is used when the host is in READY state.
	emptyOfferID = ""
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

	// GetTasks returns tasks placed or running on this host.
	GetTasks() []*mesos.TaskID

	// UpdateTasksOnHost updates the state of task placed or running on this host.
	// If task state is terminal then remove from HostToTaskMap.
	UpdateTasksOnHost(taskID string, taskState task.TaskState, taskInfo *task.TaskInfo)

	// TryMatch atomically tries to match offers from the current host with
	// given constraint.
	TryMatch(
		hostFilter *hostsvc.HostFilter,
		evaluator constraints.Evaluator,
		labelValues constraints.LabelValues) Match

	// AddMesosOffer adds a Mesos offers to the current HostSummary.
	AddMesosOffers(ctx context.Context, offer []*mesos.Offer) HostStatus

	// RemoveMesosOffer removes the given Mesos offer by its id, and returns
	// CacheStatus and possibly removed offer for tracking purpose.
	RemoveMesosOffer(offerID, reason string) (HostStatus, *mesos.Offer)

	// ClaimForLaunch releases unreserved offers for task launch.
	// An optional list of task ids is provided if the host is held for
	// the tasks
	ClaimForLaunch(
		hostOfferID string,
		launchableTasks []*hostsvc.LaunchableTask,
		taskIDs ...*peloton.TaskID) (map[string]*mesos.Offer, error)

	// CasStatus atomically sets the status to new value if current value is old,
	// otherwise returns error.
	CasStatus(old, new HostStatus) error

	// UnreservedAmount returns unreserved non-revocable and revocable resources
	// and current host status
	UnreservedAmount() (scalar.Resources, scalar.Resources, HostStatus)

	// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
	// if the PlacingOffer status has expired, and returns
	// whether the hostSummary got reset, resources amount for unreserved offers and
	// the tasks held released due to the expiration
	ResetExpiredPlacingOfferStatus(now time.Time) (bool, scalar.Resources, []*peloton.TaskID)

	// ResetExpiredHostHeldStatus resets a hostSummary status from HeldHost
	// if the HeldHost status has expired, and returns
	// whether the hostSummary got reset, resources amount for unreserved offers and
	// the tasks held released due to the expiration
	ResetExpiredHostHeldStatus(now time.Time) (bool, scalar.Resources, []*peloton.TaskID)

	// GetOffers returns offers and #offers present for this host, of type reserved, unreserved or all.
	// Returns map of offerid -> offer
	GetOffers(OfferType) map[string]*mesos.Offer

	// GetHostname returns the hostname of the host
	GetHostname() string

	// GetHostStatus returns the HostStatus of the host
	GetHostStatus() HostStatus

	// GetHostOfferID returns the hostOfferID of the host
	GetHostOfferID() string

	// HoldForTasks holds the host for the task specified.
	// If an error is returned, hostsummary would guarantee that
	// the host is not on held for the task
	HoldForTask(id *peloton.TaskID) error

	// ReleaseHoldForTasks release the hold of host for the task specified.
	// If an error is returned, hostsummary would guarantee that
	// the host is not on released for the task
	ReleaseHoldForTask(id *peloton.TaskID) error

	// ReturnPlacingHost is called when the host in PLACING state is not used,
	// and is returned by placement engine
	ReturnPlacingHost() error

	// GetHeldTask returns a slice of task that puts the host in held
	GetHeldTask() []*peloton.TaskID
}

type offerIDgenerator func() string

// returns a new random (version 4) UUID as a string
func uuidOfferID() string {
	return uuid.New()
}

// hostSummary is a data struct holding offers on a particular host.
type hostSummary struct {
	sync.Mutex

	// hostname of the host
	hostname string

	// scarceResourceTypes are resources, which are exclusively reserved for
	// specific task requirements, and to prevent every task to schedule on
	// those hosts such as GPU.
	scarceResourceTypes []string

	// Usage slack is resources that are allocated but not completely used.
	// As of now, cpus is only resource type supported as slack.
	slackResourceTypes []string

	// mesos offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
	// mesos offerID -> reserved offer
	reservedOffers map[string]*mesos.Offer

	status                        HostStatus
	statusPlacingOfferExpiration  time.Time
	hostPlacingOfferStatusTimeout time.Duration

	// When the host has been matched for placing i.e.
	// the host status is PLACING or RESERVED a unique host offer ID is
	// generated.
	// For all other host states the host offer ID is empty.
	hostOfferID      string
	offerIDgenerator offerIDgenerator

	readyCount atomic.Int32

	// a map to present for which tasks the host is held,
	// key is the task id, value is the expiration time
	// of the hold
	heldTasks map[string]time.Time

	// a map to present tasks assigned or running on this host
	// key is the mesos tasks id and value is the current task state
	tasks map[string]*task.TaskInfo

	// watchProcessor
	watchProcessor watchevent.WatchProcessor
}

// New returns a zero initialized hostSummary
func New(
	scarceResourceTypes []string,
	hostname string,
	slackResourceTypes []string,
	hostPlacingOfferStatusTimeout time.Duration,
	processor watchevent.WatchProcessor,
) HostSummary {
	return &hostSummary{
		unreservedOffers:    make(map[string]*mesos.Offer),
		reservedOffers:      make(map[string]*mesos.Offer),
		heldTasks:           make(map[string]time.Time),
		tasks:               make(map[string]*task.TaskInfo),
		scarceResourceTypes: scarceResourceTypes,
		slackResourceTypes:  slackResourceTypes,

		hostPlacingOfferStatusTimeout: hostPlacingOfferStatusTimeout,

		status: ReadyHost,

		hostname: hostname,

		offerIDgenerator: uuidOfferID,

		watchProcessor: processor,
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
	return len(a.unreservedOffers) > 0
}

// GetTasks returns tasks placed or running on this host.
func (a *hostSummary) GetTasks() []*mesos.TaskID {
	a.Lock()
	defer a.Unlock()

	var result []*mesos.TaskID
	for taskID := range a.tasks {
		t := taskID
		result = append(result, &mesos.TaskID{Value: &t})
	}

	return result
}

// UpdateTasksOnHost updates the list of tasks on this host.
// It can perform one of following actions
// - Add/Update the status of task based on recovery or eventstream.
// - Remove the task from the list if task state is terminal.
func (a *hostSummary) UpdateTasksOnHost(
	taskID string,
	taskState task.TaskState,
	taskInfo *task.TaskInfo) {
	a.Lock()
	defer a.Unlock()

	switch {
	case util.IsPelotonStateTerminal(taskState):
		delete(a.tasks, taskID)
	case taskInfo != nil:
		a.tasks[taskID] = taskInfo
	default:
		taskInfo, ok := a.tasks[taskID]
		if !ok {
			return
		}

		taskInfo.Runtime.State = taskState
	}
}

// Match represents the result of a match
type Match struct {
	// The result of the match
	Result hostsvc.HostFilterResult
	// Offer if the match is successful
	Offer *Offer
}

// isHostLimitConstraintSatisfy validates task to task affinity constraint.
// It limits number of tasks for same service to run on same host.
func (a *hostSummary) isHostLimitConstraintSatisfy(
	labelConstraint *task.LabelConstraint,
) bool {

	// Aggregates label count for all tasks running on this host.
	// label_key -> label_value -> count
	labelsCount := make(map[string]map[string]uint32)
	for _, task := range a.tasks {
		labels := task.GetConfig().GetLabels()

		for _, label := range labels {
			key := label.GetKey()
			value := label.GetValue()

			if _, ok := labelsCount[key]; !ok {
				labelsCount[key] = map[string]uint32{value: 1}
				continue
			}

			labelsCount[key][value] = labelsCount[key][value] + 1
		}
	}

	// Checks whether label constraint requirement is met or not.
	// requirement = 1 && zero task running -> satisfies the requirement
	// requirement = 1 && one task running -> does not satisfy requirement
	// requirement = 2 && one task running -> satisfies the requirement
	label := labelConstraint.GetLabel()
	if labelCountValue, ok := labelsCount[label.GetKey()]; ok {
		if labelCountValue[label.GetValue()] >= labelConstraint.GetRequirement() {
			return false
		}
	}

	return true
}

// matchHostFilter determines whether given HostFilter matches
// the given map of offers.
func matchHostFilter(
	offerMap map[string]*mesos.Offer,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	labelValues constraints.LabelValues,
	scalarAgentRes scalar.Resources,
	scarceResourceTypes []string) hostsvc.HostFilterResult {

	if len(offerMap) == 0 {
		return hostsvc.HostFilterResult_NO_OFFER
	}

	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		scalarRes := scalar.FromOfferMap(offerMap)

		if c.GetResourceConstraint().GetRevocable() {
			revocable, _ := scalar.FilterRevocableMesosResources(
				scalar.FromOffersMapToMesosResources(offerMap))
			// ToDo: Implement clean design to not hard code cpus
			scalarRevocable := scalar.FromMesosResources(revocable)
			scalarRes.CPU = scalarRevocable.CPU
		} else {
			_, nonRevocable := scalar.FilterRevocableMesosResources(
				scalar.FromOffersMapToMesosResources(offerMap))
			scalarRes = scalar.FromMesosResources(nonRevocable)
		}

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

	hostname := firstOffer.GetHostname()
	hc := c.GetSchedulingConstraint()

	return hmutil.MatchSchedulingConstraint(
		hostname, labelValues, firstOffer.GetAttributes(), hc, evaluator)
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
	evaluator constraints.Evaluator,
	labelValues constraints.LabelValues) Match {
	a.Lock()
	defer a.Unlock()

	if a.status != ReadyHost && a.status != HeldHost {
		return Match{
			Result: hostsvc.HostFilterResult_MISMATCH_STATUS,
		}
	}

	if !a.HasOffer() {
		return Match{
			Result: hostsvc.HostFilterResult_NO_OFFER,
		}
	}

	// for host in Held state, it is only a match if the filter
	// hint contains the host
	if a.status == HeldHost {
		var hintFound bool
		for _, hostHint := range filter.GetHint().GetHostHint() {
			if hostHint.GetHostname() == a.hostname {
				hintFound = true
				break
			}
		}

		if !hintFound {
			return Match{
				Result: hostsvc.HostFilterResult_MISMATCH_STATUS,
			}
		}
	}

	// Validates task affinity constraint for stateless workload
	constraint := filter.GetSchedulingConstraint()
	if constraint.GetType() == task.Constraint_LABEL_CONSTRAINT {
		if !a.isHostLimitConstraintSatisfy(constraint.GetLabelConstraint()) {
			return Match{Result: hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS}
		}
	}

	result := matchHostFilter(
		a.unreservedOffers,
		filter,
		evaluator,
		labelValues,
		scalar.FromMesosResources(host.GetAgentInfo(a.GetHostname()).GetResources()),
		a.scarceResourceTypes)

	if result != hostsvc.HostFilterResult_MATCH {
		return Match{Result: result}
	}

	// Its a match!
	var offers []*mesos.Offer
	for _, o := range a.unreservedOffers {
		offer := proto.Clone(o).(*mesos.Offer)

		if filter.GetResourceConstraint().GetRevocable() {
			offer.Resources, _ = scalar.FilterMesosResources(
				offer.GetResources(),
				func(r *mesos.Resource) bool {
					if r.GetRevocable() != nil {
						return true
					}
					return !hmutil.IsSlackResourceType(r.GetName(), a.slackResourceTypes)
				})
		} else {
			_, offer.Resources = scalar.FilterRevocableMesosResources(offer.GetResources())
		}
		offers = append(offers, offer)
	}

	// Setting status to `PlacingHost`: this ensures proper state
	// tracking of resources on the host and also ensures offers on
	// this host will not be sent to another `AcquireHostOffers`
	// call before released.
	err := a.casStatusLockFree(a.status, PlacingHost)
	if err != nil {
		return Match{
			Result: hostsvc.HostFilterResult_NO_OFFER,
		}
	}

	// Add offer to the match
	return Match{
		Result: hostsvc.HostFilterResult_MATCH,
		Offer: &Offer{
			ID:     a.hostOfferID,
			Offers: offers,
		},
	}
}

// hasLabeledReservedResources returns if given offer has labeled
// reserved resources.
func hasLabeledReservedResources(offer *mesos.Offer) bool {
	for _, res := range offer.GetResources() {
		if res.GetRole() != "" &&
			res.GetRole() != unreservedRole &&
			res.GetReservation().GetLabels() != nil {
			return true
		}
	}
	return false
}

// AddMesosOffers adds a Mesos offers to the current hostSummary and returns
// its status for tracking purpose.
func (a *hostSummary) AddMesosOffers(
	ctx context.Context,
	offers []*mesos.Offer) HostStatus {
	a.Lock()
	defer a.Unlock()

	var offerIDs []string
	for _, offer := range offers {
		// filter out revocable resources whose type we don't recognize
		offerID := offer.GetId().GetValue()
		offer.Resources, _ = scalar.FilterMesosResources(
			offer.Resources,
			func(r *mesos.Resource) bool {
				if r.GetRevocable() == nil {
					return true
				}
				return hmutil.IsSlackResourceType(r.GetName(), a.slackResourceTypes)
			})
		if !hasLabeledReservedResources(offer) {
			a.unreservedOffers[offerID] = offer
		} else {
			a.reservedOffers[offerID] = offer
		}

		offerIDs = append(offerIDs, offer.GetId().GetValue())
	}

	if a.status == ReadyHost || a.status == HeldHost {
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	}

	log.WithFields(log.Fields{
		"offer_ids":               strings.Join(offerIDs, ","),
		"outstanding_offer_count": len(a.unreservedOffers),
		"hostname":                a.GetHostname(),
		"host_state":              a.status,
	}).Info("offers added to host summary")

	return a.status
}

// ClaimForLaunch atomically check that current hostSummary is in Placing
// status, release offers so caller can use them to launch tasks, and reset
// status to ready.
func (a *hostSummary) ClaimForLaunch(
	hostOfferID string,
	launchableTasks []*hostsvc.LaunchableTask,
	taskIDs ...*peloton.TaskID) (map[string]*mesos.Offer,
	error) {
	a.Lock()
	defer a.Unlock()

	if a.status != PlacingHost {
		return nil, errors.New("host status is not Placing")
	}

	if a.hostOfferID != hostOfferID {
		return nil, errors.New("host offer id does not match")
	}

	result := make(map[string]*mesos.Offer)
	result, a.unreservedOffers = a.unreservedOffers, result

	for _, t := range launchableTasks {
		taskID := t.GetTaskId().GetValue()
		a.tasks[taskID] = &task.TaskInfo{
			Config: t.GetConfig(),
			Runtime: &task.RuntimeInfo{
				State:     task.TaskState_LAUNCHED,
				StartTime: time.Now().Format(time.RFC3339Nano),
			},
		}
	}

	for _, taskID := range taskIDs {
		a.releaseHoldForTaskLockFree(taskID)
	}

	newState := a.getResetStatus()
	// Reset status to held/ready depending on if the host is held for
	// other tasks.
	if err := a.casStatusLockFree(PlacingHost, newState); err != nil {
		return nil, errors.Wrap(err,
			"failed to move host to Ready state")
	}
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
			"offer":    offerID,
			"hostname": a.GetHostname(),
			"reason":   reason,
		}).Info("Ready offer removed")
	default:
		// This could trigger INVALID_OFFER error later.
		log.WithFields(log.Fields{
			"offer_id":                  offer.Id.GetValue(),
			"resources_removed":         scalar.FromOffer(offer),
			"unreserved_resources_left": scalar.FromOfferMap(a.unreservedOffers),
			"status":                    a.status,
			"reason":                    reason,
		}).Warn("offer removed while not in ready status")
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

// Notify the client whenever there is a change in host summary object
func (a *hostSummary) notifyEvent() {
	a.watchProcessor.NotifyEventChange(a.createHostSummaryObject())
}

// casStatus atomically and lock-freely sets the status to new value
// if current value is old, otherwise returns error. This should wrapped
// around locking
func (a *hostSummary) casStatusLockFree(old, new HostStatus) error {
	defer a.notifyEvent()

	if a.status != old {
		return InvalidHostStatus{a.status}
	}
	a.status = new

	switch a.status {
	case ReadyHost:
		// if its a ready host then reset the hostOfferID
		a.hostOfferID = emptyOfferID
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	case PlacingHost:
		// generate the offer id for a placing host.
		a.hostOfferID = a.offerIDgenerator()
		a.statusPlacingOfferExpiration = time.Now().Add(a.hostPlacingOfferStatusTimeout)
		a.readyCount.Store(0)
	case ReservedHost:
		// generate the offer id for a placing host.
		a.hostOfferID = a.offerIDgenerator()
		a.readyCount.Store(0)
	case HeldHost:
		a.hostOfferID = emptyOfferID
		a.readyCount.Store(int32(len(a.unreservedOffers)))
	}
	return nil
}

// UnreservedAmount returns unreserved non-revocable and revocable resources
// and current host status
func (a *hostSummary) UnreservedAmount() (scalar.Resources, scalar.Resources, HostStatus) {
	a.Lock()
	defer a.Unlock()

	unreservedResources := scalar.FromOffersMapToMesosResources(a.unreservedOffers)
	revocable, nonRevocable := scalar.FilterRevocableMesosResources(unreservedResources)

	return scalar.FromMesosResources(nonRevocable),
		scalar.FromMesosResources(revocable),
		a.status
}

// ResetExpiredPlacingOfferStatus resets a hostSummary status from PlacingOffer
// to ReadyOffer if the PlacingOffer status has expired, and returns
// whether the hostSummary got reset to READY/HELD
func (a *hostSummary) ResetExpiredPlacingOfferStatus(now time.Time) (bool, scalar.Resources, []*peloton.TaskID) {
	a.Lock()
	defer a.Unlock()

	var taskExpired []*peloton.TaskID

	if !a.HasOffer() &&
		a.status == PlacingHost &&
		now.After(a.statusPlacingOfferExpiration) {

		// some tasks held on the host may expire during the time,
		// expire the tasks and calculate the new status
		taskExpired = a.releaseExpiredHeldTask(now)

		newStatus := a.getResetStatus()

		log.WithFields(log.Fields{
			"time":            now,
			"current_status":  a.status,
			"new_status":      newStatus,
			"ready_count":     a.readyCount.Load(),
			"offer_resources": scalar.FromOfferMap(a.unreservedOffers),
			"task_expired":    taskExpired,
		}).Warn("reset host from placing state after timeout")

		a.casStatusLockFree(PlacingHost, newStatus)
		return true, scalar.FromOfferMap(a.unreservedOffers), taskExpired
	}

	return false, scalar.Resources{}, taskExpired
}

// ResetExpiredHostHeldStatus resets a hostSummary status from HeldHost
// to ReadyHost if the HeldHost status has expired, and returns
// whether the hostSummary got reset to READY/HELD
func (a *hostSummary) ResetExpiredHostHeldStatus(now time.Time) (bool, scalar.Resources, []*peloton.TaskID) {
	a.Lock()
	defer a.Unlock()

	var taskExpired []*peloton.TaskID

	taskExpired = a.releaseExpiredHeldTask(now)

	// keep the host status if it is in PLACING/Reserved
	if len(taskExpired) != 0 && a.status == HeldHost {
		newStatus := a.getResetStatus()

		log.WithFields(log.Fields{
			"time":            now,
			"current_status":  a.status,
			"new_status":      newStatus,
			"ready_count":     a.readyCount.Load(),
			"offer_resources": scalar.FromOfferMap(a.unreservedOffers),
			"task_expired":    taskExpired,
		}).Warn("remove expired task for host in held status after timeout")

		a.casStatusLockFree(HeldHost, newStatus)

		// if host is still in held state, then no resource is freed
		res := scalar.Resources{}
		if newStatus != HeldHost {
			res = scalar.FromOfferMap(a.unreservedOffers)
		}
		return true, res, taskExpired
	}

	// reset happens, but host state is not changed,
	// so no res is released
	if len(taskExpired) != 0 {
		return true, scalar.Resources{}, taskExpired
	}

	return false, scalar.Resources{}, taskExpired
}

func (a *hostSummary) releaseExpiredHeldTask(now time.Time) []*peloton.TaskID {
	var taskExpired []*peloton.TaskID

	for taskID, expirationTime := range a.heldTasks {
		if now.After(expirationTime) {
			a.releaseHoldForTaskLockFree(&peloton.TaskID{Value: taskID})
			taskExpired = append(taskExpired, &peloton.TaskID{Value: taskID})
		}
	}

	return taskExpired
}

// GetOffers returns offers, and #offers present for this host, of type reserved, unreserved or all.
// Returns map of offerid -> offer
func (a *hostSummary) GetOffers(offertype OfferType) map[string]*mesos.Offer {
	a.Lock()
	defer a.Unlock()
	return a.getOffers(offertype)
}

// getOffers is a unprotected method
// returns offers, and #offers present for this host, of type
// reserved, unreserved or all.
// Returns map of offerid -> offer
func (a *hostSummary) getOffers(offertype OfferType) map[string]*mesos.Offer {
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
		offers = a.getOffers(Unreserved)
		reservedOffers := a.getOffers(Reserved)
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

// GetHostOfferID returns the hostOffID of the host
func (a *hostSummary) GetHostOfferID() string {
	return a.hostOfferID
}

// HoldForTasks holds the host for the task specified
func (a *hostSummary) HoldForTask(id *peloton.TaskID) error {
	a.Lock()
	defer a.Unlock()

	if a.status == ReservedHost {
		return errors.Wrap(&InvalidHostStatus{status: a.status},
			"cannot change host state to Held")
	}

	if a.status == ReadyHost {
		if err := a.casStatusLockFree(ReadyHost, HeldHost); err != nil {
			return err
		}
	}
	// for PLACING and HELD, state no need to change host state
	if _, ok := a.heldTasks[id.GetValue()]; !ok {
		a.heldTasks[id.GetValue()] = time.Now().Add(hostHeldStatusTimeout)
	}

	log.WithFields(log.Fields{
		"hostname":  a.hostname,
		"status":    a.status,
		"host_held": a.heldTasks,
		"task_id":   id.GetValue(),
	}).Debug("hold task")

	return nil
}

// GetHeldTask returns a slice of task that puts the host in held
func (a *hostSummary) GetHeldTask() []*peloton.TaskID {
	a.Lock()
	defer a.Unlock()

	var result []*peloton.TaskID
	for taskID := range a.heldTasks {
		result = append(result, &peloton.TaskID{Value: taskID})
	}

	return result
}

// ReleaseHoldForTasks release the hold of host for the task specified
func (a *hostSummary) ReleaseHoldForTask(id *peloton.TaskID) error {
	a.Lock()
	defer a.Unlock()

	// try to reset the host status iff the task to be released
	// is the only task left in held
	if len(a.heldTasks) == 1 && a.status == HeldHost {
		if _, ok := a.heldTasks[id.GetValue()]; ok {
			log.WithFields(log.Fields{
				"hostname": a.hostname,
				"task_id":  id,
			}).Debug("host is ready after hold release")
			if err := a.casStatusLockFree(a.status, ReadyHost); err != nil {
				return err
			}
		}
	}

	a.releaseHoldForTaskLockFree(id)

	return nil
}

func (a *hostSummary) releaseHoldForTaskLockFree(id *peloton.TaskID) {
	if _, exist := a.heldTasks[id.GetValue()]; !exist {
		// this can happen for various reasons such as
		// a task is launched again on the same host after timeout
		log.WithFields(log.Fields{
			"hostname": a.hostname,
			"task_id":  id.GetValue(),
		}).Info("task held is not found on host")
	} else {
		delete(a.heldTasks, id.GetValue())
	}

	log.WithFields(log.Fields{
		"hostname":  a.hostname,
		"status":    a.status,
		"host_held": a.heldTasks,
		"task_id":   id.GetValue(),
	}).Debug("release task")
}

// ReturnPlacingHost is called when the host in PLACING state is not used,
// and is returned by placement engine
func (a *hostSummary) ReturnPlacingHost() error {
	a.Lock()
	defer a.Unlock()

	if a.status != PlacingHost {
		return &InvalidHostStatus{status: a.status}
	}

	newStatus := a.getResetStatus()

	log.WithFields(log.Fields{
		"new_status": newStatus,
		"held_tasks": a.heldTasks,
	}).Debug("return placing hosts")

	return a.casStatusLockFree(PlacingHost, newStatus)
}

// getResetStatus returns the new host status for a host
// that is going to be reset from PLACING/HELD state.
func (a *hostSummary) getResetStatus() HostStatus {
	newStatus := ReadyHost
	if len(a.heldTasks) != 0 {
		newStatus = HeldHost
	}

	return newStatus
}

// create host summary object
func (a *hostSummary) createHostSummaryObject() *halphapb.HostSummary {
	obj := &halphapb.HostSummary{
		Hostname: a.GetHostname(),
		Offers:   a.unreservedOffers,
	}
	return obj
}
