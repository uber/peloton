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

package hostcache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/yarpc/yarpcerrors"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/pkg/common/v1alpha/constraints"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// HostStatus represents status (Ready/Placing/Reserved/Held) of the host in
// host cache
type HostStatus int

const (
	// ReadyHost represents a host ready to be used.
	ReadyHost HostStatus = iota + 1

	// PlacingHost represents a host being used by placement engine.
	PlacingHost

	// ReservedHost represents a host that is reserved for tasks.
	ReservedHost

	// HeldHost represents a host hat is held for tasks, which is used for
	// in-place update.
	HeldHost
)

const (
	// hostHeldHostStatusTimeout is a timeout for resetting.
	// HeldHost status back to ReadyHost status.
	// TODO: Make this configurable (T3312219).
	hostHeldStatusTimeout = 3 * time.Minute

	// emptyLeaseID is used when the host is in READY state.
	emptyLeaseID = ""
)

type HostSummary interface {
	// TryMatch atomically tries to match the current host with given
	// HostFilter, and lock the host if it does.
	TryMatch(filter *hostmgr.HostFilter) Match

	// ReleasePodResources adds back resources to the current hostSummary.
	ReleasePodResources(ctx context.Context, podID string)

	// CompleteLease verifies that the leaseID on this host is still valid.
	CompleteLease(leaseID string, newPodToResMap map[string]scalar.Resources) error

	// CasStatus sets the status to new value if current value is old, otherwise
	// returns error.
	CasStatus(old, new HostStatus) error

	// GetCapacity returns the capacity of the host.
	GetCapacity() scalar.Resources

	// GetAllocated returns the allocation of the host.
	GetAllocated() scalar.Resources

	// SetCapacity sets the capacity of the host.
	SetCapacity(r scalar.Resources)

	// GetVersion returns the version of the host.
	GetVersion() string

	// SetVersion sets the version of the host.
	SetVersion(v string)

	// GetHostname returns the hostname of the host.
	GetHostname() string

	// GetHostStatus returns the HostStatus of the host.
	GetHostStatus() HostStatus

	// GetHostLease creates and returns a host lease.
	GetHostLease() *hostmgr.HostLease

	// TerminateLease is called when terminating the lease on a host.
	TerminateLease(leaseID string) error

	// HandlePodEvent is called when a pod event occurs for a pod
	// that affects this host.
	HandlePodEvent(event *p2kscalar.PodEvent) error
}

// hostSummary is a data struct holding resources and metadata of a host.
type hostSummary struct {
	mu sync.RWMutex

	// hostname of the host
	hostname string

	// capacity of the host
	capacity scalar.Resources

	// resources allocated on the host. this should always be equal to the sum
	// of resources in podToResMap
	allocated scalar.Resources

	// labels on this host
	labels []*peloton.Label

	// pod map of PodID to resources for pods that run on this host
	podToResMap map[string]scalar.Resources

	// a map of podIDs for which the host is held
	// key is the podID, value is the expiration time of the hold
	heldPodIDs map[string]time.Time

	// locking status of this host
	status HostStatus

	// LeaseID is a valid UUID when the host is locked for placement and will
	// be used to ensure that the the host is used to launch only those pods
	// for which the lease was acquired by placement engine. Will be empty if
	// host is not in placing state. This leaseID does not correspond to a
	// chunk of resources on that host, but the entire host. So we run the risk
	// of locking the entire host even if the resource constraint is small. We
	// can optimize this further by maintaining a list of leaseIDs per host.
	leaseID string

	// Resource version of this host.
	version string
}

// New returns a zero initialized HostSummary object.
func newHostSummary(
	hostname string,
	r *peloton.Resources,
	version string,
) HostSummary {
	rs := scalar.FromPelotonResources(r)
	return &hostSummary{
		status:      ReadyHost,
		hostname:    hostname,
		podToResMap: make(map[string]scalar.Resources),
		heldPodIDs:  make(map[string]time.Time),
		capacity:    rs,
		version:     version,
	}
}

// TryMatch atomically tries to match the current host with given HostFilter,
// and lock the host if it does. If current hostSummary is matched, this host
// will be marked as `PLACING`, after which it cannot be used by another
// placement engine until released. If current host is not matched by given
// HostFilter, the host status will remain unchanged.
func (a *hostSummary) TryMatch(
	filter *hostmgr.HostFilter,
) Match {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != ReadyHost && a.status != HeldHost {
		return Match{
			Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
		}
	}

	// For host in Held state, it is only a match if the filter hint contains
	// the host.
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
				Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			}
		}
	}

	result := a.matchHostFilter(filter)

	if result != hostmgr.HostFilterResult_HOST_FILTER_MATCH {
		return Match{Result: result}
	}

	// TODO: Handle oversubscription

	// Setting status to `PlacingHost`: this ensures proper state tracking of
	// resources on the host and also ensures that this host will not be used by
	// another placement engine before it is released.
	err := a.casStatus(a.status, PlacingHost)
	if err != nil {
		return Match{
			Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
		}
	}

	return Match{
		Result:   hostmgr.HostFilterResult_HOST_FILTER_MATCH,
		HostName: a.hostname,
	}
}

// ReleasePodResources adds back resources to the current hostSummary.
// When a pod is terminal, it will be deleted and this function will be called
// to remove that pod from the host summary and free up the resources allocated
// to that pod.
func (a *hostSummary) ReleasePodResources(
	ctx context.Context,
	podID string,
) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.podToResMap[podID]; !ok {
		// TODO: add failure metric
		log.WithField("podID", podID).Error("pod not found in host summary")
		return
	}
	delete(a.podToResMap, podID)
	a.calculateAllocated()
}

// CompleteLease verifies that the leaseID on this host is still valid.
// It checks that current hostSummary is in Placing status, updates podToResMap
// to the host summary, recalculates allocated resources and set the host status
// to Ready/Held.
func (a *hostSummary) CompleteLease(
	leaseID string,
	newPodToResMap map[string]scalar.Resources,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != PlacingHost {
		return yarpcerrors.InvalidArgumentErrorf("host status is not Placing")
	}

	if a.leaseID != leaseID {
		return yarpcerrors.InvalidArgumentErrorf("host leaseID does not match")
	}

	// Reset status to held/ready depending on if the host is held for
	// other tasks.
	newState := a.getResetStatus()
	if err := a.casStatus(PlacingHost, newState); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to unlock host: %s", err)
	}

	// At this point the lease is terminated, the host is back in ready/held
	// status but we need to validate if the new pods can be successfully
	// launched on this host. Note that the lease has to be terminated before
	// this step irrespective of the outcome
	if err := a.validateNewPods(newPodToResMap); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("pod validation failed: %s", err)
	}

	// Update podToResMap with newPodToResMap for the new pods to be launched
	// Reduce available resources by the resources required by the new pods
	a.updatePodToResMap(newPodToResMap)

	log.WithFields(log.Fields{
		"hostname":   a.hostname,
		"pods":       newPodToResMap,
		"new_status": newState,
	}).Debug("pods added to the host for launch")

	return nil
}

// CasStatus sets the status to new value if current value is old, otherwise
// returns error.
func (a *hostSummary) CasStatus(old, new HostStatus) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.casStatus(old, new); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to set cas status: %s", err)
	}

	return nil
}

// GetCapacity returns the capacity of the host.
func (a *hostSummary) GetCapacity() scalar.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.capacity
}

// GetAllocated returns the allocation of the host.
func (a *hostSummary) GetAllocated() scalar.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.allocated
}

// SetCapacity sets the capacity of the host.
func (a *hostSummary) SetCapacity(r scalar.Resources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.capacity = r
}

// GetVersion returns the version of the host.
func (a *hostSummary) GetVersion() string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.version
}

// SetVersion sets the version of the host.
func (a *hostSummary) SetVersion(v string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.version = v
}

// GetHostname returns the hostname of the host.
func (a *hostSummary) GetHostname() string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.hostname
}

// GetHostStatus returns the HostStatus of the host.
func (a *hostSummary) GetHostStatus() HostStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.status
}

// GetHostLease creates and returns a host lease.
func (a *hostSummary) GetHostLease() *hostmgr.HostLease {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return &hostmgr.HostLease{
		LeaseId: &hostmgr.LeaseID{
			Value: a.leaseID,
		},
		HostSummary: &pbhost.HostSummary{
			Hostname:  a.hostname,
			Resources: scalar.ToPelotonResources(a.getAvailable()),
			Labels:    a.labels,
		},
	}
}

// TerminateLease is called when terminating the lease on a host.
// This will be called when host in PLACING state is not used, and placement
// engine decides to terminate its lease and set the host back to Ready/Held.
func (a *hostSummary) TerminateLease(leaseID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != PlacingHost {
		return yarpcerrors.InvalidArgumentErrorf("invalid status %v", a.status)
	}

	if a.leaseID != leaseID {
		return yarpcerrors.InvalidArgumentErrorf("host leaseID does not match")
	}

	newStatus := a.getResetStatus()
	if err := a.casStatus(PlacingHost, newStatus); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to set cas status: %s", err)
	}

	return nil
}

// HandlePodEvent makes sure that we update the host summary according to the
// pod events that occur in the cluster.
// Add events should no-op because we already allocated the resources while
// completing the lease and launching the pods.
// Update events are not handled at the moment, but theoretically should only
// noop because a pod update can only change the image of the pod, not the
// resource profile.
// Delete events should release the resources of the pod that was deleted.
func (a *hostSummary) HandlePodEvent(event *p2kscalar.PodEvent) error {
	switch event.EventType {
	case p2kscalar.AddPod, p2kscalar.UpdatePod:
		// We do not need to do anything during an Add event, as it will
		// always follow a Launch, which already populated this host summary.
		// Update events only change the image of the pod, and as such the
		// resource accounting doesn't change.
		return nil
	case p2kscalar.DeletePod:
		// The release error scenario is handled inside release. If the pod
		// was already deleted, ReleasePodResources no-ops, which is correct
		// here.
		a.ReleasePodResources(context.Background(), event.Event.PodId.Value)
		return nil
	}
	return fmt.Errorf("unsupported pod event type: %v", event.EventType)
}

// getResetStatus returns the new host status for a host that is going to be
// reset from PLACING/HELD state.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) getResetStatus() HostStatus {
	newStatus := ReadyHost
	if len(a.heldPodIDs) != 0 {
		newStatus = HeldHost
	}

	return newStatus
}

// validateNewPods will return an error if:
// 1. The pod already exists on the host map.
// 2. The host has insufficient resources to place new pods.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) validateNewPods(
	newPodToResMap map[string]scalar.Resources,
) error {
	var needed scalar.Resources

	available := a.getAvailable()
	for podID, res := range newPodToResMap {
		if _, ok := a.podToResMap[podID]; ok {
			return fmt.Errorf("pod %v already exists on the host", podID)
		}
		needed = needed.Add(res)
	}
	if !available.Contains(needed) {
		return errors.New("host has insufficient resources")
	}
	return nil
}

// calculateAllocated walks through the current list of pods on this host and
// calculates total allocated resources.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) calculateAllocated() {
	var allocated scalar.Resources
	// calculate current allocation based on the new pods map
	for _, r := range a.podToResMap {
		allocated = allocated.Add(r)
	}
	a.allocated = allocated
}

// updatepodToResMap updates the current podToResMap with the new podToResMap
// and also recalculate available resources based on the new podToResMap.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) updatePodToResMap(
	newPodToResMap map[string]scalar.Resources,
) {
	// Add new pods to the pods map.
	for podID, res := range newPodToResMap {
		a.podToResMap[podID] = res
	}
	a.calculateAllocated()
}

// casStatus lock-freely sets the status to new value and update lease ID if
// current value is old, otherwise returns error.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) casStatus(oldStatus, newStatus HostStatus) error {
	if a.status != oldStatus {
		return fmt.Errorf("Invalid old status: %v", oldStatus)
	}
	a.status = newStatus

	switch a.status {
	case ReadyHost:
		// if its a ready host then reset the hostOfferID
		a.leaseID = emptyLeaseID
	case PlacingHost:
		// generate the offer id for a placing host.
		a.leaseID = uuid.New()
	case ReservedHost:
		// generate the offer id for a placing host.
		a.leaseID = uuid.New()
	case HeldHost:
		a.leaseID = emptyLeaseID
	}
	return nil
}

// matchHostFilter determines whether given HostFilter matches the host.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) matchHostFilter(
	c *hostmgr.HostFilter,
) hostmgr.HostFilterResult {

	min := c.GetResourceConstraint().GetMinimum()
	available := a.getAvailable()

	if min != nil {
		// Get min required resources.
		minRes := scalar.FromResourceSpec(min)
		if !available.Contains(minRes) {
			return hostmgr.HostFilterResult_HOST_FILTER_INSUFFICIENT_RESOURCES
		}
	}

	// TODO: Match ports resources.

	sc := c.GetSchedulingConstraint()

	// If constraints don't specify an exclusive host, then reject
	// hosts that are designated as exclusive.
	if constraints.IsNonExclusiveConstraint(sc) &&
		constraints.HasExclusiveLabel(a.labels) {
		log.WithField("hostname", a.hostname).Debug("Skipped exclusive host")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	if sc == nil {
		// No scheduling constraint, we have a match.
		return hostmgr.HostFilterResult_HOST_FILTER_MATCH
	}

	// Only evaluator based on host constraints is in use.
	evaluator := constraints.NewEvaluator(
		pbpod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST)

	lv := constraints.GetHostLabelValues(a.hostname, a.labels)
	result, err := evaluator.Evaluate(sc, lv)
	if err != nil {
		log.WithError(err).
			Error("Error when evaluating input constraint")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	switch result {
	case constraints.EvaluateResultMatch:
		fallthrough
	case constraints.EvaluateResultNotApplicable:
		log.WithFields(log.Fields{
			"values":     lv,
			"hostname":   a.hostname,
			"constraint": sc,
		}).Debug("Attributes match constraint")
	default:
		log.WithFields(log.Fields{
			"values":     lv,
			"hostname":   a.hostname,
			"constraint": sc,
		}).Debug("Attributes do not match constraint")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	return hostmgr.HostFilterResult_HOST_FILTER_MATCH
}

// getAvailable calculates available resources by subtracting the current
// allocation from host capacity.
// This function assumes hostSummary lock is held before calling.
func (a *hostSummary) getAvailable() scalar.Resources {
	available, ok := a.capacity.TrySubtract(a.allocated)
	if !ok {
		// continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated":   a.allocated,
				"podToResMap": a.podToResMap,
				"capacity":    a.capacity,
			},
		).Error("Allocated more resources than capacity")
	}
	return available
}
