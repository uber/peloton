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
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/hostsummary"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	hmscalar "github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	uatomic "github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_hostCacheMetricsRefresh       = "hostCacheMetricsRefresh"
	_hostCacheMetricsRefreshPeriod = 10 * time.Second
	_hostCachePruneHeldHosts       = "hostCachePruneHeldHosts"
	_hostCachePruneHeldHostsPeriod = 180 * time.Second
)

// HostCache manages cluster resources, and provides necessary abstractions to
// interact with underlying system.
type HostCache interface {
	// AcquireLeases acquires leases on hosts that match the filter constraints.
	AcquireLeases(hostFilter *hostmgr.HostFilter) ([]*hostmgr.HostLease, map[string]uint32)

	// TerminateLease is called when the lease is not going to be used, and we
	// want to release the lock on the host.
	TerminateLease(hostname string, leaseID string) error

	// CompleteLease is called when launching pods on a host that has been
	// previously leased to the Placement engine.
	CompleteLease(hostname string, leaseID string, podToSpecMap map[string]*pbpod.PodSpec) error

	// GetClusterCapacity gets the total capacity and allocation of the cluster.
	GetClusterCapacity() (capacity, allocation hmscalar.Resources)

	// Start will start the goroutine that listens for host events.
	Start()

	// Stop will stop the host cache go routine that listens for host events.
	Stop()

	// GetSummaries returns a list of host summaries that the host cache is
	// managing.
	GetSummaries() (summaries []hostsummary.HostSummary)

	// HandlePodEvent is called by pod events manager on receiving a pod event.
	HandlePodEvent(event *scalar.PodEvent)

	// ResetExpiredHeldHostSummaries resets the status of each hostSummary if
	// the helds have expired and returns the hostnames which got reset.
	ResetExpiredHeldHostSummaries(now time.Time) []string

	// GetHostHeldForPod returns the host that is held for the pod.
	GetHostHeldForPod(podID *peloton.PodID) string

	// HoldForPods holds the host for the pods specified.
	HoldForPods(hostname string, podIDs []*peloton.PodID) error

	// ReleaseHoldForPods release the hold of host for the pods specified.
	ReleaseHoldForPods(hostname string, podIDs []*peloton.PodID) error

	// CompleteLaunchPod is called when a pod is successfully launched.
	// This is for things like removing pods allocated to the pod
	// from available ports. This is called after successful launch
	// of individual pod. We cannot do this in CompleteLease.
	// For example, ports should not be removed after a failed launch,
	// otherwise the ports are leaked.
	CompleteLaunchPod(hostname string, pod *models.LaunchablePod) error

	// RecoverPodInfo updates pods info running on a particular host,
	// it is used only when hostsummary needs to recover the info
	// upon restart
	RecoverPodInfoOnHost(
		id *peloton.PodID,
		hostname string,
		state pbpod.PodState,
		spec *pbpod.PodSpec,
	)

	// AddPodsToHost is a temporary method to add host entries in host cache.
	// It would be removed after CompleteLease is called when launching pod.
	AddPodsToHost(tasks []*hostsvc.LaunchableTask, hostname string)
}

// hostCache is an implementation of HostCache interface.
type hostCache struct {
	mu sync.RWMutex

	// Map of hostname to HostSummary.
	hostIndex map[string]hostsummary.HostSummary

	// Map of podID to host held.
	podHeldIndex map[string]string

	// The event channel on which the underlying cluster manager plugin will send
	// host events to host cache.
	hostEventCh chan *scalar.HostEvent

	// Lifecycle manager.
	lifecycle lifecycle.LifeCycle

	// background manager.
	backgroundMgr background.Manager

	// Metrics.
	metrics *Metrics
}

// New returns a new instance of host cache.
func New(
	hostEventCh chan *scalar.HostEvent,
	backgroundMgr background.Manager,
	parent tally.Scope,
) HostCache {
	return &hostCache{
		hostIndex:     make(map[string]hostsummary.HostSummary),
		podHeldIndex:  make(map[string]string),
		hostEventCh:   hostEventCh,
		lifecycle:     lifecycle.NewLifeCycle(),
		metrics:       NewMetrics(parent),
		backgroundMgr: backgroundMgr,
	}
}

func (c *hostCache) GetSummaries() []hostsummary.HostSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	summaries := make([]hostsummary.HostSummary, 0, len(c.hostIndex))
	for _, summary := range c.hostIndex {
		summaries = append(summaries, summary)
	}
	return summaries
}

// AcquireLeases acquires leases on hosts that match the filter constraints.
// The lease will be held until Jobmgr actively launches pods using the leaseID.
// Returns:
// []*hostmgr.HostLease: List of leases acquired on matching hosts.
// map[string]uint32: map filtering result string (i.e. HOST_FILTER_INVALID) to
// number of hosts per result for debugging purpose.
func (c *hostCache) AcquireLeases(
	hostFilter *hostmgr.HostFilter,
) ([]*hostmgr.HostLease, map[string]uint32) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	matcher := hostsummary.NewMatcher(hostFilter)

	// If host hint is provided, try to return the hosts in hints first.
	for _, filterHints := range hostFilter.GetHint().GetHostHint() {
		if hs, ok := c.hostIndex[filterHints.GetHostname()]; ok {
			matcher.TryMatch(hs.GetHostname(), hs)
			if matcher.HostLimitReached() {
				break
			}
		}
	}

	// TODO: implement defrag/firstfit ranker, for now default to first fit
	for hostname, hs := range c.hostIndex {
		matcher.TryMatch(hostname, hs)
		if matcher.HostLimitReached() {
			break
		}
	}

	var hostLeases []*hostmgr.HostLease
	hostLimitReached := matcher.HostLimitReached()
	for _, hostname := range matcher.GetHostNames() {
		hs := c.hostIndex[hostname]
		hostLeases = append(hostLeases, hs.GetHostLease())
	}

	if !hostLimitReached {
		// Still proceed to return something.
		log.WithFields(log.Fields{
			"host_filter":         hostFilter,
			"matched_host_leases": hostLeases,
			"match_result_counts": matcher.GetFilterCounts(),
		}).Debug("Number of hosts matched is fewer than max hosts")
	}
	return hostLeases, matcher.GetFilterCounts()
}

// TerminateLease is called when a lease that was previously acquired, and a
// host locked, is no longer in use. The leaseID of the acquired host should be
// supplied in this call so that the hostcache can match the leaseID.
// At this point, the existing lease is terminated and the host can be used for
// further placement.
// Error cases:
//		LeaseID doesn't match
//		Host is not in Placing status
func (c *hostCache) TerminateLease(
	hostname string,
	leaseID string,
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hs, err := c.getSummary(hostname)
	if err != nil {
		return err
	}
	if err := hs.TerminateLease(leaseID); err != nil {
		// TODO: metrics
		return err
	}
	return nil
}

// CompleteLease is called when launching pods on a host that has been
// previously leased to the Placement engine. The leaseID of the acquired host
// should be supplied in this call so that the hostcache can match the leaseID,
// verify that sufficient resources are present on the host to launch all the
// pods in podToSpecMap, and then allow the pods to be launched on this host.
// At this point, the existing lease is Completed and the host can be used for
// further placement.
// Error cases:
// 		LeaseID doesn't match
//		Host is not in Placing status
// 		There are insufficient resources on the requested host
func (c *hostCache) CompleteLease(
	hostname string,
	leaseID string,
	podToSpecMap map[string]*pbpod.PodSpec,
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hs, err := c.getSummary(hostname)
	if err != nil {
		return err
	}
	if err := hs.CompleteLease(leaseID, podToSpecMap); err != nil {
		// TODO: metrics
		return err
	}

	// TODO: remove held hosts.
	return nil
}

// GetClusterCapacity gets the total cluster capacity and allocation
func (c *hostCache) GetClusterCapacity() (
	capacity, allocation hmscalar.Resources,
) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Go through the hostIndex and calculate capacity and allocation
	// and sum it up to get these at a cluster level
	// TODO: do this for slack resources too.
	for _, hs := range c.hostIndex {
		capacity = capacity.Add(hs.GetCapacity().NonSlack)
		allocation = allocation.Add(hs.GetAllocated().NonSlack)
	}
	return
}

// ResetExpiredHeldHostSummaries resets the status of each hostSummary if
// the holds have expired and returns the hostnames which got reset.
func (c *hostCache) ResetExpiredHeldHostSummaries(deadline time.Time) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var pruned []string
	for hostname, hs := range c.hostIndex {
		isFreed, _, podIDExpired := hs.DeleteExpiredHolds(deadline)
		if isFreed {
			pruned = append(pruned, hostname)
		}
		for _, id := range podIDExpired {
			c.removePodHold(id)
		}
	}
	log.WithField("hosts", pruned).Debug("Hosts pruned")
	return pruned
}

func (c *hostCache) GetHostHeldForPod(podID *peloton.PodID) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hn, ok := c.podHeldIndex[podID.GetValue()]
	if !ok {
		// TODO: this should return an error. But keep it the same way as in
		// offerpool for now.
		return ""
	}
	return hn
}

func (c *hostCache) HoldForPods(hostname string, podIDs []*peloton.PodID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hs, err := c.getSummary(hostname)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range podIDs {
		if err := hs.HoldForPod(id); err != nil {
			errs = append(errs, err)
			continue
		}
		c.addPodHold(hostname, id)
	}
	if len(errs) > 0 {
		return yarpcerrors.InternalErrorf("failed to hold pods: %s", multierr.Combine(errs...))
	}
	return nil
}

func (c *hostCache) ReleaseHoldForPods(hostname string, podIDs []*peloton.PodID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hs, err := c.getSummary(hostname)
	if err != nil {
		return err
	}
	for _, id := range podIDs {
		hs.ReleaseHoldForPod(id)
		c.removePodHold(id)
	}
	return nil
}

// RefreshMetrics refreshes the metrics for hosts in ready and placing state.
func (c *hostCache) RefreshMetrics() {
	totalAvailable := hmscalar.Resources{}
	totalAllocated := hmscalar.Resources{}
	readyHosts := float64(0)
	placingHosts := float64(0)
	heldHosts := float64(0)

	hosts := c.GetSummaries()

	for _, h := range hosts {
		allocated, capacity := h.GetAllocated(), h.GetCapacity()
		available, _ := capacity.NonSlack.TrySubtract(allocated.NonSlack)
		totalAllocated = totalAllocated.Add(allocated.NonSlack)
		totalAvailable = totalAvailable.Add(available)

		switch h.GetHostStatus() {
		case hostsummary.ReadyHost:
			readyHosts++
		case hostsummary.PlacingHost:
			placingHosts++
		}
		if len(h.GetHeldPods()) > 0 {
			heldHosts++
		}
	}

	c.metrics.Available.Update(totalAvailable)
	c.metrics.Allocated.Update(totalAllocated)
	c.metrics.ReadyHosts.Update(readyHosts)
	c.metrics.PlacingHosts.Update(placingHosts)
	c.metrics.HeldHosts.Update(heldHosts)
	c.metrics.AvailableHosts.Update(float64(len(hosts)))
}

// addPodHold add a pod to podHeldIndex. Replace the old host if exists.
func (c *hostCache) addPodHold(hostname string, id *peloton.PodID) {
	old, ok := c.podHeldIndex[id.GetValue()]
	if ok && old != hostname {
		log.WithFields(log.Fields{
			"new_host": hostname,
			"old_host": old,
			"task_id":  id.GetValue(),
		}).Warn("pod is held by multiple hosts")
	}
	c.podHeldIndex[id.GetValue()] = hostname
}

// removePodHold deletes id from podHeldIndex regardless of hostname.
func (c *hostCache) removePodHold(id *peloton.PodID) {
	delete(c.podHeldIndex, id.GetValue())
}

func (c *hostCache) CompleteLaunchPod(hostname string, pod *models.LaunchablePod) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hs, err := c.getSummary(hostname)
	if err != nil {
		return errors.Wrapf(err, "cannot find host %q", hostname)
	}
	hs.CompleteLaunchPod(pod)
	return nil
}

// getSummary returns host summary given name. If the host does not exist,
// return error not found.
func (c *hostCache) getSummary(hostname string) (hostsummary.HostSummary, error) {
	hs, ok := c.hostIndex[hostname]
	if !ok {
		// TODO: metrics
		return nil, yarpcerrors.NotFoundErrorf("cannot find host %s in cache", hostname)
	}
	return hs, nil
}

// waitForHostEvents will start a goroutine that waits on the host events
// channel. The underlying plugin will send events to this channel when any
// underlying host status changes. Example: allocated resources change,
// host goes down or is put in maintenance mode.
func (c *hostCache) waitForHostEvents() {
	for {
		select {
		case event := <-c.hostEventCh:
			switch event.GetEventType() {
			case scalar.AddHost:
				c.addHost(event)
			case scalar.UpdateHostSpec:
				c.updateHostSpec(event)
			case scalar.DeleteHost:
				c.deleteHost(event)
			case scalar.UpdateHostAvailableRes:
				c.updateHostAvailable(event)
			case scalar.UpdateAgent:
				c.updateAgent(event)
			}
		case <-c.lifecycle.StopCh():
			return
		}
	}
}

// Handle pod event which is sent by the pod events manager. This is relevant
// only in case of K8S where we do resource accounting based on pod events.
// Example: a pod delete event will lead to giving back pod's resources to the
// host.
func (c *hostCache) HandlePodEvent(event *scalar.PodEvent) {
	// TODO: evaluate locking strategy
	c.mu.Lock()
	defer c.mu.Unlock()

	hostname := event.Event.GetHostname()
	summary, found := c.hostIndex[hostname]
	if !found {
		// TODO(pourchet): Figure out how to handle this.
		// This could happen if reconciliation was not done before
		// we start processing pod events.
		log.WithFields(log.Fields{
			"hostname": hostname,
			"pod_id":   event.Event.GetPodId().GetValue(),
		}).Error("delete pod event ignored: host summary not found")
		return
	}

	summary.HandlePodEvent(event)
}

func (c *hostCache) addHost(event *scalar.HostEvent) {
	// TODO: evaluate locking strategy
	c.mu.Lock()
	defer c.mu.Unlock()

	hostInfo := event.GetHostInfo()
	version := hostInfo.GetResourceVersion()
	capacity := hostInfo.GetCapacity()

	// Check if the host already exists in the cache and reject if the event is
	// of older version.
	if existing, ok := c.hostIndex[hostInfo.GetHostName()]; ok {
		evtVersion := hostInfo.GetResourceVersion()

		// Check if event has older resource version, ignore if it does
		currentVersion := existing.GetVersion()
		if scalar.IsOldVersion(currentVersion, evtVersion) {
			log.WithFields(log.Fields{
				"hostname":        hostInfo.GetHostName(),
				"capacity":        capacity,
				"event_version":   evtVersion,
				"current_version": currentVersion,
			}).Debug("ignore add event")
			return
		}
	}

	// TODO: figure out how to differemtiate mesos/k8s hosts,
	// now addHost is only used by k8s hosts
	c.hostIndex[hostInfo.GetHostName()] = hostsummary.NewKubeletHostSummary(
		hostInfo.GetHostName(),
		capacity,
		version,
	)
	log.WithFields(log.Fields{
		"hostname": hostInfo.GetHostName(),
		"capacity": hostInfo.GetCapacity(),
		"version":  version,
	}).Debug("add host to cache")
}

func (c *hostCache) updateHostSpec(event *scalar.HostEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hs hostsummary.HostSummary
	var ok bool

	hostInfo := event.GetHostInfo()
	evtVersion := hostInfo.GetResourceVersion()
	capacity := hostInfo.GetCapacity()

	if hs, ok = c.hostIndex[hostInfo.GetHostName()]; !ok {
		// Host not found, possibly an out of order even during host
		// maintenance, due to host being removed from host manager before API
		// server.
		// If for some reason a host was indeed missing, it will be added via
		// reconcile logic.
		log.WithFields(log.Fields{
			"hostname":      hostInfo.GetHostName(),
			"capacity":      capacity,
			"event_version": evtVersion,
		}).Debug("ignore update event, host not found in cache")
		return
	}

	// Check if event has older resource version, ignore if it does.
	currentVersion := hs.GetVersion()
	if scalar.IsOldVersion(currentVersion, evtVersion) {
		log.WithFields(log.Fields{
			"hostname":        hostInfo.GetHostName(),
			"capacity":        capacity,
			"event_version":   evtVersion,
			"current_version": currentVersion,
		}).Debug("ignore update event")
		return
	}

	hs.SetCapacity(capacity)
	hs.SetVersion(evtVersion)
	log.WithFields(log.Fields{
		"hostname": hostInfo.GetHostName(),
		"capacity": hostInfo.GetCapacity(),
		"version":  evtVersion,
	}).Debug("update host in cache")
}

func (c *hostCache) deleteHost(event *scalar.HostEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hostInfo := event.GetHostInfo()
	version := hostInfo.GetResourceVersion()

	// Check if the host already exists in the cache and reject if the event is
	// of older version.
	if existing, ok := c.hostIndex[hostInfo.GetHostName()]; ok {
		evtVersion := hostInfo.GetResourceVersion()

		// Check if event has older resource version, ignore if it does.
		currentVersion := existing.GetVersion()
		if scalar.IsOldVersion(currentVersion, evtVersion) {
			log.WithFields(log.Fields{
				"hostname":        hostInfo.GetHostName(),
				"event_version":   evtVersion,
				"current_version": currentVersion,
			}).Debug("ignore delete event")
			return
		}
	}

	delete(c.hostIndex, hostInfo.GetHostName())
	log.WithFields(log.Fields{
		"hostname": hostInfo.GetHostName(),
		"capacity": hostInfo.GetCapacity(),
		"version":  version,
	}).Debug("delete host from cache")
}

// only applicable to mesos
func (c *hostCache) updateAgent(event *scalar.HostEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hs hostsummary.HostSummary
	var ok bool

	hostInfo := event.GetHostInfo()
	evtVersion := hostInfo.GetResourceVersion()

	hs, ok = c.hostIndex[hostInfo.GetHostName()]

	if !ok {
		hs = hostsummary.NewMesosHostSummary(hostInfo.GetHostName())
		c.hostIndex[hostInfo.GetHostName()] = hs
	}

	hs.SetCapacity(hostInfo.GetCapacity())
	hs.SetVersion(evtVersion)
	log.WithFields(log.Fields{
		"hostname":  hostInfo.GetHostName(),
		"available": hostInfo.GetAvailable(),
		"version":   evtVersion,
	}).Debug("update agent info in host cache")
}

// only applicable to mesos
func (c *hostCache) updateHostAvailable(event *scalar.HostEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hs hostsummary.HostSummary
	var ok bool

	hostInfo := event.GetHostInfo()
	evtVersion := hostInfo.GetResourceVersion()

	hs, ok = c.hostIndex[hostInfo.GetHostName()]

	if !ok {
		hs = hostsummary.NewMesosHostSummary(hostInfo.GetHostName())
		c.hostIndex[hostInfo.GetHostName()] = hs
	}

	hs.SetAvailable(hostInfo.GetAvailable())
	hs.SetVersion(evtVersion)
	log.WithFields(log.Fields{
		"hostname":  hostInfo.GetHostName(),
		"available": hostInfo.GetAvailable(),
		"version":   evtVersion,
	}).Debug("update host in cache")
}

// Start will start the goroutine that listens for host events.
func (c *hostCache) Start() {
	if !c.lifecycle.Start() {
		return
	}

	c.backgroundMgr.RegisterWorks(
		background.Work{
			Name: _hostCacheMetricsRefresh,
			Func: func(_ *uatomic.Bool) {
				c.RefreshMetrics()
			},
			Period: _hostCacheMetricsRefreshPeriod,
		},
	)

	c.backgroundMgr.RegisterWorks(
		background.Work{
			Name: _hostCachePruneHeldHosts,
			Func: func(_ *uatomic.Bool) {
				c.ResetExpiredHeldHostSummaries(time.Now())
			},
			Period: _hostCachePruneHeldHostsPeriod,
		},
	)

	go c.waitForHostEvents()

	log.Warn("hostCache started")
}

// Stop will stop the host cache go routine that listens for host events.
func (c *hostCache) Stop() {
	if !c.lifecycle.Stop() {
		return
	}

	// Wait for drainer to be stopped
	c.lifecycle.Wait()
	log.Info("hostCache stopped")
}

// Reconcile explicitly reconciles host cache.
func (c *hostCache) Reconcile() error {
	// TODO: Implement
	return nil
}

// RecoverPodInfo updates pods info running on a particular host,
// it is used only when hostsummary needs to recover the info
// upon restart
func (c *hostCache) RecoverPodInfoOnHost(
	id *peloton.PodID,
	hostname string,
	state pbpod.PodState,
	spec *pbpod.PodSpec,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hs hostsummary.HostSummary
	hs, ok := c.hostIndex[hostname]
	if !ok {
		if spec.GetMesosSpec() != nil {
			hs = hostsummary.NewMesosHostSummary(hostname)
		} else {
			// TODO: populate capacity and version correctly
			hs = hostsummary.NewKubeletHostSummary(hostname, models.HostResources{}, "")
		}
		c.hostIndex[hostname] = hs
	}

	hs.RecoverPodInfo(id, state, spec)
}

// AddPodsToHost is a temporary method to add host entries in host cache.
// It would be removed after CompleteLease is called when launching pod.
func (c *hostCache) AddPodsToHost(tasks []*hostsvc.LaunchableTask, hostname string) {
	for _, lt := range tasks {
		jobID, instanceID, err := util.ParseJobAndInstanceID(lt.GetTaskId().GetValue())
		if err != nil {
			log.WithFields(log.Fields{
				"mesos_id": lt.GetTaskId().GetValue(),
			}).WithError(err).Error("fail to parse ID when RecoverPodInfoOnHost in LaunchTask")
			continue
		}

		c.RecoverPodInfoOnHost(
			util.CreatePodIDFromMesosTaskID(lt.GetTaskId()),
			hostname,
			pbpod.PodState_POD_STATE_LAUNCHED,
			api.ConvertTaskConfigToPodSpec(lt.GetConfig(), jobID, instanceID),
		)
	}
}
