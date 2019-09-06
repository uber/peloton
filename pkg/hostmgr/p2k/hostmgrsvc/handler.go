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

package hostmgrsvc

import (
	"context"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	v1alpha "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	"github.com/uber/peloton/pkg/hostmgr/p2k/podeventmanager"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// ServiceHandler implements private.hostmgr.v1alpha.svc.HostManagerService.
type ServiceHandler struct {
	// Scheduler plugin.
	plugin plugins.Plugin

	// Host cache.
	hostCache hostcache.HostCache

	// podEventManager exports pod EventStream
	podEventManager podeventmanager.PodEventManager
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	plugin plugins.Plugin,
	hostCache hostcache.HostCache,
	pem podeventmanager.PodEventManager,
) *ServiceHandler {

	handler := &ServiceHandler{
		plugin:          plugin,
		hostCache:       hostCache,
		podEventManager: pem,
	}
	d.Register(svc.BuildHostManagerServiceYARPCProcedures(handler))
	return handler
}

// AcquireHosts implements HostManagerService.AcquireHosts.
func (h *ServiceHandler) AcquireHosts(
	ctx context.Context,
	req *svc.AcquireHostsRequest,
) (resp *svc.AcquireHostsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("req", req).
				WithError(err).
				Warn("HostMgr.AcquireHosts failed")
		}
	}()

	// TODO: call v0 AcquireHostOffers API for mesos, translate that result
	// to AcquireHostsResponse where HostOfferID will become the LeaseID and
	// mesos offer per host will be translated to HostSummary (attributes will
	// become labels and offers will become resources)

	filter := req.GetFilter()
	if filter == nil {
		return nil, yarpcerrors.InternalErrorf("invalid host filter")
	}

	leases, filterCount := h.hostCache.AcquireLeases(filter)

	return &svc.AcquireHostsResponse{
		Hosts:              leases,
		FilterResultCounts: filterCount,
	}, nil
}

// LaunchPods implements HostManagerService.LaunchPods.
func (h *ServiceHandler) LaunchPods(
	ctx context.Context,
	req *svc.LaunchPodsRequest,
) (resp *svc.LaunchPodsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithFields(
				log.Fields{
					"launchable_pods": req.GetPods(),
					"hostname":        req.GetHostname(),
					"lease_id":        req.GetLeaseId().GetValue(),
				}).
				WithError(err).
				Warn("HostMgr.LaunchPods failed")
		}
	}()

	// TODO: call v0 LaunchTasks API for mesos, translate that result
	// to LaunchPodsResponse
	if err = validateLaunchPodsRequest(req); err != nil {
		return nil, err
	}

	// Check if pods to be launched is held by the correct host.
	// If host held is the same as requested or is empty, it is a noop.
	// If host held is different from host in the launch request, releasing
	// held host.
	holdsToRelease := make(map[string][]*peloton.PodID)
	for _, pod := range req.GetPods() {
		hostHeld := h.hostCache.GetHostHeldForPod(pod.GetPodId())
		if len(hostHeld) != 0 && hostHeld != req.GetHostname() {
			log.WithFields(log.Fields{
				"pod_id":         pod.GetPodId().GetValue(),
				"host_held":      hostHeld,
				"host_requested": req.GetHostname(),
			}).Debug("Pod not launching on the host held")
			holdsToRelease[hostHeld] = append(holdsToRelease[hostHeld], pod.GetPodId())
		}
	}
	for hostHeld, pods := range holdsToRelease {
		if err := h.hostCache.ReleaseHoldForPods(hostHeld, pods); err != nil {
			// Only log warning so we get a sense of how often this is
			// happening. It is okay to leave the hold as is because pruner
			// will remove it eventually.
			log.WithFields(log.Fields{
				"pod_ids":   pods,
				"host_held": hostHeld,
				"error":     err,
			}).Warn("Cannot release held host, relying on pruner for cleanup.")
		}
	}

	// Save ports in the first container in PodSpec.
	podToSpecMap := make(map[string]*pbpod.PodSpec)
	for _, pod := range req.GetPods() {
		spec := pod.GetSpec()
		if ports := pod.GetPorts(); len(ports) > 0 {
			cs := spec.GetContainers()[0]
			cs.Ports = buildPortSpec(ports)
		}
		// podToSpecMap: Should we check for repeat podID here?
		podToSpecMap[pod.GetPodId().GetValue()] = spec
	}

	if err = h.hostCache.CompleteLease(
		req.GetHostname(),
		req.GetLeaseId().GetValue(),
		podToSpecMap,
	); err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"lease_id": req.GetLeaseId().GetValue(),
		"hostname": req.GetHostname(),
		"pods":     podToSpecMap,
	}).Debug("LaunchPods success")

	var launchablePods []*models.LaunchablePod

	for _, pod := range req.GetPods() {
		launchablePods = append(launchablePods, &models.LaunchablePod{
			PodId: pod.GetPodId(),
			Spec:  pod.GetSpec(),
			Ports: pod.GetPorts(),
		})
	}

	// Should we check for repeat podID here?
	launched, err := h.plugin.LaunchPods(
		ctx,
		launchablePods,
		req.GetHostname(),
	)
	for _, pod := range launched {
		h.hostCache.CompleteLaunchPod(req.GetHostname(), pod)
	}
	if err != nil {
		return nil, err
	}

	return &svc.LaunchPodsResponse{}, nil
}

func buildPortSpec(ports map[string]uint32) (pss []*pbpod.PortSpec) {
	for k, v := range ports {
		pss = append(pss, &pbpod.PortSpec{
			Name:    k,
			Value:   v,
			EnvName: k,
		})
	}
	return pss
}

// KillPods implements HostManagerService.KillPods.
func (h *ServiceHandler) KillPods(
	ctx context.Context,
	req *svc.KillPodsRequest,
) (resp *svc.KillPodsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("pod_ids", req.GetPodIds()).
				WithError(err).
				Warn("HostMgr.KillPods failed")
		}
	}()

	log.WithFields(log.Fields{
		"pod_id": req.GetPodIds(),
	}).Debug("KillPods success")

	for _, podID := range req.GetPodIds() {
		err := h.plugin.KillPod(ctx, podID.GetValue())
		if err != nil {
			return nil, err
		}
	}
	return &svc.KillPodsResponse{}, nil
}

// KillAndHoldPods implements HostManagerService.KillAndHoldPods.
func (h *ServiceHandler) KillAndHoldPods(
	ctx context.Context,
	req *svc.KillAndHoldPodsRequest,
) (resp *svc.KillAndHoldPodsResponse, err error) {
	defer func() {
		if err != nil {
			log.WithField("pod_entries", req.GetEntries()).
				WithError(err).
				Warn("HostMgr.KillAndHoldPods failed")
		}
	}()

	// Hold hosts for pods and log failures if any.
	podsToHold := make(map[string][]*peloton.PodID)
	for _, entry := range req.GetEntries() {
		podsToHold[entry.GetHostToHold()] = append(
			podsToHold[entry.GetHostToHold()], entry.GetPodId())
	}
	for host, pods := range podsToHold {
		if err := h.hostCache.HoldForPods(host, pods); err != nil {
			log.WithFields(log.Fields{
				"host":    host,
				"pod_ids": pods,
			}).WithError(err).
				Warn("Failed to hold the host")
		}
	}

	log.WithFields(log.Fields{
		"pod_entries": req.GetEntries(),
	}).Debug("KillPods success")

	// Kill pods. Release host if task kill on host fails.
	var errs []error
	var failed []*peloton.PodID
	holdToRelease := make(map[string][]*peloton.PodID)
	for _, entry := range req.GetEntries() {
		// TODO: kill pods in parallel.
		err := h.plugin.KillPod(ctx, entry.GetPodId().GetValue())
		if err != nil {
			errs = append(errs, err)
			failed = append(failed, entry.GetPodId())
			holdToRelease[entry.GetHostToHold()] = append(
				holdToRelease[entry.GetHostToHold()], entry.GetPodId())
		}
	}
	for host, pods := range holdToRelease {
		if err := h.hostCache.ReleaseHoldForPods(host, pods); err != nil {
			log.WithFields(log.Fields{
				"host":  host,
				"error": err,
			}).Warn("Failed to release host")
		}
	}
	if len(errs) != 0 {
		return &svc.KillAndHoldPodsResponse{},
			yarpcerrors.InternalErrorf(multierr.Combine(errs...).Error())
	}
	return &svc.KillAndHoldPodsResponse{}, nil
}

// ClusterCapacity implements HostManagerService.ClusterCapacity.
func (h *ServiceHandler) ClusterCapacity(
	ctx context.Context,
	req *svc.ClusterCapacityRequest,
) (resp *svc.ClusterCapacityResponse, err error) {

	capacity, allocation := h.hostCache.GetClusterCapacity()

	return &svc.ClusterCapacityResponse{
		Capacity:   toHostMgrSvcResources(capacity),
		Allocation: toHostMgrSvcResources(allocation),
	}, nil
}

// GetEvents returns all outstanding pod events in the event stream.
// It is for debug purpose only.
func (h *ServiceHandler) GetEvents(
	ctx context.Context,
	req *svc.GetEventsRequest,
) (resp *svc.GetEventsResponse, err error) {
	events, err := h.podEventManager.GetEvents()
	if err != nil {
		return nil, err
	}
	return &svc.GetEventsResponse{Events: events}, nil
}

// TerminateLeases implements HostManagerService.TerminateLeases.
func (h *ServiceHandler) TerminateLeases(
	ctx context.Context,
	req *svc.TerminateLeasesRequest,
) (resp *svc.TerminateLeasesResponse, err error) {
	var errs error
	for _, lease := range req.Leases {
		err := h.hostCache.TerminateLease(lease.Hostname, lease.LeaseId.Value)
		errs = multierr.Append(errs, err)
	}
	if errs != nil {
		return nil, yarpcerrors.InternalErrorf(errs.Error())
	}
	return &svc.TerminateLeasesResponse{}, nil
}

// GetHostCache returns a dump of the host cache.
func (h *ServiceHandler) GetHostCache(
	ctx context.Context,
	req *svc.GetHostCacheRequest,
) (resp *svc.GetHostCacheResponse, err error) {
	resp = &svc.GetHostCacheResponse{
		Summaries: []*svc.GetHostCacheResponse_Summary{},
	}
	for _, summary := range h.hostCache.GetSummaries() {
		allocation, capacity := summary.GetAllocated(), summary.GetCapacity()
		resp.Summaries = append(resp.Summaries, &svc.GetHostCacheResponse_Summary{
			Hostname: summary.GetHostname(),
			Status:   fmt.Sprintf("%v", summary.GetHostStatus()),
			Allocation: []*v1alpha.Resource{
				{Kind: "cpu", Capacity: allocation.NonSlack.CPU},
				{Kind: "mem", Capacity: allocation.NonSlack.Mem},
				{Kind: "disk", Capacity: allocation.NonSlack.Disk},
				{Kind: "gpu", Capacity: allocation.NonSlack.GPU},
			},
			Capacity: []*v1alpha.Resource{
				{Kind: "cpu", Capacity: capacity.NonSlack.CPU},
				{Kind: "mem", Capacity: capacity.NonSlack.Mem},
				{Kind: "disk", Capacity: capacity.NonSlack.Disk},
				{Kind: "gpu", Capacity: capacity.NonSlack.GPU},
			},
		})
	}
	return resp, nil
}

// validateLaunchPodsRequest does some sanity checks on launch pods request.
func validateLaunchPodsRequest(req *svc.LaunchPodsRequest) error {
	if len(req.Pods) <= 0 {
		return yarpcerrors.InternalErrorf("Empty pods list")
	}
	if req.GetLeaseId().GetValue() == "" {
		return yarpcerrors.InternalErrorf("Empty lease id")
	}
	if req.GetHostname() == "" {
		return yarpcerrors.InternalErrorf("Empty host name")
	}
	return nil
}

// toHostSvcResources convert scalar.Resource into hostmgrsvc format.
func toHostMgrSvcResources(r scalar.Resources) []*hostmgr.Resource {
	return []*hostmgr.Resource{
		{
			Kind:     common.CPU,
			Capacity: r.CPU,
		}, {
			Kind:     common.DISK,
			Capacity: r.Disk,
		}, {
			Kind:     common.GPU,
			Capacity: r.GPU,
		}, {
			Kind:     common.MEMORY,
			Capacity: r.Mem,
		},
	}
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *ServiceHandler {
	return &ServiceHandler{}
}
