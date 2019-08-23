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

	// TODO: handle held pods for in-place updates.

	// Convert LaunchablePods to a map of podID to scalar resources before
	// completing the lease.
	podToResMap := make(map[string]scalar.Resources)
	for _, pod := range req.GetPods() {
		// TODO: Should we check for repeat podID here?
		podToResMap[pod.GetPodId().GetValue()] = scalar.FromPodSpec(
			pod.GetSpec(),
		)
	}

	if err = h.hostCache.CompleteLease(
		req.GetHostname(),
		req.GetLeaseId().GetValue(),
		podToResMap,
	); err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"lease_id": req.GetLeaseId().GetValue(),
		"hostname": req.GetHostname(),
		"pods":     podToResMap,
	}).Debug("LaunchPods success")

	var launchablePods []*models.LaunchablePod

	for _, pod := range req.GetPods() {
		launchablePods = append(launchablePods, &models.LaunchablePod{
			PodId: pod.GetPodId(),
			Spec:  pod.GetSpec(),
		})
	}

	// Should we check for repeat podID here?
	if err := h.plugin.LaunchPods(
		ctx,
		launchablePods,
		req.GetHostname(),
	); err != nil {
		return nil, err
	}

	return &svc.LaunchPodsResponse{}, nil
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
				{Kind: "cpu", Capacity: allocation.CPU},
				{Kind: "mem", Capacity: allocation.Mem},
				{Kind: "disk", Capacity: allocation.Disk},
				{Kind: "gpu", Capacity: allocation.GPU},
			},
			Capacity: []*v1alpha.Resource{
				{Kind: "cpu", Capacity: capacity.CPU},
				{Kind: "mem", Capacity: capacity.Mem},
				{Kind: "disk", Capacity: capacity.Disk},
				{Kind: "gpu", Capacity: capacity.GPU},
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
