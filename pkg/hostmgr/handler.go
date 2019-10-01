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

package hostmgr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	halphapb "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/util"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/hostmgr/config"
	"github.com/uber/peloton/pkg/hostmgr/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	"github.com/uber/peloton/pkg/hostmgr/reserver"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// This is the number of completed reservations which
	// will be fetched in one call from the reserver.
	_completedReservationLimit = 10
)

// validation errors
var (
	errEmptyExecutorList                 = errors.New("empty executor list")
	errEmptyExecutorID                   = errors.New("empty executor id")
	errEmptyOfferOperations              = errors.New("empty operations")
	errEmptyTaskList                     = errors.New("empty task list")
	errEmptyAgentID                      = errors.New("empty agent id")
	errEmptyHostName                     = errors.New("empty hostname")
	errEmptyHostOfferID                  = errors.New("empty host offer")
	errNilReservation                    = errors.New("reservation is nil")
	errLaunchOperationIsNotLastOperation = errors.New("launch operation is not the last operation")
	errReservationNotFound               = errors.New("reservation could not be made")
)

// ServiceHandler implements peloton.private.hostmgr.InternalHostService.
type ServiceHandler struct {
	schedulerClient       mpb.SchedulerClient
	operatorMasterClient  mpb.MasterOperatorClient
	metrics               *metrics.Metrics
	offerPool             offerpool.Pool
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider
	roleName              string
	mesosDetector         hostmgr_mesos.MasterDetector
	reserver              reserver.Reserver
	hmConfig              config.Config
	slackResourceTypes    []string
	watchProcessor        watchevent.WatchProcessor
	disableKillTasks      atomic.Bool
	hostPoolManager       manager.HostPoolManager
	goalStateDriver       goalstate.Driver
	hostInfoOps           ormobjects.HostInfoOps // DB ops for host_info table
	hostCache             hostcache.HostCache
	plugin                plugins.Plugin
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	d *yarpc.Dispatcher,
	metrics *metrics.Metrics,
	schedulerClient mpb.SchedulerClient,
	masterOperatorClient mpb.MasterOperatorClient,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	mesosConfig hostmgr_mesos.Config,
	mesosDetector hostmgr_mesos.MasterDetector,
	hmConfig *config.Config,
	slackResourceTypes []string,
	watchProcessor watchevent.WatchProcessor,
	hostPoolManager manager.HostPoolManager,
	goalStateDriver goalstate.Driver,
	hostInfoOps ormobjects.HostInfoOps,
	hostCache hostcache.HostCache,
	plugin plugins.Plugin,
) *ServiceHandler {

	handler := &ServiceHandler{
		schedulerClient:       schedulerClient,
		operatorMasterClient:  masterOperatorClient,
		metrics:               metrics,
		offerPool:             offer.GetEventHandler().GetOfferPool(),
		frameworkInfoProvider: frameworkInfoProvider,
		roleName:              mesosConfig.Framework.Role,
		mesosDetector:         mesosDetector,
		slackResourceTypes:    slackResourceTypes,
		watchProcessor:        watchProcessor,
		hostPoolManager:       hostPoolManager,
		goalStateDriver:       goalStateDriver,
		hostInfoOps:           hostInfoOps,
		hostCache:             hostCache,
		plugin:                plugin,
	}
	// Creating Reserver object for handler
	handler.reserver = reserver.NewReserver(
		handler.metrics,
		hmConfig,
		handler.offerPool)
	d.Register(hostsvc.BuildInternalHostServiceYARPCProcedures(handler))

	return handler
}

// validateHostFilter validates the host filter passed to
// AcquireHostOffers or GetHosts request.
func validateHostFilter(
	filter *hostsvc.HostFilter) *hostsvc.InvalidHostFilter {
	if filter == nil {
		return &hostsvc.InvalidHostFilter{
			Message: "Empty host filter",
		}
	}
	return nil
}

// GetReserver returns the reserver object
func (h *ServiceHandler) GetReserver() reserver.Reserver {
	return h.reserver
}

// DisableKillTasks toggles the flag to disable send kill tasks request
// to mesos master
func (h *ServiceHandler) DisableKillTasks(
	ctx context.Context,
	body *hostsvc.DisableKillTasksRequest,
) (*hostsvc.DisableKillTasksResponse, error) {

	h.disableKillTasks.Store(true)
	log.Info("Disabled the kill tasks request to mesos master. ",
		"To enable restart host manager")

	return &hostsvc.DisableKillTasksResponse{}, nil
}

// GetOutstandingOffers returns all the offers present in offer pool.
func (h *ServiceHandler) GetOutstandingOffers(
	ctx context.Context,
	body *hostsvc.GetOutstandingOffersRequest,
) (*hostsvc.GetOutstandingOffersResponse, error) {

	hostOffers, count := h.offerPool.GetAllOffers()
	if count == 0 {
		return &hostsvc.GetOutstandingOffersResponse{
			Error: &hostsvc.GetOutstandingOffersResponse_Error{
				NoOffers: &hostsvc.NoOffersError{
					Message: "no offers present in offer pool",
				},
			},
		}, nil
	}

	outstandingOffers := make([]*mesos.Offer, 0, count)

	for _, hostOffer := range hostOffers {
		for _, offer := range hostOffer {
			outstandingOffers = append(outstandingOffers, offer)
		}
	}

	return &hostsvc.GetOutstandingOffersResponse{
		Offers: outstandingOffers,
	}, nil
}

// GetHostsByQuery implements InternalHostService.GetHostsByQuery.
// This function gets host resources from offer pool and filters
// host list based on the requirements passed in the request
// through hostsvc.HostFilter.
func (h *ServiceHandler) GetHostsByQuery(
	ctx context.Context,
	body *hostsvc.GetHostsByQueryRequest,
) (*hostsvc.GetHostsByQueryResponse, error) {
	filterHostnames := body.GetHostnames()
	hostSummaries, _ := h.offerPool.GetHostSummaries(filterHostnames)
	hostSummariesCount := len(hostSummaries)

	if hostSummariesCount == 0 {
		return &hostsvc.GetHostsByQueryResponse{}, nil
	}

	cmpLess := body.GetCmpLess()
	resourcesLimit := scalar.FromResourceConfig(body.GetResource())

	hosts := make([]*hostsvc.GetHostsByQueryResponse_Host, 0, hostSummariesCount)
	for hostname, hostSummary := range hostSummaries {
		resources := scalar.FromOffersMapToMesosResources(hostSummary.GetOffers(summary.All))

		if !body.GetIncludeRevocable() {
			_, nonRevocable := scalar.FilterRevocableMesosResources(resources)
			nonRevocableResources := scalar.FromMesosResources(nonRevocable)
			if !nonRevocableResources.Compare(resourcesLimit, cmpLess) {
				continue
			}
			hosts = append(hosts, &hostsvc.GetHostsByQueryResponse_Host{
				Hostname:  hostname,
				Resources: nonRevocable,
				Status:    toHostStatus(hostSummary.GetHostStatus()),
				HeldTasks: hostSummary.GetHeldTask(),
				Tasks:     hostSummary.GetTasks(),
			})
		} else {
			combined, _ := scalar.FilterMesosResources(
				resources,
				func(r *mesos.Resource) bool {
					if r.GetRevocable() != nil {
						return true
					}
					return !hmutil.IsSlackResourceType(r.GetName(), h.slackResourceTypes)
				})

			combinedResources := scalar.FromMesosResources(combined)
			if !combinedResources.Compare(resourcesLimit, cmpLess) {
				continue
			}
			hosts = append(hosts, &hostsvc.GetHostsByQueryResponse_Host{
				Hostname:  hostname,
				Resources: combined,
				Status:    toHostStatus(hostSummary.GetHostStatus()),
				HeldTasks: hostSummary.GetHeldTask(),
				Tasks:     hostSummary.GetTasks(),
			})
		}
	}

	return &hostsvc.GetHostsByQueryResponse{
		Hosts: hosts,
	}, nil
}

// WatchHostSummaryEvent creates a watch to get notified about changes to Host Summary event.
// Changed objects are streamed back to the caller till the watch is
// cancelled.
func (h *ServiceHandler) WatchHostSummaryEvent(
	req *hostsvc.WatchEventRequest,
	stream hostsvc.InternalHostServiceServiceWatchHostSummaryEventYARPCServer,
) error {
	// Create watch for host summary event

	log.WithField("request", req).
		Debug("starting new event watch")

	topic := watchevent.GetTopicFromInput(req.GetTopic())
	if topic != watchevent.HostSummary {
		return yarpcerrors.InvalidArgumentErrorf("Invalid topic expected hostSummary")
	}
	watchID, eventClient, err := h.watchProcessor.NewEventClient(topic)

	if err != nil {
		log.WithError(err).
			Warn("failed to create  watch client")
		return err
	}

	defer func() {
		h.watchProcessor.StopEventClient(watchID)
	}()

	initResp := &hostsvc.WatchHostSummaryEventResponse{
		WatchId: watchID,
		Topic:   req.GetTopic(),
	}
	if err := stream.Send(initResp); err != nil {
		log.WithField("watch_id", watchID).
			WithError(err).
			Warn("failed to send initial response for  watch event")
		return err
	}

	for {
		select {
		case event := <-eventClient.Input:
			topicOfEvent := watchevent.GetTopicFromTheEvent(event)

			if topicOfEvent != watchevent.HostSummary {
				log.Warn("watch processor sends wrong event, expected hostSummary received different object")
				return errors.New("watch processor sends different topic than required")
			}
			resp := &hostsvc.WatchHostSummaryEventResponse{
				WatchId:          watchID,
				Topic:            req.GetTopic(),
				HostSummaryEvent: event.(*halphapb.HostSummary),
			}

			if err := stream.Send(resp); err != nil {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("failed to send response for  watch event")
				return err
			}
		case s := <-eventClient.Signal:
			log.WithFields(log.Fields{
				"watch_id": watchID,
				"signal":   s,
			}).Debug("received signal")

			err := handleSignal(
				watchID,
				s,
				map[watchevent.StopSignal]tally.Counter{
					watchevent.StopSignalCancel:   h.metrics.WatchEventCancel,
					watchevent.StopSignalOverflow: h.metrics.WatchEventOverflow,
				},
			)

			if !yarpcerrors.IsCancelled(err) {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("watch stopped due to signal")
			}

			return err
		}
	}
}

// WatchEventStreamEvent creates a watch to get notified about changes to mesos task update event.
// Changed objects are streamed back to the caller till the watch is
// cancelled.

func (h *ServiceHandler) WatchEventStreamEvent(
	req *hostsvc.WatchEventRequest,
	stream hostsvc.InternalHostServiceServiceWatchEventStreamEventYARPCServer,
) error {
	// Create watch for mesos task update

	log.WithField("request", req).
		Debug("starting new event watch")

	topic := watchevent.GetTopicFromInput(req.GetTopic())

	if topic != watchevent.EventStream {
		return yarpcerrors.InvalidArgumentErrorf("Invalid topic expected eventstream")
	}
	watchID, eventClient, err := h.watchProcessor.NewEventClient(topic)
	if err != nil {
		log.WithError(err).
			Warn("failed to create  watch client")
		return err
	}

	defer func() {
		h.watchProcessor.StopEventClient(watchID)
	}()

	initResp := &hostsvc.WatchEventStreamEventResponse{
		WatchId: watchID,
		Topic:   req.GetTopic(),
	}
	if err := stream.Send(initResp); err != nil {
		log.WithField("watch_id", watchID).
			WithError(err).
			Warn("failed to send initial response for  watch event")
		return err
	}

	for {
		select {
		case event := <-eventClient.Input:

			topicOfEvent := watchevent.GetTopicFromTheEvent(event)

			if topicOfEvent != watchevent.EventStream {
				log.Warn("watch processor not sending right event, expected eventstream, received different object")
				return errors.New("watch processor sending different topic than required")
			}
			resp := &hostsvc.WatchEventStreamEventResponse{
				WatchId:         watchID,
				Topic:           req.GetTopic(),
				MesosTaskUpdate: event.(*pb_eventstream.Event),
			}

			if err := stream.Send(resp); err != nil {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("failed to send response for  watch event")
				return err
			}
		case s := <-eventClient.Signal:
			log.WithFields(log.Fields{
				"watch_id": watchID,
				"signal":   s,
			}).Debug("received signal")

			err := handleSignal(
				watchID,
				s,
				map[watchevent.StopSignal]tally.Counter{
					watchevent.StopSignalCancel:   h.metrics.WatchEventCancel,
					watchevent.StopSignalOverflow: h.metrics.WatchEventOverflow,
				},
			)

			if !yarpcerrors.IsCancelled(err) {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("watch stopped due to signal")
			}

			return err
		}
	}

}

// handleSignal converts StopSignal to appropriate yarpcerror
func handleSignal(
	watchID string,
	s watchevent.StopSignal,
	metrics map[watchevent.StopSignal]tally.Counter,
) error {
	c := metrics[s]
	if c != nil {
		c.Inc(1)
	}

	switch s {
	case watchevent.StopSignalCancel:
		return yarpcerrors.CancelledErrorf("watch cancelled: %s", watchID)
	case watchevent.StopSignalOverflow:
		return yarpcerrors.InternalErrorf("event overflow: %s", watchID)
	default:
		return yarpcerrors.InternalErrorf("unexpected signal: %s", s)
	}
}

// Cancel cancels a watch. The watch stream will get an error indicating
// watch was cancelled and the stream will be closed.
func (h *ServiceHandler) CancelWatchEvent(
	ctx context.Context,
	req *hostsvc.CancelWatchRequest,
) (*hostsvc.CancelWatchResponse, error) {
	watchID := req.GetWatchId()

	err := h.watchProcessor.StopEventClient(watchID)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			h.metrics.WatchCancelNotFound.Inc(1)
		}

		log.WithField("watch_id", watchID).
			WithError(err).
			Warn("failed to stop task client")

		return nil, err
	}

	return &hostsvc.CancelWatchResponse{}, nil

}

// AcquireHostOffers implements InternalHostService.AcquireHostOffers.
func (h *ServiceHandler) AcquireHostOffers(
	ctx context.Context,
	body *hostsvc.AcquireHostOffersRequest,
) (response *hostsvc.AcquireHostOffersResponse, err error) {

	defer func() {
		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		h.metrics.AcquireHostOffers.Inc(1)
		h.metrics.AcquireHostOffersCount.Inc(int64(len(response.HostOffers)))
	}()

	if invalid := validateHostFilter(body.GetFilter()); invalid != nil {
		err = yarpcerrors.InvalidArgumentErrorf("invalid filter")
		h.metrics.AcquireHostOffersInvalid.Inc(1)

		log.WithField("filter", body.GetFilter()).
			Warn("Invalid Filter")

		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				InvalidHostFilter: invalid,
			},
		}, errors.Wrap(err, "invalid filter")
	}

	result, resultCount, err := h.offerPool.ClaimForPlace(ctx, body.GetFilter())
	if err != nil {
		h.metrics.AcquireHostOffersFail.Inc(1)
		log.WithField("filter", body.GetFilter()).
			WithError(err).
			Warn("ClaimForPlace failed")

		return &hostsvc.AcquireHostOffersResponse{
			Error: &hostsvc.AcquireHostOffersResponse_Error{
				Failure: &hostsvc.AcquireHostOffersFailure{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "claim for place failed")
	}

	response = &hostsvc.AcquireHostOffersResponse{
		HostOffers:         []*hostsvc.HostOffer{},
		FilterResultCounts: resultCount,
	}

	for hostname, hostOffer := range result {
		// ClaimForPlace returns offers grouped by host. Thus every
		// offer in hostOffer should have the same value for
		// host-specific information such as AgentId, Attributes etc.
		offers := hostOffer.Offers
		if len(offers) <= 0 {
			log.WithField("host", hostname).
				Warn("Empty offer slice from host")
			continue
		}

		var resources []*mesos.Resource
		for _, offer := range offers {
			resources = append(resources, offer.GetResources()...)
		}

		// create the peloton host offer
		pHostOffer := hostsvc.HostOffer{
			Hostname:   hostname,
			AgentId:    offers[0].GetAgentId(),
			Attributes: offers[0].GetAttributes(),
			Resources:  resources,
			Id:         &peloton.HostOfferID{Value: hostOffer.ID},
		}

		response.HostOffers = append(response.HostOffers, &pHostOffer)
		log.WithFields(log.Fields{
			"hostname":      hostname,
			"host_offer_id": hostOffer.ID,
		}).Info("AcquireHostOffers")
	}

	return response, nil
}

// GetHosts implements InternalHostService.GetHosts.
// This function gets the hosts based on resource requirements
// and constraints passed in the request through hostsvc.HostFilter
func (h *ServiceHandler) GetHosts(
	ctx context.Context,
	body *hostsvc.GetHostsRequest,
) (response *hostsvc.GetHostsResponse, err error) {

	var hosts []*hostsvc.HostInfo
	defer func() {
		if err != nil {
			h.metrics.GetHostsInvalid.Inc(1)
			err = yarpcerrors.Newf(yarpcerrors.CodeInternal, err.Error())
			return
		}

		log.WithField("body", body).Debug("GetHosts called")

		h.metrics.GetHosts.Inc(1)
		h.metrics.GetHostsCount.Inc(int64(len(hosts)))
	}()

	if invalid := validateHostFilter(body.GetFilter()); invalid != nil {
		response = h.processGetHostsFailure(invalid)
		return response, errors.New("invalid host filter")
	}

	matcher := NewMatcher(
		body.GetFilter(),
		constraints.NewEvaluator(pb_task.LabelConstraint_HOST),
		h.hostPoolManager,
		func(resourceType string) bool {
			return hmutil.IsSlackResourceType(resourceType, h.slackResourceTypes)
		})
	result, matchErr := matcher.GetMatchingHosts()
	if matchErr != nil {
		response = h.processGetHostsFailure(matchErr)
		return response, errors.New(matchErr.GetMessage())
	}

	for hostname, agentInfo := range result {
		hosts = append(hosts, util.CreateHostInfo(hostname, agentInfo))
	}

	response = &hostsvc.GetHostsResponse{
		Hosts: hosts,
	}

	return response, nil
}

// processGetHostsFailure process the GetHostsFailure and returns the
// error in respose otherwise with empty error
func (h *ServiceHandler) processGetHostsFailure(
	err interface{},
) (resp *hostsvc.GetHostsResponse) {

	resp = &hostsvc.GetHostsResponse{
		Error: &hostsvc.GetHostsResponse_Error{},
	}

	if filter, ok := err.(*hostsvc.InvalidHostFilter); ok {
		log.WithField("error", filter.Message).Warn("no matching hosts")
		resp.Error.InvalidHostFilter = filter
	}

	if hostFailure, ok := err.(*hostsvc.GetHostsFailure); ok {
		log.WithField("error", hostFailure.Message).Warn("no matching hosts")
		resp.Error.Failure = hostFailure
	}

	return resp
}

// ReleaseHostOffers implements InternalHostService.ReleaseHostOffers.
func (h *ServiceHandler) ReleaseHostOffers(
	ctx context.Context,
	body *hostsvc.ReleaseHostOffersRequest,
) (response *hostsvc.ReleaseHostOffersResponse, err error) {

	defer func() {
		if err != nil {
			h.metrics.ReleaseHostOffersFail.Inc(1)
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		h.metrics.ReleaseHostOffers.Inc(1)
		h.metrics.ReleaseHostsCount.Inc(int64(len(body.GetHostOffers())))
	}()

	for _, hostOffer := range body.GetHostOffers() {
		hostname := hostOffer.GetHostname()
		if err := h.offerPool.ReturnUnusedOffers(hostname); err != nil {
			log.WithField("hostoffer", hostOffer).
				WithError(err).
				Warn("Cannot return unused offer on host.")
		}

		log.WithFields(log.Fields{
			"hostname":      hostname,
			"agent_id":      hostOffer.GetAgentId().GetValue(),
			"host_offer_id": hostOffer.GetId().GetValue(),
		}).Info("ReleaseHostOffers")
	}

	return &hostsvc.ReleaseHostOffersResponse{}, nil
}

// OfferOperations implements InternalHostService.OfferOperations.
func (h *ServiceHandler) OfferOperations(
	ctx context.Context,
	req *hostsvc.OfferOperationsRequest) (
	*hostsvc.OfferOperationsResponse,
	error) {
	return nil, fmt.Errorf("Unimplemented")
}

// LaunchTasks implements InternalHostService.LaunchTasks.
func (h *ServiceHandler) LaunchTasks(
	ctx context.Context,
	req *hostsvc.LaunchTasksRequest,
) (response *hostsvc.LaunchTasksResponse, err error) {

	defer func() {
		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}
	}()

	if err := validateLaunchTasks(req); err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("%s", err)
		log.WithFields(log.Fields{
			"hostname":       req.GetHostname(),
			"host_offer_id":  req.GetId(),
			"mesos_agent_id": req.GetAgentId(),
		}).WithError(err).Error("validate launch tasks failed")
		h.metrics.LaunchTasksInvalid.Inc(1)

		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "validate launch tasks failed")
	}

	hostToTaskIDs := make(map[string][]*peloton.TaskID)
	for _, launchableTask := range req.GetTasks() {
		hostHeld := h.offerPool.GetHostHeldForTask(launchableTask.GetId())
		if len(hostHeld) != 0 {
			hostToTaskIDs[hostHeld] =
				append(hostToTaskIDs[hostHeld], launchableTask.GetId())
		}
	}

	for hostname, taskIDs := range hostToTaskIDs {
		if hostname != req.GetHostname() {
			log.WithFields(log.Fields{
				"task_ids":      taskIDs,
				"host_held":     hostname,
				"host_launched": req.GetHostname(),
			}).Info("task not launched on the host held")

			if err := h.offerPool.ReleaseHoldForTasks(hostname, taskIDs); err != nil {
				log.WithFields(log.Fields{
					"task_ids":  taskIDs,
					"host_held": hostname,
					"error":     err,
				}).Warn("fail to release held host when launching tasks on other hosts")
				continue
			}
		}
	}

	_, err = h.offerPool.ClaimForLaunch(
		req.GetHostname(),
		req.GetId().GetValue(),
		req.GetTasks(),
		hostToTaskIDs[req.GetHostname()]...,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"hostname":       req.GetHostname(),
			"host_offer_id":  req.GetId(),
			"mesos_agent_id": req.GetAgentId(),
		}).WithError(err).Error("claim for launch failed")
		h.metrics.LaunchTasksInvalidOffers.Inc(1)

		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				InvalidOffers: &hostsvc.InvalidOffers{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "claim for launch failed")
	}

	// temporary workaround to add hosts into cache. This step
	// was part of ClaimForLaunch
	h.hostCache.AddPodsToHost(req.GetTasks(), req.GetHostname())

	var launchablePods []*models.LaunchablePod
	for _, task := range req.GetTasks() {
		jobID, instanceID, err := util.ParseJobAndInstanceID(task.GetTaskId().GetValue())
		if err != nil {
			log.WithFields(
				log.Fields{
					"mesos_id": task.GetTaskId().GetValue(),
				}).WithError(err).
				Error("fail to parse ID when constructing launchable pods in LaunchTask")
			continue
		}

		launchablePods = append(launchablePods, &models.LaunchablePod{
			PodId: util.CreatePodIDFromMesosTaskID(task.GetTaskId()),
			Spec:  api.ConvertTaskConfigToPodSpec(task.GetConfig(), jobID, instanceID),
			Ports: task.Ports,
		})
	}

	launchedPods, err := h.plugin.LaunchPods(ctx, launchablePods, req.GetHostname())
	if err != nil {
		h.metrics.LaunchTasksFail.Inc(int64(len(req.GetTasks())))
		log.WithFields(log.Fields{
			"error":         err,
			"host_offer_id": req.GetId().GetValue(),
		}).Warn("Tasks launch failure")

		return &hostsvc.LaunchTasksResponse{
			Error: &hostsvc.LaunchTasksResponse_Error{
				LaunchFailure: &hostsvc.LaunchFailure{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "task launch failed")
	}

	h.metrics.LaunchTasks.Inc(int64(len(launchedPods)))

	var taskIDs []string
	for _, pod := range launchedPods {
		taskIDs = append(taskIDs, pod.PodId.GetValue())
	}

	log.WithFields(log.Fields{
		"task_ids":      taskIDs,
		"hostname":      req.GetHostname(),
		"host_offer_id": req.GetId().GetValue(),
	}).Info("LaunchTasks")

	return &hostsvc.LaunchTasksResponse{}, nil
}

func validateLaunchTasks(request *hostsvc.LaunchTasksRequest) error {
	if len(request.Tasks) <= 0 {
		return errEmptyTaskList
	}

	if len(request.GetAgentId().GetValue()) <= 0 {
		return errEmptyAgentID
	}

	if len(request.Hostname) <= 0 {
		return errEmptyHostName
	}

	if len(request.GetId().GetValue()) <= 0 {
		return errEmptyHostOfferID
	}

	return nil
}

// ShutdownExecutors implements InternalHostService.ShutdownExecutors.
func (h *ServiceHandler) ShutdownExecutors(
	ctx context.Context,
	body *hostsvc.ShutdownExecutorsRequest,
) (response *hostsvc.ShutdownExecutorsResponse, err error) {

	defer func() {
		log.WithField("request", body).Debug("ShutdownExecutor called.")
		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}
	}()

	shutdownExecutors := body.GetExecutors()

	if err = validateShutdownExecutors(body); err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("%s", err)
		h.metrics.ShutdownExecutorsInvalid.Inc(1)

		return &hostsvc.ShutdownExecutorsResponse{
			Error: &hostsvc.ShutdownExecutorsResponse_Error{
				InvalidExecutors: &hostsvc.InvalidExecutors{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "invalid shutdown executor request")
	}

	var wg sync.WaitGroup
	failedMutex := &sync.Mutex{}
	var failedExecutors []*hostsvc.ExecutorOnAgent
	var errs []string
	for _, shutdownExecutor := range shutdownExecutors {
		wg.Add(1)

		go func(shutdownExecutor *hostsvc.ExecutorOnAgent) {
			defer wg.Done()

			executorID := shutdownExecutor.GetExecutorId()
			agentID := shutdownExecutor.GetAgentId()

			callType := sched.Call_SHUTDOWN
			msg := &sched.Call{
				FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
				Type:        &callType,
				Shutdown: &sched.Call_Shutdown{
					ExecutorId: executorID,
					AgentId:    agentID,
				},
			}
			msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
			err := h.schedulerClient.Call(msid, msg)
			if err != nil {
				h.metrics.ShutdownExecutorsFail.Inc(1)
				log.WithFields(log.Fields{
					"executor_id": executorID,
					"agent_id":    agentID,
					"error":       err,
				}).Error("Shutdown executor failure")

				failedMutex.Lock()
				defer failedMutex.Unlock()
				failedExecutors = append(
					failedExecutors, shutdownExecutor)
				errs = append(errs, err.Error())
				return
			}

			h.metrics.ShutdownExecutors.Inc(1)
			log.WithFields(log.Fields{
				"executor_id": executorID,
				"agent_id":    agentID,
			}).Info("Shutdown executor request sent")
		}(shutdownExecutor)
	}
	wg.Wait()

	if len(failedExecutors) > 0 {
		err = errors.New("unable to shutdown executors")
		return &hostsvc.ShutdownExecutorsResponse{
			Error: &hostsvc.ShutdownExecutorsResponse_Error{
				ShutdownFailure: &hostsvc.ShutdownFailure{
					Message:   strings.Join(errs, ";"),
					Executors: failedExecutors,
				},
			},
		}, err
	}

	return &hostsvc.ShutdownExecutorsResponse{}, nil
}

func validateShutdownExecutors(request *hostsvc.ShutdownExecutorsRequest) error {
	executorList := request.GetExecutors()

	if len(executorList) <= 0 {
		return errEmptyExecutorList
	}

	for _, executor := range executorList {
		if executor.GetAgentId() == nil {
			return errEmptyAgentID
		}
		if executor.GetExecutorId() == nil {
			return errEmptyExecutorID
		}
	}
	return nil
}

// KillAndReserveTasks implements InternalHostService.KillAndReserveTasks.
func (h *ServiceHandler) KillAndReserveTasks(
	ctx context.Context,
	body *hostsvc.KillAndReserveTasksRequest,
) (*hostsvc.KillAndReserveTasksResponse, error) {

	var err error
	defer func() {
		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}
	}()

	var taskIDs []*mesos.TaskID
	heldHostToTaskIDs := make(map[string][]*peloton.TaskID)

	for _, entry := range body.GetEntries() {
		taskIDs = append(taskIDs, entry.GetTaskId())
		heldHostToTaskIDs[entry.GetHostToReserve()] =
			append(heldHostToTaskIDs[entry.GetHostToReserve()], entry.GetId())
	}

	// reserve the hosts first
	for hostname, taskIDs := range heldHostToTaskIDs {
		// fail to reserve hosts should not fail kill,
		// just fail the in-place update and move on
		// to kill the task
		if err := h.offerPool.HoldForTasks(hostname, taskIDs); err != nil {
			log.WithFields(log.Fields{
				"hostname": hostname,
				"task_ids": taskIDs,
			}).WithError(err).
				Warn("fail to hold the host")
			continue
		}
	}

	// then kill the tasks
	invalidTaskIDs, killFailure := h.killTasks(ctx, taskIDs)
	if invalidTaskIDs == nil && killFailure == nil {
		err = errors.New("unable to kill tasks")

		return &hostsvc.KillAndReserveTasksResponse{}, nil
	}

	resp := &hostsvc.KillAndReserveTasksResponse{
		Error: &hostsvc.KillAndReserveTasksResponse_Error{
			InvalidTaskIDs: invalidTaskIDs,
			KillFailure:    killFailure,
		},
	}

	// if task kill fails, try to release the tasks
	// TODO: can release only the failed tasks, for now it is ok, since
	// one task is killed in each call to the API.
	for hostname, taskIDs := range heldHostToTaskIDs {
		if err := h.offerPool.ReleaseHoldForTasks(hostname, taskIDs); err != nil {
			log.WithFields(log.Fields{
				"hostname": hostname,
				"task_ids": taskIDs,
			}).WithError(err).
				Warn("fail to release hold on host after task kill fail")
			continue
		}
	}

	return resp, nil
}

// KillTasks implements InternalHostService.KillTasks.
func (h *ServiceHandler) KillTasks(
	ctx context.Context,
	body *hostsvc.KillTasksRequest) (
	*hostsvc.KillTasksResponse, error) {

	var err error
	defer func() {
		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}
	}()

	invalidTaskIDs, killFailure := h.killTasks(ctx, body.GetTaskIds())
	// release all tasks even if some kill fails, because it is not certain
	// if the kill request does go through.
	// Worst case for releasing host when a task is not killed is in-place update
	// fails to place the task on the desired host.
	h.releaseHostsHeldForTasks(parseTaskIDsFromMesosTaskIDs(body.GetTaskIds()))

	if invalidTaskIDs != nil || killFailure != nil {
		err = errors.New("unable to kill tasks")

		return &hostsvc.KillTasksResponse{
			Error: &hostsvc.KillTasksResponse_Error{
				InvalidTaskIDs: invalidTaskIDs,
				KillFailure:    killFailure,
			},
		}, nil
	}

	return &hostsvc.KillTasksResponse{}, nil
}

func (h *ServiceHandler) killTasks(
	ctx context.Context,
	taskIds []*mesos.TaskID) (
	*hostsvc.InvalidTaskIDs, *hostsvc.KillFailure) {

	if len(taskIds) == 0 {
		return &hostsvc.InvalidTaskIDs{Message: "Empty task ids"}, nil
	}

	if h.disableKillTasks.Load() {
		return nil, &hostsvc.KillFailure{Message: "Kill tasks request is disabled"}
	}

	var failedTaskIds []*mesos.TaskID
	var errs []string
	for _, taskID := range taskIds {
		if err := h.plugin.KillPod(ctx, taskID.GetValue()); err != nil {
			errs = append(errs, err.Error())
			failedTaskIds = append(failedTaskIds, taskID)
			h.metrics.KillTasksFail.Inc(1)
		} else {
			h.metrics.KillTasks.Inc(1)
		}
	}

	if len(failedTaskIds) > 0 {
		return nil, &hostsvc.KillFailure{
			Message: strings.Join(errs, ";"),
			TaskIds: failedTaskIds,
		}
	}

	return nil, nil
}

// ReserveResources implements InternalHostService.ReserveResources.
func (h *ServiceHandler) ReserveResources(
	ctx context.Context,
	body *hostsvc.ReserveResourcesRequest) (
	*hostsvc.ReserveResourcesResponse, error) {

	log.Debug("ReserveResources called.")
	return nil, fmt.Errorf("Unimplemented")
}

// UnreserveResources implements InternalHostService.UnreserveResources.
func (h *ServiceHandler) UnreserveResources(
	ctx context.Context,
	body *hostsvc.UnreserveResourcesRequest) (
	*hostsvc.UnreserveResourcesResponse, error) {

	log.Debug("UnreserveResources called.")
	return nil, fmt.Errorf("Unimplemented")
}

// CreateVolumes implements InternalHostService.CreateVolumes.
func (h *ServiceHandler) CreateVolumes(
	ctx context.Context,
	body *hostsvc.CreateVolumesRequest) (
	*hostsvc.CreateVolumesResponse, error) {

	log.Debug("CreateVolumes called.")
	return nil, fmt.Errorf("Unimplemented")
}

// DestroyVolumes implements InternalHostService.DestroyVolumes.
func (h *ServiceHandler) DestroyVolumes(
	ctx context.Context,
	body *hostsvc.DestroyVolumesRequest) (
	*hostsvc.DestroyVolumesResponse, error) {

	log.Debug("DestroyVolumes called.")
	return nil, fmt.Errorf("Unimplemented")
}

// ClusterCapacity fetches the allocated resources to the framework
func (h *ServiceHandler) ClusterCapacity(
	ctx context.Context,
	body *hostsvc.ClusterCapacityRequest,
) (response *hostsvc.ClusterCapacityResponse, err error) {

	defer func() {
		if err != nil {
			h.metrics.ClusterCapacityFail.Inc(1)
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		h.metrics.ClusterCapacity.Inc(1)
		h.metrics.RefreshClusterCapacityGauges(response)
	}()

	frameWorkID := h.frameworkInfoProvider.GetFrameworkID(ctx)
	if len(frameWorkID.GetValue()) == 0 {
		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: "unable to fetch framework ID",
				},
			},
		}, nil
	}

	allocatedResources, _, err := h.operatorMasterClient.
		GetTasksAllocation(frameWorkID.GetValue())
	if err != nil {
		log.WithError(err).Error("error making cluster capacity request")

		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "error making cluster capacity request")
	}

	agentMap := host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		log.Error("error getting host agentmap")

		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: "error getting host agentmap",
				},
			},
		}, nil
	}
	nonRevocableClusterCapacity := agentMap.Capacity

	// NOTE: This only works if
	// 1) no quota is set for any role, or
	// 2) quota is set for the same role peloton is registered under.
	// If operator set a quota for another role but leave peloton's role unset,
	// cluster capacity will be over estimated.
	quotaResources, err := h.operatorMasterClient.GetQuota(h.roleName)
	if err != nil {
		log.WithError(err).Error("error getting quota")

		return &hostsvc.ClusterCapacityResponse{
			Error: &hostsvc.ClusterCapacityResponse_Error{
				ClusterUnavailable: &hostsvc.ClusterUnavailable{
					Message: err.Error(),
				},
			},
		}, errors.Wrap(err, "error getting quota")
	}

	if err == nil && quotaResources != nil {
		nonRevocableClusterCapacity = scalar.FromMesosResources(quotaResources)
		if nonRevocableClusterCapacity.GetCPU() <= 0 {
			nonRevocableClusterCapacity.CPU = agentMap.Capacity.GetCPU()
		}
		if nonRevocableClusterCapacity.GetMem() <= 0 {
			nonRevocableClusterCapacity.Mem = agentMap.Capacity.GetMem()
		}
		if nonRevocableClusterCapacity.GetDisk() <= 0 {
			nonRevocableClusterCapacity.Disk = agentMap.Capacity.GetDisk()
		}
		if nonRevocableClusterCapacity.GetGPU() <= 0 {
			nonRevocableClusterCapacity.GPU = agentMap.Capacity.GetGPU()
		}
	}

	revocableAllocated, nonRevocableAllocated := scalar.FilterMesosResources(
		allocatedResources, func(r *mesos.Resource) bool {
			return r.GetRevocable() != nil && hmutil.IsSlackResourceType(r.GetName(), h.slackResourceTypes)
		})
	physicalAllocated := scalar.FromMesosResources(nonRevocableAllocated)
	slackAllocated := scalar.FromMesosResources(revocableAllocated)

	response = &hostsvc.ClusterCapacityResponse{
		Resources:               toHostSvcResources(&physicalAllocated),
		AllocatedSlackResources: toHostSvcResources(&slackAllocated),
		PhysicalResources:       toHostSvcResources(&nonRevocableClusterCapacity),
		PhysicalSlackResources:  toHostSvcResources(&agentMap.SlackCapacity),
	}

	return response, nil
}

// GetMesosMasterHostPort returns the Leader Mesos Master hostname and port.
func (h *ServiceHandler) GetMesosMasterHostPort(
	ctx context.Context,
	body *hostsvc.MesosMasterHostPortRequest,
) (response *hostsvc.MesosMasterHostPortResponse, err error) {

	defer func() {
		if err != nil {
			h.metrics.GetMesosMasterHostPortFail.Inc(1)
			err = yarpcutil.ConvertToYARPCError(err)
			return
		}

		h.metrics.GetMesosMasterHostPort.Inc(1)
	}()

	mesosMasterInfo := strings.Split(h.mesosDetector.HostPort(), ":")
	if len(mesosMasterInfo) != 2 {
		err = errors.New("unable to fetch leader mesos master hostname & port")
		return nil, err
	}
	response = &hostsvc.MesosMasterHostPortResponse{
		Hostname: mesosMasterInfo[0],
		Port:     mesosMasterInfo[1],
	}

	return response, nil
}

// ReserveHosts reserves the host for a specified task in the request.
// Host Manager will keep the host offers to itself till the time
// it does not have enough offers to itself and once that's fulfilled
// it will return the reservation with the offer to placement engine.
// till the time reservation is fulfilled or reservation timeout ,
// offers from that host will not be given to any other placement engine.
func (h *ServiceHandler) ReserveHosts(
	ctx context.Context,
	req *hostsvc.ReserveHostsRequest,
) (*hostsvc.ReserveHostsResponse, error) {

	log.WithField("request", req).Debug("ReserveHosts called.")
	if err := validateReserveHosts(req); err != nil {
		return &hostsvc.ReserveHostsResponse{
			Error: &hostsvc.ReserveHostsResponse_Error{
				Failed: &hostsvc.ReservationFailed{
					Message: err.Error(),
				},
			},
		}, nil
	}

	err := h.reserver.EnqueueReservation(ctx, req.Reservation)
	if err != nil {
		log.WithError(err).Error("failed to enqueue reservation")
		return &hostsvc.ReserveHostsResponse{
			Error: &hostsvc.ReserveHostsResponse_Error{
				Failed: &hostsvc.ReservationFailed{
					Message: errReservationNotFound.Error(),
				},
			},
		}, nil
	}
	log.Debug("ReserveHosts returned.")
	return &hostsvc.ReserveHostsResponse{}, nil
}

func validateReserveHosts(req *hostsvc.ReserveHostsRequest) error {
	if req.GetReservation() == nil {
		return errNilReservation
	}
	return nil
}

// GetCompletedReservations gets the completed host reservations from
// reserver. Based on the reserver it returns the list of completed
// Reservations (hostsvc.CompletedReservation) or return the NoFound Error.
func (h *ServiceHandler) GetCompletedReservations(
	ctx context.Context,
	req *hostsvc.GetCompletedReservationRequest,
) (*hostsvc.GetCompletedReservationResponse, error) {
	log.WithField("request", req).Debug("GetCompletedReservations called.")
	completedReservations, err := h.reserver.DequeueCompletedReservation(
		ctx,
		_completedReservationLimit)
	if err != nil {
		return &hostsvc.GetCompletedReservationResponse{
			Error: &hostsvc.GetCompletedReservationResponse_Error{
				NotFound: &hostsvc.NotFound{
					Message: err.Error(),
				},
			},
		}, nil
	}
	log.Debug("GetCompletedReservations returned.")
	return &hostsvc.GetCompletedReservationResponse{
		CompletedReservations: completedReservations,
	}, nil
}

// GetDrainingHosts implements InternalHostService.GetDrainingHosts
func (h *ServiceHandler) GetDrainingHosts(
	ctx context.Context,
	request *hostsvc.GetDrainingHostsRequest,
) (*hostsvc.GetDrainingHostsResponse, error) {
	h.metrics.GetDrainingHosts.Inc(1)
	limit := request.GetLimit()

	// Get all hosts in maintenance from DB
	hostInfos, err := h.hostInfoOps.GetAll(ctx)
	if err != nil {
		h.metrics.GetDrainingHostsFail.Inc(1)
		return nil, err
	}
	// Filter in only hosts in DRAINING state
	var hostnames []string
	for _, h := range hostInfos {
		if limit > 0 && uint32(len(hostnames)) == limit {
			break
		}
		if h.GetState() == hpb.HostState_HOST_STATE_DRAINING {
			hostnames = append(hostnames, h.GetHostname())
		}
	}

	log.WithField("hostnames", hostnames).
		Debug("draining hosts returned by GetDrainingHosts")

	return &hostsvc.GetDrainingHostsResponse{
		Hostnames: hostnames,
	}, nil
}

// MarkHostDrained implements InternalHostService.MarkHostDrained
// Mark the host as drained. This method is called by Resource Manager Drainer
// when there are no tasks on the DRAINING host
func (h *ServiceHandler) MarkHostDrained(
	ctx context.Context,
	request *hostsvc.MarkHostDrainedRequest,
) (*hostsvc.MarkHostDrainedResponse, error) {
	// Get host from DB
	hostInfo, err := h.hostInfoOps.Get(ctx, request.GetHostname())
	if err != nil {
		return nil, err
	}
	// Validate host current state is DRAINING
	if hostInfo.GetState() != hpb.HostState_HOST_STATE_DRAINING {
		log.WithField("hostname", request.GetHostname()).
			Error("host cannot be marked as drained since it is not in draining state")
		return nil, yarpcerrors.NotFoundErrorf("Host not in DRAINING state")
	}

	// Update host state to DRAINED in DB
	if err := h.hostInfoOps.UpdateState(
		ctx,
		request.GetHostname(),
		hpb.HostState_HOST_STATE_DRAINED); err != nil {
		return nil, err
	}

	log.WithField("hostname", request.GetHostname()).Info("host marked as drained")

	// Enqueue into the Goal State Engine
	h.goalStateDriver.EnqueueHost(request.GetHostname(), time.Now())

	h.metrics.MarkHostDrained.Inc(1)
	return &hostsvc.MarkHostDrainedResponse{
		Hostname: request.GetHostname(),
	}, nil
}

// GetMesosAgentInfo implements InternalHostService.GetMesosAgentInfo
// Returns Mesos agent info for a single agent or all agents.
func (h *ServiceHandler) GetMesosAgentInfo(
	ctx context.Context,
	request *hostsvc.GetMesosAgentInfoRequest,
) (*hostsvc.GetMesosAgentInfoResponse, error) {
	r := &hostsvc.GetMesosAgentInfoResponse{}
	hostname := request.GetHostname()

	agentMap := host.GetAgentMap()
	if agentMap != nil {
		if hostname != "" {
			if info, ok := agentMap.RegisteredAgents[hostname]; ok {
				r.Agents = append(r.Agents, info)
			} else {
				r.Error = &hostsvc.GetMesosAgentInfoResponse_Error{
					HostNotFound: &hostsvc.HostNotFound{
						Message: "host not found",
					},
				}
			}
		} else {
			for _, info := range agentMap.RegisteredAgents {
				r.Agents = append(r.Agents, info)
			}
		}
	}
	return r, nil
}

// ReleaseHostsHeldForTasks releases the hosts which are held for the tasks provided
func (h *ServiceHandler) ReleaseHostsHeldForTasks(
	ctx context.Context,
	req *hostsvc.ReleaseHostsHeldForTasksRequest,
) (*hostsvc.ReleaseHostsHeldForTasksResponse, error) {
	if err := h.releaseHostsHeldForTasks(req.GetIds()); err != nil {
		return &hostsvc.ReleaseHostsHeldForTasksResponse{
			Error: &hostsvc.ReleaseHostsHeldForTasksResponse_Error{
				Message: err.Error(),
			},
		}, nil
	}

	return &hostsvc.ReleaseHostsHeldForTasksResponse{}, nil
}

// GetTasksByHostState gets tasks on hosts in the specified host state.
func (h *ServiceHandler) GetTasksByHostState(
	ctx context.Context,
	req *hostsvc.GetTasksByHostStateRequest,
) (response *hostsvc.GetTasksByHostStateResponse, err error) {
	return &hostsvc.GetTasksByHostStateResponse{}, nil
}

func (h *ServiceHandler) releaseHostsHeldForTasks(taskIDs []*peloton.TaskID) error {
	var errs []error
	hostHeldForTasks := make(map[string][]*peloton.TaskID)
	for _, taskID := range taskIDs {
		hostname := h.offerPool.GetHostHeldForTask(taskID)
		if len(hostname) != 0 {
			hostHeldForTasks[hostname] = append(hostHeldForTasks[hostname], taskID)
		}
	}

	for hostname, taskIDs := range hostHeldForTasks {
		if err := h.offerPool.ReleaseHoldForTasks(hostname, taskIDs); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return multierr.Combine(errs...)
}

// GetHostPoolCapacity fetches the resources for all host-pools.
func (h *ServiceHandler) GetHostPoolCapacity(
	ctx context.Context,
	body *hostsvc.GetHostPoolCapacityRequest,
) (response *hostsvc.GetHostPoolCapacityResponse, err error) {
	if h.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	response = &hostsvc.GetHostPoolCapacityResponse{}
	for _, p := range h.hostPoolManager.Pools() {
		cap := p.Capacity()
		hpResource := &hostsvc.HostPoolResources{
			PoolName:         p.ID(),
			PhysicalCapacity: toHostSvcResources(&cap.Physical),
			SlackCapacity:    toHostSvcResources(&cap.Slack),
		}
		response.Pools = append(response.Pools, hpResource)
	}
	return
}

// Helper function to convert scalar.Resource into hostsvc format.
func toHostSvcResources(rs *scalar.Resources) []*hostsvc.Resource {
	return []*hostsvc.Resource{
		{
			Kind:     common.CPU,
			Capacity: rs.CPU,
		}, {
			Kind:     common.DISK,
			Capacity: rs.Disk,
		}, {
			Kind:     common.GPU,
			Capacity: rs.GPU,
		}, {
			Kind:     common.MEMORY,
			Capacity: rs.Mem,
		},
	}
}

// Helper function to convert summary.HostStatus to string
func toHostStatus(hostStatus summary.HostStatus) string {
	var status string
	switch hostStatus {
	case summary.ReadyHost:
		status = "ready"
	case summary.PlacingHost:
		status = "placing"
	case summary.ReservedHost:
		status = "reserved"
	case summary.HeldHost:
		status = "held"
	default:
		status = "unknown"
	}
	return status
}

func parseTaskIDsFromMesosTaskIDs(ids []*mesos.TaskID) []*peloton.TaskID {
	var result []*peloton.TaskID
	for _, id := range ids {
		if taskID, err := util.ParseTaskIDFromMesosTaskID(id.GetValue()); err == nil {
			result = append(result, &peloton.TaskID{Value: taskID})
		} else {
			log.WithError(err).
				Error("unexpected mesos task id")
		}
	}

	return result
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *ServiceHandler {
	return &ServiceHandler{}
}
