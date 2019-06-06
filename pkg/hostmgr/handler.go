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
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/reservation"
	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/common/util"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/hostmgr/config"
	"github.com/uber/peloton/pkg/hostmgr/factory/operation"
	"github.com/uber/peloton/pkg/hostmgr/factory/task"
	"github.com/uber/peloton/pkg/hostmgr/host"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	mqueue "github.com/uber/peloton/pkg/hostmgr/queue"
	"github.com/uber/peloton/pkg/hostmgr/reserver"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	taskStateManager "github.com/uber/peloton/pkg/hostmgr/task"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"
	"github.com/uber/peloton/pkg/storage"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	yarpc "go.uber.org/yarpc"
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
	errOfferOperationNotSupported        = errors.New("offer operation not supported")
	errInvalidOfferOperation             = errors.New("invalid offer operation")
	errReservationNotFound               = errors.New("reservation could not be made")
)

// ServiceHandler implements peloton.private.hostmgr.InternalHostService.
type ServiceHandler struct {
	schedulerClient        mpb.SchedulerClient
	operatorMasterClient   mpb.MasterOperatorClient
	metrics                *metrics.Metrics
	offerPool              offerpool.Pool
	frameworkInfoProvider  hostmgr_mesos.FrameworkInfoProvider
	volumeStore            storage.PersistentVolumeStore
	roleName               string
	mesosDetector          hostmgr_mesos.MasterDetector
	reserver               reserver.Reserver
	hmConfig               config.Config
	maintenanceQueue       mqueue.MaintenanceQueue // queue containing machineIDs of the machines to be put into maintenance
	slackResourceTypes     []string
	maintenanceHostInfoMap host.MaintenanceHostInfoMap
	taskStateManager       taskStateManager.StateManager
	watchProcessor         watchevent.WatchProcessor
	disableKillTasks       atomic.Bool
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	d *yarpc.Dispatcher,
	metrics *metrics.Metrics,
	schedulerClient mpb.SchedulerClient,
	masterOperatorClient mpb.MasterOperatorClient,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	volumeStore storage.PersistentVolumeStore,
	mesosConfig hostmgr_mesos.Config,
	mesosDetector hostmgr_mesos.MasterDetector,
	hmConfig *config.Config,
	maintenanceQueue mqueue.MaintenanceQueue,
	slackResourceTypes []string,
	maintenanceHostInfoMap host.MaintenanceHostInfoMap,
	taskStateManager taskStateManager.StateManager, watchProcessor watchevent.WatchProcessor) *ServiceHandler {

	handler := &ServiceHandler{
		schedulerClient:        schedulerClient,
		operatorMasterClient:   masterOperatorClient,
		metrics:                metrics,
		offerPool:              offer.GetEventHandler().GetOfferPool(),
		frameworkInfoProvider:  frameworkInfoProvider,
		volumeStore:            volumeStore,
		roleName:               mesosConfig.Framework.Role,
		mesosDetector:          mesosDetector,
		maintenanceQueue:       maintenanceQueue,
		slackResourceTypes:     slackResourceTypes,
		maintenanceHostInfoMap: maintenanceHostInfoMap,
		taskStateManager:       taskStateManager,
		watchProcessor:         watchProcessor,
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

	hostOffers, count := h.offerPool.GetOffers(summary.All)
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
		})
	}

	return &hostsvc.GetHostsByQueryResponse{
		Hosts: hosts,
	}, nil
}

// Watch creates a watch to get notified about changes to mesos task update event.
// Changed objects are streamed back to the caller till the watch is
// cancelled.
func (h *ServiceHandler) WatchEvent(
	req *hostsvc.WatchEventRequest,
	stream hostsvc.InternalHostServiceServiceWatchEventYARPCServer,
) error {
	// Create watch for mesos task update

	log.WithField("request", req).
		Debug("starting new event watch")

	watchID, eventClient, err := h.watchProcessor.NewEventClient()
	if err != nil {
		log.WithError(err).
			Warn("failed to create  watch client")
		return err
	}

	defer func() {
		h.watchProcessor.StopEventClient(watchID)
	}()

	initResp := &hostsvc.WatchEventResponse{
		WatchId: watchID,
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
			resp := &hostsvc.WatchEventResponse{
				WatchId: watchID,
				Events:  []*pb_eventstream.Event{event},
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

	result, resultCount, err := h.offerPool.ClaimForPlace(body.GetFilter())
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
			"agent_id":      offers[0].GetAgentId().GetValue(),
			"host_offer_id": hostOffer.ID,
		}).Info("Acquired Host")
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

	matcher := host.NewMatcher(
		body.GetFilter(),
		constraints.NewEvaluator(pb_task.LabelConstraint_HOST),
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
		}).Info("Released Host")
	}

	return &hostsvc.ReleaseHostOffersResponse{}, nil
}

// validateOfferOperation ensures offer operations sequences are valid.
func validateOfferOperationsRequest(
	request *hostsvc.OfferOperationsRequest) error {

	operations := request.GetOperations()
	if len(operations) == 0 {
		return errEmptyOfferOperations
	}

	for index, op := range operations {
		if op.GetType() == hostsvc.OfferOperation_LAUNCH {
			if index != len(operations)-1 {
				return errLaunchOperationIsNotLastOperation
			} else if len(op.GetLaunch().GetTasks()) == 0 {
				return errEmptyTaskList
			}
		} else if op.GetType() != hostsvc.OfferOperation_CREATE &&
			op.GetType() != hostsvc.OfferOperation_RESERVE {
			return errOfferOperationNotSupported
		} else {
			// Reservation label must be specified for RESERVE/CREATE operation.
			if op.GetReservationLabels() == nil {
				return errInvalidOfferOperation
			}
		}
	}

	if len(request.GetHostname()) == 0 {
		return errEmptyHostName
	}

	return nil
}

// extractReservationLabels checks if operations on reserved offers, if yes,
// returns reservation labels, otherwise nil.
func (h *ServiceHandler) extractReservationLabels(
	req *hostsvc.OfferOperationsRequest,
) *mesos.Labels {
	reqOps := req.GetOperations()
	if len(reqOps) == 1 &&
		reqOps[0].GetType() == hostsvc.OfferOperation_LAUNCH {
		return reqOps[0].GetReservationLabels()
	}

	if len(reqOps) == 2 &&
		reqOps[0].GetType() == hostsvc.OfferOperation_CREATE &&
		reqOps[1].GetType() == hostsvc.OfferOperation_LAUNCH {
		return reqOps[0].GetReservationLabels()
	}

	return nil
}

// OfferOperations implements InternalHostService.OfferOperations.
func (h *ServiceHandler) OfferOperations(
	ctx context.Context,
	req *hostsvc.OfferOperationsRequest) (
	*hostsvc.OfferOperationsResponse,
	error) {
	log.WithField("request", req).Debug("Offer operations called.")

	if err := validateOfferOperationsRequest(req); err != nil {
		h.metrics.OfferOperationsInvalid.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: err.Error(),
				},
			},
		}, nil
	}

	reservedOfferLabels := h.extractReservationLabels(req)
	offers, err := h.offerPool.ClaimForLaunch(
		req.GetHostname(),
		reservedOfferLabels != nil, /* useReservedOffers */
		req.GetId().GetValue(),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"hostname":        req.GetHostname(),
			"host_offer_id":   req.GetId(),
			"offer_resources": scalar.FromOfferMap(offers),
		}).WithError(err).Error("claim offer for operations failed")
		h.metrics.OfferOperationsInvalidOffers.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidOffers: &hostsvc.InvalidOffers{
					Message: err.Error(),
				},
			},
		}, nil
	}

	// TODO: Use `offers` so we can support reservation, port picking, etc.
	log.WithField("offers", offers).Debug("Offers found for launch")

	var offerIds []*mesos.OfferID
	var mesosResources []*mesos.Resource
	var agentID *mesos.AgentID
	for _, offer := range offers {
		offerIds = append(offerIds, offer.GetId())
		agentID = offer.GetAgentId()
		for _, res := range offer.GetResources() {
			if reservedOfferLabels == nil ||
				reservedOfferLabels.String() == res.GetReservation().GetLabels().String() {
				mesosResources = append(mesosResources, res)
			}
		}
	}

	factory := operation.NewOfferOperationsFactory(
		req.GetOperations(),
		mesosResources,
		req.GetHostname(),
		agentID,
	)
	offerOperations, err := factory.GetOfferOperations()
	if err == nil {
		// write the volume info into db if no error.
		err = h.persistVolumeInfo(ctx, offerOperations, req.GetHostname())
	}
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"request": req,
			"offers":  offers,
		}).Error("get offer operations failed.")
		// For now, decline all offers to Mesos in the hope that next
		// call to pool will select some different host.
		// An alternative is to mark offers on the host as ready.
		if reservedOfferLabels == nil {
			if err := h.offerPool.DeclineOffers(ctx, offerIds); err != nil {
				log.WithError(err).
					WithField("offers", offerIds).
					Warn("Cannot decline offers task building error")
			}
		}

		h.metrics.OfferOperationsInvalid.Inc(1)
		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				InvalidArgument: &hostsvc.InvalidArgument{
					Message: "Cannot get offer operations: " + err.Error(),
				},
			},
		}, nil
	}

	callType := sched.Call_ACCEPT
	msg := &sched.Call{
		FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds:   offerIds,
			Operations: offerOperations,
		},
	}

	log.WithFields(log.Fields{
		"call": msg,
	}).Debug("Accepting offer with operations.")

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
	err = h.schedulerClient.Call(msid, msg)
	if err != nil {
		h.metrics.OfferOperationsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"operations": offerOperations,
			"offers":     offerIds,
			"error":      err,
		}).Warn("Offer operations failure")

		return &hostsvc.OfferOperationsResponse{
			Error: &hostsvc.OfferOperationsResponse_Error{
				Failure: &hostsvc.OperationsFailure{
					Message: err.Error(),
				},
			},
		}, nil
	}

	h.metrics.OfferOperations.Inc(1)
	return &hostsvc.OfferOperationsResponse{}, nil
}

// persistVolumeInfo write volume information into db.
func (h *ServiceHandler) persistVolumeInfo(
	ctx context.Context,
	offerOperations []*mesos.Offer_Operation,
	hostname string) error {
	createOperation := operation.GetOfferCreateOperation(offerOperations)
	// Skip creating volume if no create operation.
	if createOperation == nil {
		return nil
	}

	volumeRes := createOperation.GetCreate().GetVolumes()[0]
	volumeID := &peloton.VolumeID{
		Value: volumeRes.GetDisk().GetPersistence().GetId(),
	}

	pv, err := h.volumeStore.GetPersistentVolume(ctx, volumeID)
	if err != nil {
		_, ok := err.(*storage.VolumeNotFoundError)
		if !ok {
			// volume store db read error.
			return err
		}
	}

	if pv != nil {
		switch pv.GetState() {
		case volume.VolumeState_CREATED, volume.VolumeState_DELETED:
			log.WithFields(log.Fields{
				"volume":           pv,
				"offer_operations": offerOperations,
				"hostname":         hostname,
			}).Error("try create to create volume that already exists")
		}
		// TODO(mu): Volume info already exist in db and check if we need to update hostname
		return nil
	}

	jobID, instanceID, err := reservation.ParseReservationLabels(
		volumeRes.GetReservation().GetLabels())
	if err != nil {
		return err
	}

	volumeInfo := &volume.PersistentVolumeInfo{
		Id: volumeID,
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId:    instanceID,
		Hostname:      hostname,
		State:         volume.VolumeState_INITIALIZED,
		GoalState:     volume.VolumeState_CREATED,
		SizeMB:        uint32(volumeRes.GetScalar().GetValue()),
		ContainerPath: volumeRes.GetDisk().GetVolume().GetContainerPath(),
	}

	err = h.volumeStore.CreatePersistentVolume(ctx, volumeInfo)
	return err
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

	offers, err := h.offerPool.ClaimForLaunch(
		req.GetHostname(),
		false,
		req.GetId().GetValue(),
		hostToTaskIDs[req.GetHostname()]...,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"hostname":        req.GetHostname(),
			"host_offer_id":   req.GetId(),
			"mesos_agent_id":  req.GetAgentId(),
			"offer_resources": scalar.FromOfferMap(offers),
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

	var offerIds []*mesos.OfferID
	var mesosResources []*mesos.Resource
	for _, o := range offers {
		offerIds = append(offerIds, o.GetId())
		mesosResources = append(mesosResources, o.GetResources()...)
	}

	// TODO: Use `offers` so we can support reservation, port picking, etc.
	log.WithField("offers", offers).Debug("Offers found for launch")

	var mesosTasks []*mesos.TaskInfo
	var mesosTaskIds []string

	builder := task.NewBuilder(mesosResources)
	for _, t := range req.GetTasks() {
		mesosTask, err := builder.Build(t, nil, nil)
		if err != nil {
			log.WithFields(log.Fields{
				"tasks_total":    len(req.GetTasks()),
				"task":           t.String(),
				"host_resources": scalar.FromOfferMap(offers),
				"hostname":       req.GetHostname(),
				"host_offer_id":  req.GetId().GetValue(),
			}).WithError(err).Warn("fail to get correct mesos taskinfo")
			h.metrics.LaunchTasksInvalid.Inc(1)

			// For now, decline all offers to Mesos in the hope that next
			// call to pool will select some different host.
			// An alternative is to mark offers on the host as ready.
			if derr := h.offerPool.DeclineOffers(ctx, offerIds); derr != nil {
				log.WithError(err).WithFields(log.Fields{
					"offers":   offerIds,
					"hostname": req.GetHostname(),
				}).Warn("cannot decline offers task building error")
			}

			if err == task.ErrNotEnoughResource {
				return &hostsvc.LaunchTasksResponse{
					Error: &hostsvc.LaunchTasksResponse_Error{
						InvalidOffers: &hostsvc.InvalidOffers{
							Message: "not enough resource to run task: " + err.Error(),
						},
					},
				}, errors.Wrap(err, "not enough resource to run task")
			}

			return &hostsvc.LaunchTasksResponse{
				Error: &hostsvc.LaunchTasksResponse_Error{
					InvalidArgument: &hostsvc.InvalidArgument{
						Message: "cannot get Mesos task info: " + err.Error(),
						InvalidTasks: []*hostsvc.LaunchableTask{
							t,
						},
					},
				},
			}, errors.New("cannot get mesos task info")
		}

		mesosTask.AgentId = req.GetAgentId()
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, mesosTask.GetTaskId().GetValue())
	}

	callType := sched.Call_ACCEPT
	opType := mesos.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: offerIds,
			Operations: []*mesos.Offer_Operation{
				{
					Type: &opType,
					Launch: &mesos.Offer_Operation_Launch{
						TaskInfos: mesosTasks,
					},
				},
			},
		},
	}

	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
	err = h.schedulerClient.Call(msid, msg)
	if err != nil {
		h.metrics.LaunchTasksFail.Inc(int64(len(mesosTasks)))
		log.WithFields(log.Fields{
			"tasks":         mesosTasks,
			"offers":        offerIds,
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

	h.metrics.LaunchTasks.Inc(int64(len(mesosTasks)))

	var taskIDs []string
	for _, task := range mesosTasks {
		taskIDs = append(taskIDs, task.GetTaskId().GetValue())
	}

	var offerIDs []string
	for _, offer := range offerIds {
		offerIDs = append(offerIDs, offer.GetValue())
	}

	log.WithFields(log.Fields{
		"tasks":         taskIDs,
		"offers":        offerIDs,
		"host_offer_id": req.GetId().GetValue(),
	}).Info("Tasks launched.")

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

	var wg sync.WaitGroup
	failedMutex := &sync.Mutex{}
	var failedTaskIds []*mesos.TaskID
	var errs []string
	for _, taskID := range taskIds {
		wg.Add(1)

		go func(taskID *mesos.TaskID) {
			defer wg.Done()

			callType := sched.Call_KILL
			msg := &sched.Call{
				FrameworkId: h.frameworkInfoProvider.GetFrameworkID(ctx),
				Type:        &callType,
				Kill: &sched.Call_Kill{
					TaskId: taskID,
				},
			}

			msid := h.frameworkInfoProvider.GetMesosStreamID(ctx)
			err := h.schedulerClient.Call(msid, msg)
			if err != nil {
				h.metrics.KillTasksFail.Inc(1)
				log.WithFields(log.Fields{
					"task_id": taskID,
					"error":   err,
				}).Error("Kill task failure")

				failedMutex.Lock()
				defer failedMutex.Unlock()
				failedTaskIds = append(failedTaskIds, taskID)
				errs = append(errs, err.Error())
				return
			}

			h.metrics.KillTasks.Inc(1)
			log.WithField("task", taskID).Info("Task kill request sent")
		}(taskID)
	}

	wg.Wait()

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
	timeout := time.Duration(request.GetTimeout())
	var hostnames []string
	limit := request.GetLimit()
	// If limit is not specified, set limit to length of maintenance queue
	if limit == 0 {
		limit = uint32(h.maintenanceQueue.Length())
	}

	for i := uint32(0); i < limit; i++ {
		hostname, err := h.maintenanceQueue.Dequeue(timeout * time.Millisecond)
		if err != nil {
			if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
				// error is not due to timeout so we log the error
				log.WithError(err).
					Error("unable to dequeue task from maintenance queue")
				h.metrics.GetDrainingHostsFail.Inc(1)
				return nil, err
			}
			break
		}
		hostnames = append(hostnames, hostname)
		h.metrics.GetDrainingHosts.Inc(1)
	}
	log.WithField("hosts", hostnames).
		Debug("Maintenance Queue - Dequeued hosts")
	return &hostsvc.GetDrainingHostsResponse{
		Hostnames: hostnames,
	}, nil
}

// MarkHostsDrained implements InternalHostService.MarkHostsDrained
// Mark the host as drained. This method is called by Resource Manager Drainer
// when there are no tasks on the DRAINING hosts
func (h *ServiceHandler) MarkHostsDrained(
	ctx context.Context,
	request *hostsvc.MarkHostsDrainedRequest,
) (*hostsvc.MarkHostsDrainedResponse, error) {
	hostSet := stringset.New()
	for _, host := range request.GetHostnames() {
		hostSet.Add(host)
	}
	var machineIDs []*mesos.MachineID
	for _, hostInfo := range h.maintenanceHostInfoMap.GetDrainingHostInfos([]string{}) {
		if hostSet.Contains(hostInfo.GetHostname()) {
			machineIDs = append(machineIDs, &mesos.MachineID{
				Hostname: &hostInfo.Hostname,
				Ip:       &hostInfo.Ip,
			})
		}
	}

	if len(machineIDs) != len(request.Hostnames) {
		log.WithFields(log.Fields{
			"machine_ids_in_map": machineIDs,
			"request":            request,
		}).Errorf("failed to find some hostnames in maintenanceHostInfoMap")
	}
	// No-op if none of the hosts in the request are 'DRAINING'
	if len(machineIDs) == 0 {
		return &hostsvc.MarkHostsDrainedResponse{}, nil
	}
	var downedHosts []string
	var errs error
	for _, machineID := range machineIDs {
		// Start maintenance on the host by posting to
		// /machine/down endpoint of the Mesos Master
		err := h.operatorMasterClient.StartMaintenance([]*mesos.MachineID{machineID})
		if err != nil {
			errs = multierr.Append(errs, err)
			log.WithError(err).
				WithField("machine", machineID).
				Error(fmt.Sprintf("failed to down host"))
			h.metrics.MarkHostsDrainedFail.Inc(1)
			continue
		}

		if err := h.maintenanceHostInfoMap.UpdateHostState(
			machineID.GetHostname(),
			hpb.HostState_HOST_STATE_DRAINING,
			hpb.HostState_HOST_STATE_DOWN); err != nil {
			// log error and add this host to the list of downedHosts since
			// the Mesos StartMaintenance call was successful. The maintenance
			// map will converge on reconciliation with Mesos master.
			log.WithFields(
				log.Fields{
					"hostname": machineID.GetHostname(),
					"from":     hpb.HostState_HOST_STATE_DRAINING.String(),
					"to":       hpb.HostState_HOST_STATE_DOWN.String(),
				}).WithError(err).
				Error("failed to update host state in host map")
		}
		downedHosts = append(downedHosts, machineID.GetHostname())
	}

	h.metrics.MarkHostsDrained.Inc(int64(len(downedHosts)))
	return &hostsvc.MarkHostsDrainedResponse{
		MarkedHosts: downedHosts,
	}, errs
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
	var errs []error
	hostHeldForTasks := make(map[string][]*peloton.TaskID)
	for _, taskID := range req.GetIds() {
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
		return &hostsvc.ReleaseHostsHeldForTasksResponse{}, nil
	}

	return &hostsvc.ReleaseHostsHeldForTasksResponse{
		Error: &hostsvc.ReleaseHostsHeldForTasksResponse_Error{
			Message: multierr.Combine(errs...).Error(),
		},
	}, nil
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
