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

package offers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/offers"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v0"
)

const (
	_failedToAcquireHostOffers = "failed to acquire host offers"
	_noHostOffers              = "no offers from the cluster"
	_failedToFetchTasksOnHosts = "failed to fetch tasks on hosts"
	_timeout                   = 10 * time.Second
)

// NewService will create a new offer service.
func NewService(
	hostManager hostsvc.InternalHostServiceYARPCClient,
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient,
	metrics *metrics.Metrics) offers.Service {
	return &service{
		hostManager:     hostManager,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

type service struct {
	hostManager     hostsvc.InternalHostServiceYARPCClient
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	metrics         *metrics.Metrics
}

// Acquire fetches a batch of offers from the host manager.
func (s *service) Acquire(
	ctx context.Context,
	fetchTasks bool,
	taskType resmgr.TaskType,
	needs plugins.PlacementNeeds) (offers []models.Offer, reason string) {
	filter := plugins_v0.PlacementNeedsToHostFilter(needs)
	// Get list of host -> resources (aggregate of outstanding offers)
	hostOffers, filterResults, err := s.fetchOffers(ctx, filter)
	if err != nil {
		log.WithFields(log.Fields{
			"host_offers":    hostOffers,
			"filter_results": filterResults,
			"filter":         filter,
			"task_type":      taskType,
			"fetch_tasks":    fetchTasks,
		}).WithError(err).Error(_failedToAcquireHostOffers)
		s.metrics.OfferGetFail.Inc(1)
		return offers, _failedToAcquireHostOffers
	}

	filterRes, err := json.Marshal(filterResults)
	if err != nil {
		log.WithFields(log.Fields{
			"host_offers":         hostOffers,
			"task_type":           taskType,
			"fetch_tasks":         fetchTasks,
			"filter":              filter,
			"filter_results":      filterResults,
			"filter_results_json": string(filterRes),
		}).Error(err.Error())
		s.metrics.OfferGetFail.Inc(1)
		return offers, err.Error()
	}

	if len(hostOffers) == 0 {
		return offers, _noHostOffers
	}

	// Get tasks running on hosts from hostOffers
	var hostTasksMap map[string]*resmgrsvc.TaskList
	if fetchTasks && len(hostOffers) > 0 {
		hostTasksMap, err = s.fetchTasks(ctx, hostOffers, taskType)
		if err != nil {
			log.WithFields(log.Fields{
				"hostOffers":     hostOffers,
				"filter_results": filterResults,
				"filter":         filter,
				"task_type":      taskType,
				"fetch_tasks":    fetchTasks,
			}).WithError(err).Error(_failedToFetchTasksOnHosts)
			s.metrics.OfferGetFail.Inc(1)
			return offers, _failedToFetchTasksOnHosts
		}

		// Log tasks already running on Hosts whose offers are acquired.
		log.WithFields(log.Fields{
			"host_offers":         hostOffers,
			"filter":              filter,
			"filter_results_json": string(filterRes),
			"task_type":           taskType,
			"host_task_map":       hostTasksMap,
		}).Debug("Tasks already running on hosts")
	}

	log.WithFields(log.Fields{
		"host_offers":            hostOffers,
		"filter_results":         filterResults,
		"filter":                 filter,
		"task_type":              taskType,
		"fetch_tasks":            fetchTasks,
		"host_tasks_map_noindex": hostTasksMap,
	}).Debug("Offer service acquired offers and related tasks")

	s.metrics.OfferGet.Inc(1)

	// Create placement offers from the host offers
	return s.convertOffers(hostOffers, hostTasksMap, time.Now()), string(filterRes)
}

// Release returns the acquired offers back to host manager.
func (s *service) Release(
	ctx context.Context,
	hosts []models.Offer) {
	if len(hosts) == 0 {
		return
	}

	hostOffers := make([]*hostsvc.HostOffer, 0, len(hosts))
	for _, offer := range hosts {
		agentID := offer.AgentID()
		hostOffers = append(hostOffers, &hostsvc.HostOffer{
			Id:       &peloton.HostOfferID{Value: offer.ID()},
			Hostname: offer.Hostname(),
			AgentId:  &mesos.AgentID{Value: &agentID},
		})
	}

	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	// ToDo: buffer the hosts until we have a batch of a certain size and return that.
	request := &hostsvc.ReleaseHostOffersRequest{
		HostOffers: hostOffers,
	}
	response, err := s.hostManager.ReleaseHostOffers(ctx, request)

	if err != nil {
		log.WithField("error", err).Error("release host offers failed")
		return
	}

	if respErr := response.GetError(); respErr != nil {
		log.WithFields(log.Fields{
			"release_host_request":        request,
			"release_host_response":       response,
			"release_host_response_error": respErr,
		}).Error("release host offers error")
		// TODO: Differentiate known error types by metrics and logs.
	} else {
		log.WithFields(log.Fields{
			"release_host_request":  request,
			"release_host_response": response,
		}).Debug("release host offers request returned")
	}
}

// fetchOffers returns the offers by each host and count of all offers from host manager.
func (s *service) fetchOffers(
	ctx context.Context,
	filter *hostsvc.HostFilter) ([]*hostsvc.HostOffer, map[string]uint32, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	offersRequest := &hostsvc.AcquireHostOffersRequest{
		Filter: filter,
	}
	offersResponse, err := s.hostManager.AcquireHostOffers(ctx, offersRequest)
	if err != nil {
		return nil, nil, err
	}

	log.WithFields(log.Fields{
		"acquire_host_offers_request":  offersRequest,
		"acquire_host_offers_response": offersResponse,
	}).Debug("acquire host offers returned")

	if respErr := offersResponse.GetError(); respErr != nil {
		return nil, nil, errors.New(respErr.String())
	}

	return offersResponse.GetHostOffers(), offersResponse.GetFilterResultCounts(), nil
}

// fetchTasks returns the tasks running on provided host from resource manager.
func (s *service) fetchTasks(
	ctx context.Context,
	hostOffers []*hostsvc.HostOffer,
	taskType resmgr.TaskType) (map[string]*resmgrsvc.TaskList, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	// Extract the hostnames
	hostnames := make([]string, 0, len(hostOffers))
	for _, hostOffer := range hostOffers {
		hostnames = append(hostnames, hostOffer.Hostname)
	}

	// Get tasks running on provided hosts
	tasksRequest := &resmgrsvc.GetTasksByHostsRequest{
		Type:      taskType,
		Hostnames: hostnames,
	}
	tasksResponse, err := s.resourceManager.GetTasksByHosts(ctx, tasksRequest)
	if err != nil {
		return nil, err
	}

	return tasksResponse.HostTasksMap, nil
}

// convertOffers creates host offers into placement offers.
// One key notion is to add already running tasks on this host
// such that placement can take care of task-task affinity.
func (s *service) convertOffers(
	hostOffers []*hostsvc.HostOffer,
	tasks map[string]*resmgrsvc.TaskList,
	now time.Time) []models.Offer {
	offers := make([]models.Offer, 0, len(hostOffers))
	for _, hostOffer := range hostOffers {
		var taskList []*resmgr.Task
		if tasks != nil && tasks[hostOffer.Hostname] != nil {
			taskList = tasks[hostOffer.Hostname].Tasks
		}
		offers = append(offers, models_v0.NewHostOffers(hostOffer, taskList, now))
	}

	return offers
}
