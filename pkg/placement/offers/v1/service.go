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
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/models/v1"
	"github.com/uber/peloton/pkg/placement/offers"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v1"
)

const (
	_failedToAcquireHosts      = "failed to acquire hosts"
	_failedToFetchTasksOnHosts = "failed to fetch tasks on hosts"
	_noHostsAcquired           = "no hosts acquired"
	_timeout                   = 10 * time.Second
)

type service struct {
	hostManager     hostsvc.HostManagerServiceYARPCClient
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	metrics         *metrics.Metrics
}

// NewService returns a new offer service that calls
// out to the v1 api of hostmanager.
func NewService(
	hostManager hostsvc.HostManagerServiceYARPCClient,
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient,
	metrics *metrics.Metrics,
) offers.Service {
	return &service{
		hostManager:     hostManager,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

// Acquire acquires a batch of leases from host manager.
func (s *service) Acquire(
	ctx context.Context,
	fetchTasks bool,
	taskType resmgr.TaskType,
	needs plugins.PlacementNeeds,
) ([]models.Offer, string) {
	filter := plugins_v1.PlacementNeedsToHostFilter(needs)
	req := &hostsvc.AcquireHostsRequest{Filter: filter}

	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	resp, err := s.hostManager.AcquireHosts(ctx, req)
	if err != nil {
		log.WithFields(log.Fields{
			"filter":      filter,
			"task_type":   taskType,
			"fetch_tasks": fetchTasks,
		}).WithError(err).Error(_failedToAcquireHosts)
		s.metrics.OfferGetFail.Inc(1)
		return nil, fmt.Sprintf("failed to acquire hosts: %s", err)
	}

	log.WithFields(log.Fields{
		"acquire_hosts_request":  req,
		"acquire_hosts_response": resp,
	}).Debug("acquire host offers returned")

	if len(resp.Hosts) == 0 {
		log.WithFields(log.Fields{
			"host_offers": resp.Hosts,
			"filter":      filter,
			"task_type":   taskType,
			"fetch_tasks": fetchTasks,
		}).Error(_noHostsAcquired)
		return nil, _noHostsAcquired
	}

	// Ignore error. It literally will never happen.
	jsonFilterRes, _ := json.Marshal(resp.FilterResultCounts)

	hostnames := []string{}
	for _, host := range resp.Hosts {
		hostnames = append(hostnames, host.GetHostSummary().GetHostname())
	}

	tasksLists := map[string][]*resmgr.Task{}
	if fetchTasks {
		tasksLists, err = s.fetchTaskLists(ctx, taskType, hostnames)
		if err != nil {
			log.WithFields(log.Fields{
				"host_offers":    resp.Hosts,
				"filter_results": jsonFilterRes,
				"filter":         filter,
				"task_type":      taskType,
				"fetch_tasks":    fetchTasks,
			}).WithError(err).Error(_failedToFetchTasksOnHosts)
			s.metrics.OfferGetFail.Inc(1)
			return nil, fmt.Sprintf("failed to fetch tasks on hosts: %v", err)
		}
	}

	log.WithFields(log.Fields{
		"hosts_acquired":      resp.Hosts,
		"filter_results":      resp.FilterResultCounts,
		"filter_results_json": string(jsonFilterRes),
		"fetch_tasks":         fetchTasks,
		"task_type":           taskType,
	}).Debug("HostManager acquired the hosts")
	s.metrics.OfferGet.Inc(1)

	offers := make([]models.Offer, len(resp.Hosts))
	for i, host := range resp.Hosts {
		hostname := host.GetHostSummary().GetHostname()
		offers[i] = models_v1.NewOffer(host, tasksLists[hostname])
	}
	return offers, string(jsonFilterRes)
}

// Release releases a set of leases from host manager so they can
// be re-acquired.
func (s *service) Release(
	ctx context.Context,
	leases []models.Offer,
) {
	if len(leases) == 0 {
		return
	}

	req := &hostsvc.TerminateLeasesRequest{}
	for _, lease := range leases {
		pair := &hostsvc.TerminateLeasesRequest_LeasePair{
			Hostname: lease.Hostname(),
			LeaseId: &hostmgr.LeaseID{
				Value: lease.ID(),
			},
		}
		req.Leases = append(req.Leases, pair)
	}

	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	_, err := s.hostManager.TerminateLeases(ctx, req)
	if err != nil {
		log.WithField("error", err).Error("release host offers failed")
		return
	}

	log.WithFields(log.Fields{
		"terminate_host_leases_request": req,
	}).Debug("terminate host leases request returned with no error")
}

// fetchTaskLists asks the resource manager for a list of all the tasks
// that are running on a given set of hostnames.
func (s *service) fetchTaskLists(
	ctx context.Context,
	taskType resmgr.TaskType,
	hostnames []string,
) (map[string][]*resmgr.Task, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	tasksRequest := &resmgrsvc.GetTasksByHostsRequest{
		Type:      taskType,
		Hostnames: hostnames,
	}
	tasksResponse, err := s.resourceManager.GetTasksByHosts(ctx, tasksRequest)
	if err != nil {
		return nil, err
	}

	result := map[string][]*resmgr.Task{}
	for name, list := range tasksResponse.HostTasksMap {
		result[name] = list.Tasks
	}
	return result, nil
}
