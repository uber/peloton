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

package hosts

import (
	"context"
	"errors"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/placement/metrics"
	models_v0 "github.com/uber/peloton/pkg/placement/models/v0"

	log "github.com/sirupsen/logrus"
)

const (
	_failedToFetchTasksOnHosts = "failed to fetch tasks on hosts"
	_timeout                   = 10 * time.Second
)

var (
	errfailedToAcquireHosts = errors.New("failed to acquire hosts")
	errnotImplemented       = errors.New("Not Implemented")
	errNoValidHosts         = errors.New("no valid hosts for reservation")
	errNoValidTask          = errors.New("no valid task for reservation")
)

// Service will manage hosts used to get hosts and reserve hosts.
type Service interface {
	// GetHosts fetches a batch of hosts from the host manager matching filter.
	GetHosts(ctx context.Context, task *resmgr.Task, filter *hostsvc.HostFilter) (hosts []*models_v0.Host, err error)

	// ReserveHost Makes reservation for the host in hostmanager.
	ReserveHost(ctx context.Context, host []*models_v0.Host, task *resmgr.Task) (err error)

	// GetCompletedReservation gets the completed reservation
	// from host manager
	GetCompletedReservation(
		ctx context.Context) ([]*hostsvc.CompletedReservation, error)
}

// service is implementing Service interface
type service struct {
	// hostmanager yarpc client
	hostManager hostsvc.InternalHostServiceYARPCClient
	// resource manager yarpc client
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	// placement engine metrics object
	metrics *metrics.Metrics
}

// NewService will create a new host service.
func NewService(
	hostManager hostsvc.InternalHostServiceYARPCClient,
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient,
	metrics *metrics.Metrics) Service {
	return &service{
		hostManager:     hostManager,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

// GetHosts fetches a batch of hosts from the host manager
// based on the host filter provided
func (s *service) GetHosts(
	ctx context.Context,
	task *resmgr.Task,
	filter *hostsvc.HostFilter) (hosts []*models_v0.Host, err error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	req := &hostsvc.GetHostsRequest{
		Filter: filter,
	}
	res, err := s.hostManager.GetHosts(ctx, req)
	if err != nil {
		s.metrics.HostGetFail.Inc(1)
		return nil, errfailedToAcquireHosts
	}
	if respErr := res.GetError(); respErr != nil {
		s.metrics.HostGetFail.Inc(1)
		return nil, errors.New(respErr.String())
	}

	// Get tasks running on hosts
	var hostTasksMap map[string]*resmgrsvc.TaskList
	if len(res.Hosts) > 0 {
		hostTasksMap, err = s.getTasks(ctx, res.Hosts, task.Type)
		if err != nil {
			log.WithFields(log.Fields{
				"hosts":     hosts,
				"filter":    filter,
				"task_type": task.Type,
			}).Info(_failedToFetchTasksOnHosts)
			s.metrics.HostGetFail.Inc(1)
			return nil, err
		}
	}
	s.metrics.HostGet.Inc(1)
	return s.fillTasksInHost(res.GetHosts(), hostTasksMap), nil
}

// fillTasksInHost creates host Info into placement hosts.
// One key notion is to add already running tasks on this host
// such that placement can take care of task-task affinity.
func (s *service) fillTasksInHost(
	hosts []*hostsvc.HostInfo,
	tasks map[string]*resmgrsvc.TaskList) []*models_v0.Host {
	placementHosts := make([]*models_v0.Host, 0, len(hosts))
	for _, host := range hosts {
		var taskList []*resmgr.Task
		if tasks != nil && tasks[host.Hostname] != nil {
			taskList = tasks[host.Hostname].Tasks
		}
		placementHosts = append(placementHosts, models_v0.NewHosts(host, taskList))
	}

	return placementHosts
}

// getTasks returns the tasks running on provided host from resource manager.
func (s *service) getTasks(
	ctx context.Context,
	hostsInfo []*hostsvc.HostInfo,
	taskType resmgr.TaskType) (map[string]*resmgrsvc.TaskList, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	// Extract the hostname
	hostnames := make([]string, 0, len(hostsInfo))
	for _, hostInfo := range hostsInfo {
		hostnames = append(hostnames, hostInfo.Hostname)
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

// ReserveHost reserves the given host for the given task in Host Manager
func (s *service) ReserveHost(ctx context.Context,
	hosts []*models_v0.Host,
	task *resmgr.Task) error {
	if len(hosts) <= 0 {
		return errNoValidHosts
	}
	if task == nil {
		return errNoValidTask
	}

	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	req := &hostsvc.ReserveHostsRequest{
		Reservation: &hostsvc.Reservation{
			Task:  task,
			Hosts: HostInfoToHostModel(hosts),
		},
	}
	resp, err := s.hostManager.ReserveHosts(ctx, req)
	if err != nil {
		return err
	}

	if resp.GetError() != nil {
		return errors.New(resp.GetError().String())
	}
	return nil
}

// HostInfoToHostModel returns the array of models_v0.Host to hostsvc.HostInfo
func HostInfoToHostModel(hostModels []*models_v0.Host) []*hostsvc.HostInfo {
	hInfos := make([]*hostsvc.HostInfo, 0, len(hostModels))
	for _, hModel := range hostModels {
		hInfos = append(hInfos, hModel.Host)
	}
	return hInfos
}

// GetCompletedReservation gets the completed reservation from host manager
func (s *service) GetCompletedReservation(ctx context.Context,
) ([]*hostsvc.CompletedReservation, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	completedResp, err := s.hostManager.GetCompletedReservations(
		ctx,
		&hostsvc.GetCompletedReservationRequest{},
	)

	if err != nil {
		return nil, err
	}
	if completedResp.GetError() != nil {
		return nil, errors.New(completedResp.GetError().GetNotFound().GetMessage())
	}
	return completedResp.GetCompletedReservations(), nil
}
