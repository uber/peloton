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

package hostsvc

import (
	"context"

	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/host/drainer"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover"
	hostpool_mgr "github.com/uber/peloton/pkg/hostmgr/hostpool/manager"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	metrics         *Metrics
	drainer         drainer.Drainer
	hostPoolManager hostpool_mgr.HostPoolManager
	hostMover       hostmover.HostMover
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	drainer drainer.Drainer,
	hostPoolManager hostpool_mgr.HostPoolManager,
	hostMover hostmover.HostMover) {
	handler := &serviceHandler{
		metrics:         NewMetrics(parent.SubScope("hostsvc")),
		drainer:         drainer,
		hostPoolManager: hostPoolManager,
		hostMover:       hostMover,
	}
	d.Register(host_svc.BuildHostServiceYARPCProcedures(handler))
	log.Info("Hostsvc handler initialized")
}

// QueryHosts returns the hosts which are in one of the specified states.
// A host, at any given time, will be in one of the following states
// 		1.HostState_HOST_STATE_UP - The host is up and running
// 		2.HostState_HOST_STATE_DRAINING - The tasks running on the host are being rescheduled and
// 										  there will be no further placement of tasks on the host
//		3.HostState_HOST_STATE_DRAINED - There are no tasks running on this host and it is ready to be 'DOWN'ed
// 		4.HostState_HOST_STATE_DOWN - The host is in maintenance.
func (m *serviceHandler) QueryHosts(
	ctx context.Context,
	request *host_svc.QueryHostsRequest,
) (*host_svc.QueryHostsResponse, error) {
	m.metrics.QueryHostsAPI.Inc(1)

	// Add request.HostStates to a set to remove duplicates
	hostStateSet := stringset.New()
	for _, state := range request.GetHostStates() {
		hostStateSet.Add(state.String())
	}

	if request.HostStates == nil || len(request.HostStates) == 0 {
		for _, state := range hpb.HostState_name {
			hostStateSet.Add(state)
		}
	}

	var hostInfos []*hpb.HostInfo
	drainingHostsInfo, err := m.drainer.GetAllDrainingHostInfos()
	if err != nil {
		return nil, err
	}
	drainedHostsInfo, err := m.drainer.GetAllDrainedHostInfos()
	if err != nil {
		return nil, err
	}
	downHostsInfo, err := m.drainer.GetAllDownHostInfos()
	if err != nil {
		return nil, err
	}
	for _, hostState := range hostStateSet.ToSlice() {
		switch hostState {
		case hpb.HostState_HOST_STATE_UP.String():
			upHosts, err := host.BuildHostInfoForRegisteredAgents()
			if err != nil {
				m.metrics.QueryHostsFail.Inc(1)
				return nil, yarpcerrors.InternalErrorf(err.Error())
			}
			// Remove draining / drained / down hosts from the result.
			// This is needed because AgentMap is updated every 15s
			// and might not have the up to date information.
			for _, hostInfo := range drainingHostsInfo {
				delete(upHosts, hostInfo.GetHostname())
			}
			for _, hostInfo := range drainedHostsInfo {
				delete(upHosts, hostInfo.GetHostname())
			}
			for _, hostInfo := range downHostsInfo {
				delete(upHosts, hostInfo.GetHostname())
			}

			for _, hostInfo := range upHosts {
				hostInfos = append(hostInfos, hostInfo)
			}
		case hpb.HostState_HOST_STATE_DRAINING.String():
			for _, hostInfo := range drainingHostsInfo {
				hostInfos = append(hostInfos, hostInfo)
			}
		case hpb.HostState_HOST_STATE_DRAINED.String():
			for _, hostInfo := range drainedHostsInfo {
				hostInfos = append(hostInfos, hostInfo)
			}
		case hpb.HostState_HOST_STATE_DOWN.String():
			for _, hostInfo := range downHostsInfo {
				hostInfos = append(hostInfos, hostInfo)
			}
		}
	}
	if m.hostPoolManager != nil {
		for _, h := range hostInfos {
			p, err := m.hostPoolManager.GetPoolByHostname(h.GetHostname())
			if err == nil {
				h.CurrentPool = p.ID()
			}
			d, err := m.hostPoolManager.GetDesiredPool(h.GetHostname())
			if err == nil {
				h.DesiredPool = d
			}
		}
	}

	m.metrics.QueryHostsSuccess.Inc(1)
	return &host_svc.QueryHostsResponse{
		HostInfos: hostInfos,
	}, nil
}

// StartMaintenance puts the host(s) into DRAINING state by posting a maintenance
// schedule to Mesos Master. Inverse offers are sent out and all future offers
// from the(se) host(s) are tagged with unavailability (Please check Mesos
// Maintenance Primitives for more info). The hosts are first drained of tasks
// before they are put into maintenance by posting to /machine/down endpoint of
// Mesos Master. The hosts transition from UP to DRAINING and finally to DOWN.
func (m *serviceHandler) StartMaintenance(
	ctx context.Context,
	request *host_svc.StartMaintenanceRequest,
) (*host_svc.StartMaintenanceResponse, error) {
	// StartMaintenanceRequest using deprecated field `hostnames`
	var errs error
	if len(request.GetHostnames()) != 0 {
		for _, hostname := range request.GetHostnames() {
			if err := m.startMaintenance(ctx, hostname); err != nil {
				// Not error out on 1rst error, continue and aggregate errors
				errs = multierr.Append(errs, err)
			}
		}
		if errs != nil {
			return nil, yarpcerrors.InternalErrorf(errs.Error())
		}
		return &host_svc.StartMaintenanceResponse{}, nil
	}
	// StartMaintenanceRequest using prefered field `hostname`
	if err := m.startMaintenance(ctx, request.GetHostname()); err != nil {
		if yarpcerrors.IsStatus(err) {
			// Allow YARPC NotFound error to be returned as such
			return nil, err
		}
		return nil, yarpcerrors.InternalErrorf(err.Error())
	}
	return &host_svc.StartMaintenanceResponse{
		Hostname: request.GetHostname(),
	}, nil
}

func (m *serviceHandler) startMaintenance(
	ctx context.Context,
	hostname string,
) error {
	m.metrics.StartMaintenanceAPI.Inc(1)
	if err := m.drainer.StartMaintenance(ctx, hostname); err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return err
	}
	m.metrics.StartMaintenanceSuccess.Inc(1)
	return nil
}

// CompleteMaintenance completes maintenance on the specified hosts. It brings
// UP a host which is in maintenance by posting to /machine/up endpoint of
// Mesos Master i.e. the machine transitions from DOWN to UP state
// (Please check Mesos Maintenance Primitives for more info)
func (m *serviceHandler) CompleteMaintenance(
	ctx context.Context,
	request *host_svc.CompleteMaintenanceRequest,
) (*host_svc.CompleteMaintenanceResponse, error) {
	// CompleteMaintenanceRequest using deprecated field `hostnames`
	var errs error
	if len(request.GetHostnames()) != 0 {
		for _, hostname := range request.GetHostnames() {
			if err := m.completeMaintenance(ctx, hostname); err != nil {
				// Not error out on 1rst error, continue and aggregate errors
				errs = multierr.Append(errs, err)
			}
		}
		if errs != nil {
			return nil, yarpcerrors.InternalErrorf(errs.Error())
		}
		return &host_svc.CompleteMaintenanceResponse{}, nil
	}
	// CompleteMaintenanceRequest using prefered field `hostname`
	if err := m.completeMaintenance(ctx, request.GetHostname()); err != nil {
		if yarpcerrors.IsYARPCError(err) {
			// Allow YARPC NotFound error to be returned as such
			return nil, err
		}
		return nil, yarpcerrors.InternalErrorf(err.Error())
	}
	return &host_svc.CompleteMaintenanceResponse{
		Hostname: request.GetHostname(),
	}, nil
}

func (m *serviceHandler) completeMaintenance(
	ctx context.Context,
	hostname string,
) error {
	m.metrics.CompleteMaintenanceAPI.Inc(1)
	if err := m.drainer.CompleteMaintenance(ctx, hostname); err != nil {
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return err
	}

	m.metrics.CompleteMaintenanceSuccess.Inc(1)
	return nil
}

// List all host pools
func (m *serviceHandler) ListHostPools(
	ctx context.Context,
	request *host_svc.ListHostPoolsRequest,
) (response *host_svc.ListHostPoolsResponse, err error) {
	if m.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	allPools := m.hostPoolManager.Pools()
	response = &host_svc.ListHostPoolsResponse{
		Pools: make([]*hpb.HostPoolInfo, 0, len(allPools)),
	}
	for _, pool := range allPools {
		poolHosts := pool.Hosts()
		info := &hpb.HostPoolInfo{
			Name:  pool.ID(),
			Hosts: make([]string, 0, len(poolHosts)),
		}
		for h := range poolHosts {
			info.Hosts = append(info.Hosts, h)
		}
		response.Pools = append(response.Pools, info)
	}
	return
}

// Create a host pool
func (m *serviceHandler) CreateHostPool(
	ctx context.Context,
	request *host_svc.CreateHostPoolRequest,
) (response *host_svc.CreateHostPoolResponse, err error) {
	if m.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	name := request.GetName()
	if name == "" {
		err = yarpcerrors.InvalidArgumentErrorf("name")
		return
	}
	if _, err1 := m.hostPoolManager.GetPool(name); err1 != nil {
		m.hostPoolManager.RegisterPool(name)
		response = &host_svc.CreateHostPoolResponse{}
	} else {
		err = yarpcerrors.AlreadyExistsErrorf("")
	}
	return
}

// Delete a host pool
func (m *serviceHandler) DeleteHostPool(
	ctx context.Context,
	request *host_svc.DeleteHostPoolRequest,
) (response *host_svc.DeleteHostPoolResponse, err error) {
	if m.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	name := request.GetName()
	if name == common.DefaultHostPoolID {
		err = yarpcerrors.InvalidArgumentErrorf("default pool")
	} else if _, err1 := m.hostPoolManager.GetPool(name); err1 != nil {
		err = yarpcerrors.NotFoundErrorf("")
	} else {
		m.hostPoolManager.DeregisterPool(name)
		response = &host_svc.DeleteHostPoolResponse{}
	}
	return
}

// Change the pool of a host
func (m *serviceHandler) ChangeHostPool(
	ctx context.Context,
	request *host_svc.ChangeHostPoolRequest,
) (response *host_svc.ChangeHostPoolResponse, err error) {
	if m.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	err = m.hostPoolManager.ChangeHostPool(
		request.GetHostname(),
		request.GetSourcePool(),
		request.GetDestinationPool(),
	)
	if err == nil {
		response = &host_svc.ChangeHostPoolResponse{}
	}
	return
}

// MoveHosts from source pool to destination
func (m *serviceHandler) MoveHosts(
	ctx context.Context,
	request *host_svc.MoveHostsRequest,
) (response *host_svc.MoveHostsResponse, err error) {
	if m.hostPoolManager == nil {
		err = yarpcerrors.UnimplementedErrorf("host pools not enabled")
		return
	}
	err = m.hostMover.MoveHosts(
		ctx,
		request.GetSourcePool(),
		request.GetSrcPoolDesiredHosts(),
		request.GetDestinationPool(),
		request.GetDestPoolDesiredHosts(),
	)
	if err == nil {
		response = &host_svc.MoveHostsResponse{}
	}
	return
}
