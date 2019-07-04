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
	"fmt"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"

	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/queue"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/multierr"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	maintenanceQueue       queue.MaintenanceQueue
	metrics                *Metrics
	operatorMasterClient   mpb.MasterOperatorClient
	maintenanceHostInfoMap host.MaintenanceHostInfoMap
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	operatorMasterClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue,
	hostInfoMap host.MaintenanceHostInfoMap) {
	handler := &serviceHandler{
		maintenanceQueue:       maintenanceQueue,
		metrics:                NewMetrics(parent.SubScope("hostsvc")),
		operatorMasterClient:   operatorMasterClient,
		maintenanceHostInfoMap: hostInfoMap,
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
	request *host_svc.QueryHostsRequest) (*host_svc.QueryHostsResponse, error) {
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
	drainingHostsInfo := m.maintenanceHostInfoMap.GetDrainingHostInfos([]string{})
	downHostsInfo := m.maintenanceHostInfoMap.GetDownHostInfos([]string{})
	for _, hostState := range hostStateSet.ToSlice() {
		switch hostState {
		case hpb.HostState_HOST_STATE_UP.String():
			upHosts, err := buildHostInfoForRegisteredAgents()
			if err != nil {
				m.metrics.QueryHostsFail.Inc(1)
				return nil, yarpcerrors.InternalErrorf(err.Error())
			}
			// Remove draining and down hosts from the result.
			// This is needed because AgentMap is updated every 15s
			// and might not have the up to date information.
			for _, hostInfo := range drainingHostsInfo {
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
		case hpb.HostState_HOST_STATE_DOWN.String():
			for _, hostInfo := range downHostsInfo {
				hostInfos = append(hostInfos, hostInfo)
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
		if yarpcerrors.IsYARPCError(err) {
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
	// Validate requested host is registered in UP state
	if !host.IsHostUp(hostname) {
		m.metrics.StartMaintenanceFail.Inc(1)
		return yarpcerrors.NotFoundErrorf("Host is not registered as an UP agent")
	}

	machineID, err := buildMachineIDForHost(hostname)
	if err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return err
	}

	// Get current maintenance schedule
	response, err := m.operatorMasterClient.GetMaintenanceSchedule()
	if err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return err
	}
	schedule := response.GetSchedule()
	// Set current time as the `start` of maintenance window
	nanos := time.Now().UnixNano()

	// The maintenance duration has no real significance. A machine can be put into
	// maintenance even after its maintenance window has passed. According to Mesos,
	// omitting the duration means that the unavailability will last forever. Since
	// we do not know the duration, we are omitting it.

	// Construct updated maintenance window including new host
	maintenanceWindow := &mesos_maintenance.Window{
		MachineIds: []*mesos.MachineID{machineID},
		Unavailability: &mesos.Unavailability{
			Start: &mesos.TimeInfo{
				Nanoseconds: &nanos,
			},
		},
	}
	schedule.Windows = append(schedule.Windows, maintenanceWindow)

	// Post updated maintenance schedule
	if err = m.operatorMasterClient.UpdateMaintenanceSchedule(schedule); err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return err
	}
	log.WithField("maintenance_schedule", schedule).
		Info("Maintenance Schedule posted to Mesos Master")

	hostInfo := &hpb.HostInfo{
		Hostname: machineID.GetHostname(),
		Ip:       machineID.GetIp(),
		State:    hpb.HostState_HOST_STATE_DRAINING,
	}
	m.maintenanceHostInfoMap.AddHostInfo(hostInfo)
	// Enqueue hostname into maintenance queue to initiate
	// the rescheduling of tasks running on this host
	if err = m.maintenanceQueue.Enqueue(hostInfo.Hostname); err != nil {
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
	// Get all DOWN hosts and validate requested host is in DOWN state
	downHostInfoMap := make(map[string]*hpb.HostInfo)
	for _, hostInfo := range m.maintenanceHostInfoMap.GetDownHostInfos([]string{}) {
		downHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	hostInfo, ok := downHostInfoMap[hostname]
	if !ok {
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return yarpcerrors.NotFoundErrorf("Host is not DOWN")
	}

	// Stop Maintenance for the host on Mesos Master
	machineID := &mesos.MachineID{
		Hostname: &hostInfo.Hostname,
		Ip:       &hostInfo.Ip,
	}
	if err := m.operatorMasterClient.StopMaintenance([]*mesos.MachineID{machineID}); err != nil {
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return err
	}

	m.maintenanceHostInfoMap.RemoveHostInfo(hostInfo.Hostname)

	m.metrics.CompleteMaintenanceSuccess.Inc(1)
	return nil
}

// Build host info for registered agents
func buildHostInfoForRegisteredAgents() (map[string]*hpb.HostInfo, error) {
	agentMap := host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return nil, nil
	}
	upHosts := make(map[string]*hpb.HostInfo)
	for _, agent := range agentMap.RegisteredAgents {
		hostname := agent.GetAgentInfo().GetHostname()
		agentIP, _, err := util.ExtractIPAndPortFromMesosAgentPID(
			agent.GetPid())
		if err != nil {
			return nil, err
		}
		hostInfo := &hpb.HostInfo{
			Hostname: hostname,
			Ip:       agentIP,
			State:    hpb.HostState_HOST_STATE_UP,
		}
		upHosts[hostname] = hostInfo
	}
	return upHosts, nil
}

// Build machine ID for a specified host
func buildMachineIDForHost(hostname string) (*mesos.MachineID, error) {
	agentMap := host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return nil, fmt.Errorf("no registered agents")
	}
	if _, ok := agentMap.RegisteredAgents[hostname]; !ok {
		return nil, fmt.Errorf("unknown host %s", hostname)
	}
	pid := agentMap.RegisteredAgents[hostname].GetPid()
	ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(pid)
	if err != nil {
		return nil, err
	}
	machineID := &mesos.MachineID{
		Hostname: &hostname,
		Ip:       &ip,
	}
	return machineID, nil
}
