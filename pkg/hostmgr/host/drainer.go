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

package host

import (
	"context"
	"fmt"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/queue"

	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
	"go.uber.org/yarpc/yarpcerrors"
)

// Drainer defines the interface for host drainer
type Drainer interface {
	// Start the module
	Start()

	// Stop the module
	Stop()

	// GetDrainingHostInfos returns HostInfo of the specified DRAINING hosts.
	// If the hostFilter is empty then HostInfos of all DRAINING hosts
	// is returned.
	GetDrainingHostInfos(hostFilter []string) []*pbhost.HostInfo

	// GetDownHostInfos returns HostInfo of the specified DOWN hosts.
	// If the hostFilter is empty then HostInfos of all DOWN hosts
	// is returned.
	GetDownHostInfos(hostFilter []string) []*pbhost.HostInfo

	// StartMaintenance initiates putting a host into maintenance.
	StartMaintenance(ctx context.Context, hostname string) error

	// CompleteMaintenance brings a host out of maintenance.
	CompleteMaintenance(ctx context.Context, hostname string) error
}

// drainer defines the host drainer which drains
// the hosts which are to be put into maintenance
type drainer struct {
	drainerPeriod          time.Duration
	pelotonAgentRole       string
	masterOperatorClient   mpb.MasterOperatorClient
	maintenanceQueue       queue.MaintenanceQueue
	lifecycle              lifecycle.LifeCycle // lifecycle manager
	maintenanceHostInfoMap MaintenanceHostInfoMap
}

// NewDrainer creates a new host drainer
func NewDrainer(
	drainerPeriod time.Duration,
	pelotonAgentRole string,
	masterOperatorClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue,
	hostInfoMap MaintenanceHostInfoMap,
) Drainer {
	return &drainer{
		drainerPeriod:          drainerPeriod,
		pelotonAgentRole:       pelotonAgentRole,
		masterOperatorClient:   masterOperatorClient,
		maintenanceQueue:       maintenanceQueue,
		lifecycle:              lifecycle.NewLifeCycle(),
		maintenanceHostInfoMap: hostInfoMap,
	}
}

// Start starts the host drainer
func (d *drainer) Start() {
	if !d.lifecycle.Start() {
		log.Warn("drainer is already started, no" +
			" action will be performed")
		return
	}

	go func() {
		defer d.lifecycle.StopComplete()

		ticker := time.NewTicker(d.drainerPeriod)
		defer ticker.Stop()

		log.Info("Starting Host drainer")

		for {
			select {
			case <-d.lifecycle.StopCh():
				log.Info("Exiting Host drainer")
				return
			case <-ticker.C:
				err := d.reconcileMaintenanceState()
				if err != nil {
					log.WithError(err).
						Warn("Maintenance state reconciliation unsuccessful")
				}
			}
		}
	}()
}

// Stop stops the host drainer
func (d *drainer) Stop() {
	if !d.lifecycle.Stop() {
		log.Warn("drainer is already stopped, no" +
			" action will be performed")
		return
	}
	// Wait for drainer to be stopped
	d.lifecycle.Wait()
	log.Info("drainer stopped")
}

func (d *drainer) reconcileMaintenanceState() error {
	response, err := d.masterOperatorClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}
	var drainingHosts []string
	var hostInfos []*pbhost.HostInfo
	for _, drainingMachine := range response.GetStatus().GetDrainingMachines() {
		machineID := drainingMachine.GetId()
		hostInfos = append(hostInfos,
			&pbhost.HostInfo{
				Hostname: machineID.GetHostname(),
				Ip:       machineID.GetIp(),
				State:    pbhost.HostState_HOST_STATE_DRAINING,
			})
		drainingHosts = append(drainingHosts, machineID.GetHostname())
	}

	for _, downMachine := range response.GetStatus().GetDownMachines() {
		hostInfos = append(hostInfos,
			&pbhost.HostInfo{
				Hostname: downMachine.GetHostname(),
				Ip:       downMachine.GetIp(),
				State:    pbhost.HostState_HOST_STATE_DOWN,
			})
	}
	d.maintenanceHostInfoMap.ClearAndFillMap(hostInfos)

	var errs error

	for _, drainingHost := range drainingHosts {
		if err := d.maintenanceQueue.Enqueue(drainingHost); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

// StartMaintenance puts the host(s) into DRAINING state by posting a maintenance
// schedule to Mesos Master.
func (d *drainer) StartMaintenance(
	ctx context.Context,
	hostname string,
) error {
	if !d.isPelotonAgent(hostname) {
		return yarpcerrors.AbortedErrorf("host is not a Peloton agent")
	}

	machineID, err := buildMachineIDForHost(hostname)
	if err != nil {
		return err
	}

	// Get current maintenance schedule
	response, err := d.masterOperatorClient.GetMaintenanceSchedule()
	if err != nil {
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
	maintenanceWindow := &mesosmaintenance.Window{
		MachineIds: []*mesos.MachineID{machineID},
		Unavailability: &mesos.Unavailability{
			Start: &mesos.TimeInfo{
				Nanoseconds: &nanos,
			},
		},
	}
	schedule.Windows = append(schedule.Windows, maintenanceWindow)

	// Post updated maintenance schedule
	if err = d.masterOperatorClient.UpdateMaintenanceSchedule(schedule); err != nil {
		return err
	}
	log.WithField("maintenance_schedule", schedule).
		Info("Maintenance Schedule posted to Mesos Master")

	hostInfo := &pbhost.HostInfo{
		Hostname: machineID.GetHostname(),
		Ip:       machineID.GetIp(),
		State:    pbhost.HostState_HOST_STATE_DRAINING,
	}
	d.maintenanceHostInfoMap.AddHostInfo(hostInfo)
	// Enqueue hostname into maintenance queue to initiate
	// the rescheduling of tasks running on this host
	if err = d.maintenanceQueue.Enqueue(hostInfo.Hostname); err != nil {
		return err
	}

	return nil
}

// CompleteMaintenance brings a host out of maintenance.
func (d *drainer) CompleteMaintenance(
	ctx context.Context,
	hostname string,
) error {
	// Get all DOWN hosts and validate requested host is in DOWN state
	downHostInfoMap := make(map[string]*pbhost.HostInfo)
	for _, hostInfo := range d.maintenanceHostInfoMap.GetDownHostInfos([]string{}) {
		downHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	hostInfo, ok := downHostInfoMap[hostname]
	if !ok {
		return yarpcerrors.NotFoundErrorf("Host is not DOWN")
	}

	// Stop Maintenance for the host on Mesos Master
	machineID := []*mesos.MachineID{
		{
			Hostname: &hostInfo.Hostname,
			Ip:       &hostInfo.Ip,
		},
	}
	if err := d.masterOperatorClient.StopMaintenance(machineID); err != nil {
		return err
	}

	d.maintenanceHostInfoMap.RemoveHostInfo(hostInfo.Hostname)

	return nil
}

// GetDrainingHostInfos returns HostInfo of the specified DRAINING hosts
// If the hostFilter is empty then HostInfos of all DRAINING hosts is returned
func (d *drainer) GetDrainingHostInfos(hostFilter []string) []*pbhost.HostInfo {
	return d.maintenanceHostInfoMap.GetDrainingHostInfos(hostFilter)
}

// GetDownHostInfos returns HostInfo of the specified DOWN hosts
// If the hostFilter is empty then HostInfos of all DOWN hosts is returned
func (d *drainer) GetDownHostInfos(hostFilter []string) []*pbhost.HostInfo {
	return d.maintenanceHostInfoMap.GetDownHostInfos(hostFilter)
}

// isPelotonAgent returns true if the host is registered as Peloton agent
func (d *drainer) isPelotonAgent(hostname string) bool {
	agentInfo := GetAgentInfo(hostname)
	if agentInfo == nil {
		return false
	}

	// For Peloton agents, AgentInfo->Resources->Reservations->Role would be set
	// to Peloton role because of how agents are configured
	// (check mesos.framework in /config/hostmgr/base.yaml)
	for _, r := range agentInfo.GetResources() {
		reservations := r.GetReservations()
		// look at the latest(active) reservation info and check if the
		// agent is registered to peloton framework. This check is necessary
		// to ensure Peloton does not put into maintenance a host which
		// it does not manage.
		if len(reservations) > 0 &&
			reservations[len(reservations)-1].GetRole() == d.pelotonAgentRole {
			return true
		}
	}

	return false
}

// Build machine ID for a specified host
func buildMachineIDForHost(hostname string) (*mesos.MachineID, error) {
	agentMap := GetAgentMap()
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
