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

package drainer

import (
	"context"
	"time"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/queue"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// Drainer defines the interface for host drainer
type Drainer interface {
	// Start the module
	Start()

	// Stop the module
	Stop()

	// GetAllDrainingHostInfos returns HostInfo of all DRAINING hosts.
	GetAllDrainingHostInfos() ([]*pbhost.HostInfo, error)

	// GetAllDrainedHostInfos returns HostInfo of all DRAINED hosts.
	GetAllDrainedHostInfos() ([]*pbhost.HostInfo, error)

	// GetAllDownHostInfos returns HostInfo of all DOWN hosts.
	GetAllDownHostInfos() ([]*pbhost.HostInfo, error)

	// StartMaintenance initiates putting a host into maintenance.
	StartMaintenance(ctx context.Context, hostname string) error

	// CompleteMaintenance brings a host out of maintenance.
	CompleteMaintenance(ctx context.Context, hostname string) error
}

// drainer defines the host drainer which drains
// the hosts which are to be put into maintenance
type drainer struct {
	drainerPeriod        time.Duration
	pelotonAgentRole     string
	masterOperatorClient mpb.MasterOperatorClient
	lifecycle            lifecycle.LifeCycle // lifecycle manager
	goalStateDriver      goalstate.Driver
	hostInfoOps          ormobjects.HostInfoOps // DB ops for host_info table
	taskEvictionQueue    queue.TaskQueue
}

// NewDrainer creates a new host drainer
func NewDrainer(
	drainerPeriod time.Duration,
	pelotonAgentRole string,
	masterOperatorClient mpb.MasterOperatorClient,
	goalStateDriver goalstate.Driver,
	hostInfoOps ormobjects.HostInfoOps,
	taskEvictionQueue queue.TaskQueue,
) Drainer {
	return &drainer{
		drainerPeriod:        drainerPeriod,
		pelotonAgentRole:     pelotonAgentRole,
		masterOperatorClient: masterOperatorClient,
		lifecycle:            lifecycle.NewLifeCycle(),
		goalStateDriver:      goalStateDriver,
		hostInfoOps:          hostInfoOps,
		taskEvictionQueue:    taskEvictionQueue,
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

		// Start goal state driver
		d.goalStateDriver.Start()

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
	// Stop goal state driver
	d.goalStateDriver.Stop()
	// Wait for drainer to be stopped
	d.lifecycle.Wait()
	log.Info("drainer stopped")
}

// reconcileMaintenanceState reconciles host maintenance states between DB and Mesos Master.
// If the 2 states differ, the state read from Mesos Master is persisted in DB
// and the host is enqueued into the host goal state engine to converge
// towards the goal state already defined by the user for that host
func (d *drainer) reconcileMaintenanceState() error {
	ctx := context.Background()

	// Get all hosts in maintenance from DB
	hostInfosFromDB, err := d.hostInfoOps.GetAll(ctx)
	if err != nil {
		return err
	}

	hostInfosFromMesosMaster := make(map[string]*pbhost.HostInfo)

	// Get all hosts in registered state (UP + DRAINING) from Mesos Master
	registeredAgents, err := host.BuildHostInfoForRegisteredAgents()
	if err != nil {
		return err
	}
	for hostname, hostInfo := range registeredAgents {
		hostInfosFromMesosMaster[hostname] = hostInfo
	}

	// Get all hosts in maintenance state from Mesos Master
	response, err := d.masterOperatorClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}

	for _, drainingMachine := range response.GetStatus().GetDrainingMachines() {
		machineID := drainingMachine.GetId()
		// Overwrite the hostInfo if a host was temporary registered as UP
		// while it is actually DRAINING
		hostInfosFromMesosMaster[machineID.GetHostname()] = &pbhost.HostInfo{
			Hostname: machineID.GetHostname(),
			Ip:       machineID.GetIp(),
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		}
	}

	for _, downMachine := range response.GetStatus().GetDownMachines() {
		hostInfosFromMesosMaster[downMachine.GetHostname()] = &pbhost.HostInfo{
			Hostname: downMachine.GetHostname(),
			Ip:       downMachine.GetIp(),
			State:    pbhost.HostState_HOST_STATE_DOWN,
		}
	}

	// For each host in maintenance from DB, its current state should match
	// the current state read from Mesos Master
	// If not, update the host state in DB and enqueue into the goal state
	for _, hFromDB := range hostInfosFromDB {
		hostname := hFromDB.GetHostname()
		hFromMesos, ok := hostInfosFromMesosMaster[hostname]

		// Current state from DB matching current maintenance state in Mesos Master
		if ok && hFromDB.GetState() == hFromMesos.State {
			continue
		}

		// Current state from DB not matching current maintenance state in Mesos Master
		if ok && hFromDB.GetState() != hFromMesos.State {
			log.WithFields(log.Fields{
				"hostname":                hostname,
				"state_from_db":           hFromDB.GetState().String(),
				"state_from_mesos_master": hFromMesos.State.String(),
			}).Info("reconcilation of host maintenance state")
			// Update host state in DB
			// No lock possible so potential race between reconcile and goal state engine
			// but state should converge anyway to the goal state
			if err := d.hostInfoOps.UpdateState(
				ctx,
				hostname,
				hFromMesos.State,
			); err != nil {
				return err
			}
			// Enqueue into host goal state engine
			d.goalStateDriver.EnqueueHost(hostname, time.Now())
			continue
		}
	}
	return nil
}

// StartMaintenance puts the host(s) into DRAINING state by posting a maintenance
// schedule to Mesos Master.
func (d *drainer) StartMaintenance(
	ctx context.Context,
	hostname string,
) error {
	// Validate requested host is a registered peloton agent
	if !host.IsRegisteredAsPelotonAgent(hostname, d.pelotonAgentRole) {
		return yarpcerrors.AbortedErrorf("host is not a Peloton agent")
	}

	// Set goal state to DOWN
	if err := d.hostInfoOps.UpdateGoalState(
		ctx,
		hostname,
		pbhost.HostState_HOST_STATE_DOWN,
	); err != nil {
		return err
	}

	log.WithField("hostname", hostname).Info("start host maintenance")

	// Enqueue into the goal state engine
	d.goalStateDriver.EnqueueHost(hostname, time.Now())

	return nil
}

// CompleteMaintenance brings a host out of maintenance.
func (d *drainer) CompleteMaintenance(
	ctx context.Context,
	hostname string,
) error {
	// Get host from DB
	hostInfo, err := d.hostInfoOps.Get(ctx, hostname)
	if err != nil {
		return err
	}

	// Only a host in state DOWN is allowed to be requested for maintenance completion
	if hostInfo.GetState() != pbhost.HostState_HOST_STATE_DOWN {
		return yarpcerrors.NotFoundErrorf("Host is not DOWN")
	}

	// Set host goal state is UP
	if err := d.hostInfoOps.UpdateGoalState(
		ctx,
		hostname,
		pbhost.HostState_HOST_STATE_UP); err != nil {
		return err
	}

	log.WithField("hostname", hostname).Info("complete host maintenance")

	// Enqueue into the goal state engine
	d.goalStateDriver.EnqueueHost(hostname, time.Now())

	return nil
}

// GetAllDrainingHostInfos returns HostInfo of all DRAINING hosts
func (d *drainer) GetAllDrainingHostInfos() ([]*pbhost.HostInfo, error) {
	return d.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DRAINING)
}

// GetAllDrainedHostInfos returns HostInfo of all DRAINED hosts
func (d *drainer) GetAllDrainedHostInfos() ([]*pbhost.HostInfo, error) {
	return d.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DRAINED)
}

// GetAllDownHostInfos returns HostInfo of all DOWN hosts
func (d *drainer) GetAllDownHostInfos() ([]*pbhost.HostInfo, error) {
	return d.getAllHostInfosPerState(pbhost.HostState_HOST_STATE_DOWN)
}

// getAllHostInfosPerState returns HostInfo of all hosts for a state
func (d *drainer) getAllHostInfosPerState(state pbhost.HostState) ([]*pbhost.HostInfo, error) {
	// Read host infos in maintenance from DB
	// TODO: replace with read from host cache
	ctx := context.Background()
	hostInfosInMaintenance, err := d.hostInfoOps.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	// Build results
	var results []*pbhost.HostInfo
	for _, h := range hostInfosInMaintenance {
		if h.GetState() == state {
			results = append(results, h)
		}
	}
	return results, nil
}
