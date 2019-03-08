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
	"time"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/queue"

	host "github.com/uber/peloton/.gen/peloton/api/v0/host"

	log "github.com/sirupsen/logrus"
)

// drainer defines the host drainer which drains
// the hosts which are to be put into maintenance
type drainer struct {
	drainerPeriod          time.Duration
	masterOperatorClient   mpb.MasterOperatorClient
	maintenanceQueue       queue.MaintenanceQueue
	lifecycle              lifecycle.LifeCycle // lifecycle manager
	maintenanceHostInfoMap MaintenanceHostInfoMap
}

// Drainer defines the interface for host drainer
type Drainer interface {
	Start()
	Stop()
}

// NewDrainer creates a new host drainer
func NewDrainer(
	drainerPeriod time.Duration,
	masterOperatorClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue,
	hostInfoMap MaintenanceHostInfoMap,
) Drainer {
	return &drainer{
		drainerPeriod:          drainerPeriod,
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
	var hostInfos []*host.HostInfo
	for _, drainingMachine := range response.GetStatus().GetDrainingMachines() {
		machineID := drainingMachine.GetId()
		hostInfos = append(hostInfos,
			&host.HostInfo{
				Hostname: machineID.GetHostname(),
				Ip:       machineID.GetIp(),
				State:    host.HostState_HOST_STATE_DRAINING,
			})
		drainingHosts = append(drainingHosts, machineID.GetHostname())
	}

	for _, downMachine := range response.GetStatus().GetDownMachines() {
		hostInfos = append(hostInfos,
			&host.HostInfo{
				Hostname: downMachine.GetHostname(),
				Ip:       downMachine.GetIp(),
				State:    host.HostState_HOST_STATE_DOWN,
			})
	}
	d.maintenanceHostInfoMap.ClearAndFillMap(hostInfos)
	return d.maintenanceQueue.Enqueue(drainingHosts)
}
