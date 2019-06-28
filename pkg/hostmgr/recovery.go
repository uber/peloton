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
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/queue"
	"go.uber.org/multierr"

	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

// recoveryHandler restores the contents of MaintenanceQueue
// from Mesos Maintenance Status
type recoveryHandler struct {
	metrics                *metrics.Metrics
	maintenanceQueue       queue.MaintenanceQueue
	masterOperatorClient   mpb.MasterOperatorClient
	maintenanceHostInfoMap host.MaintenanceHostInfoMap
}

// NewRecoveryHandler creates a recoveryHandler
func NewRecoveryHandler(parent tally.Scope,
	maintenanceQueue queue.MaintenanceQueue,
	masterOperatorClient mpb.MasterOperatorClient,
	maintenanceHostInfoMap host.MaintenanceHostInfoMap) RecoveryHandler {
	recovery := &recoveryHandler{
		metrics:                metrics.NewMetrics(parent),
		maintenanceQueue:       maintenanceQueue,
		masterOperatorClient:   masterOperatorClient,
		maintenanceHostInfoMap: maintenanceHostInfoMap,
	}
	return recovery
}

// Stop is a no-op for recovery handler
func (r *recoveryHandler) Stop() error {
	log.Info("Stopping recovery")
	return nil
}

// Start requeues all 'DRAINING' hosts into maintenance queue
func (r *recoveryHandler) Start() error {
	err := r.recoverMaintenanceState()
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		return err
	}

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}

func (r *recoveryHandler) recoverMaintenanceState() error {
	// Clear contents of maintenance queue before
	// enqueuing, to ensure removal of stale data
	r.maintenanceQueue.Clear()

	response, err := r.masterOperatorClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}

	clusterStatus := response.GetStatus()
	if clusterStatus == nil {
		log.Info("Empty maintenance status received")
		return nil
	}

	var drainingHosts []string
	var hostInfos []*hpb.HostInfo
	for _, drainingMachine := range clusterStatus.GetDrainingMachines() {
		machineID := drainingMachine.GetId()
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: machineID.GetHostname(),
				Ip:       machineID.GetIp(),
				State:    hpb.HostState_HOST_STATE_DRAINING,
			})
		drainingHosts = append(drainingHosts, machineID.GetHostname())
	}

	for _, downMachine := range clusterStatus.GetDownMachines() {
		hostInfos = append(hostInfos,
			&hpb.HostInfo{
				Hostname: downMachine.GetHostname(),
				Ip:       downMachine.GetIp(),
				State:    hpb.HostState_HOST_STATE_DOWN,
			})
	}
	r.maintenanceHostInfoMap.ClearAndFillMap(hostInfos)

	var errs error
	for _, drainingHost := range drainingHosts {
		if err := r.maintenanceQueue.Enqueue(drainingHost); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}
