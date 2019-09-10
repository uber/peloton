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

package goalstate

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/host/mesoshelper"
)

// HostUntrack untracks the host by removing it
// from the goal state engine's entity map
func HostUntrack(ctx context.Context, entity goalstate.Entity) error {
	hostEntity := entity.(*hostEntity)
	gsDriver := hostEntity.driver
	gsDriver.DeleteHost(hostEntity.hostname)
	return nil
}

// HostRequeue re-enqueues the host into the goal state engine
// This is needed as host entity GetState() & GetGoalState() are reads from DB
// which can error out (e.g. DB unavailable), the retry mechanism is handled by
// re-enqueuing into the goal state engine.
// TODO: remove once host state & goalState backed by host cache
func HostRequeue(ctx context.Context, entity goalstate.Entity) error {
	hostEntity := entity.(*hostEntity)
	gsDriver := hostEntity.driver
	gsDriver.DeleteHost(hostEntity.hostname)

	// Since RequeueHost is called when entity state and/or goal state is invalid
	// due to DB failure, sleeping acts as a static backoff before attempting
	// new DB read
	time.Sleep(1 * time.Minute)

	gsDriver.EnqueueHost(hostEntity.hostname, time.Now())
	return nil
}

// HostDrain is an idempotent action enqueuing the host
// for draining of its tasks
func HostDrain(ctx context.Context, entity goalstate.Entity) error {
	hostEntity := entity.(*hostEntity)
	hostname := hostEntity.hostname
	gsDriver := hostEntity.driver

	// Get IP from host record in DB
	h, err := gsDriver.hostInfoOps.Get(ctx, hostname)
	if err != nil {
		return err
	}
	IP := h.GetIp()

	// Taking lock to update Mesos Master maintenance schedule
	// This is required as each host action HostDrain appends
	// a new host to the schedule on top of the existing schedule
	// and Mesos Master allows schedule overwrite, so concurrency control
	// is required
	defer func() {
		gsDriver.maintenanceScheduleLock.Unlock()
		log.WithField("hostname", hostname).
			Debug("host releases lock for updating mesos master maintenance schedule")
	}()

	gsDriver.maintenanceScheduleLock.Lock()

	log.WithField("hostname", hostname).
		Debug("host takes lock for updating mesos master maintenance schedule")

	if err := mesoshelper.AddHostToMaintenanceSchedule(gsDriver.mesosMasterClient, hostname, IP); err != nil {
		return err
	}

	// Update host state to DRAINING in DB
	if err := gsDriver.hostInfoOps.UpdateState(
		ctx,
		hostname,
		hpb.HostState_HOST_STATE_DRAINING); err != nil {
		return err
	}

	// No need to enqueue into goal state as the host needs to first be fully drained
	// to be then updated to drained state and enqueued again into
	// the goal state engine

	log.WithFields(log.Fields{
		"hostname":    hostname,
		"action_name": "HostDrain",
	}).Info("goal state action succeeded")

	return nil
}

// HostDown is an idempotent action registering the host as DOWN in maintenance
func HostDown(ctx context.Context, entity goalstate.Entity) error {
	hostEntity := entity.(*hostEntity)
	hostname := hostEntity.hostname
	gsDriver := hostEntity.driver

	// Get IP from host record in DB
	h, err := gsDriver.hostInfoOps.Get(ctx, hostname)
	if err != nil {
		return err
	}
	IP := h.GetIp()

	// Register host as down
	if err := mesoshelper.RegisterHostAsDown(gsDriver.mesosMasterClient, hostname, IP); err != nil {
		return err
	}

	// Update host current state to DOWN in DB
	if err = gsDriver.hostInfoOps.UpdateState(
		ctx,
		hostname,
		hpb.HostState_HOST_STATE_DOWN); err != nil {
		return err
	}

	// Enqueue into goal state engine for host untracking since
	// state converged to goal state
	gsDriver.EnqueueHost(hostname, time.Now())

	log.WithFields(log.Fields{
		"hostname":    hostname,
		"action_name": "HostDown",
	}).Info("goal state action succeeded")

	return nil
}

// HostUp is an idempotent action registering the host as UP, out of maintenance
func HostUp(ctx context.Context, entity goalstate.Entity) error {
	hostEntity := entity.(*hostEntity)
	hostname := hostEntity.hostname
	gsDriver := hostEntity.driver

	// Get IP from host record in DB
	h, err := gsDriver.hostInfoOps.Get(ctx, hostname)
	if err != nil {
		return err
	}
	IP := h.GetIp()

	// Register host as up
	if err := mesoshelper.RegisterHostAsUp(gsDriver.mesosMasterClient, hostname, IP); err != nil {
		return err
	}

	// Delete host from DB
	if err := gsDriver.hostInfoOps.Delete(ctx, hostname); err != nil {
		return err
	}

	// Since host deleted from DB, also delete it from goal state engine entity map
	gsDriver.DeleteHost(hostname)

	log.WithFields(log.Fields{
		"hostname":    hostname,
		"action_name": "HostUp",
	}).Info("goal state action succeeded")

	return nil
}
