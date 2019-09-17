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

package manager

import (
	"context"
	"sync"
	"time"

	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	"github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_defaultReconcileInterval = 10 * time.Second

	_updateHostPoolErrMsg = "failed to update host pool in host_infos table"
)

// HostPoolManager provides abstraction to manage host pools of a cluster.
type HostPoolManager interface {
	// GetPool gets a host pool from cache by given pool id.
	GetPool(poolID string) (hostpool.HostPool, error)

	// Pools returns all host pools from cache.
	Pools() map[string]hostpool.HostPool

	// GetPoolByHostname returns the host pool given host belongs to.
	GetPoolByHostname(hostname string) (hostpool.HostPool, error)

	// RegisterPool creates a host pool with given ID if not exists.
	RegisterPool(poolID string)

	// DeregisterPool deletes existing host pool with given ID.
	DeregisterPool(poolID string) error

	// ChangeHostPool changes host pool of given host from source pool to
	// destination pool.
	ChangeHostPool(hostname, srcPool, destPool string) error

	// GetDesiredPool gets desired pool of given host in db.
	GetDesiredPool(hostname string) (string, error)

	// UpdateDesiredPool updates desired pool of given host in db.
	UpdateDesiredPool(hostname, poolID string) error

	// Start starts the host pool cache go routine that reconciles host pools.
	// It returns error if failed to recover host pool data from db.
	Start() error

	// Stop stops the host pool cache go routine that reconciles host pools.
	Stop()
}

// hostPoolManager implements HostPoolManager interface.
// it ensures:
// - host pool cache is consistent with db.
// - host pool cache is consistent with host cache in host manager
//   for recovery from restart etc.
// - every host in the cluster belongs to, and only belongs to ONE host pool.
// TODO: Use new host cache once it merges with agent map.
type hostPoolManager struct {
	mu sync.RWMutex

	// reconcileInternal defines how frequently host pool manager reconciles
	// host pool cache.
	reconcileInternal time.Duration

	// event stream handler
	eventStreamHandler *eventstream.Handler

	// hostInfoOps is the interface to update host_infos table.
	hostInfoOps objects.HostInfoOps

	// poolIndex is map from host pool id to host pool.
	poolIndex map[string]hostpool.HostPool

	// hostToPoolMap is map from hostname to id of host pool it belongs to.
	hostToPoolMap map[string]string

	// Lifecycle manager.
	lifecycle lifecycle.LifeCycle

	// Parent scope of host pool manager metrics.
	parentScope tally.Scope

	// Metrics.
	metrics *Metrics
}

// New returns a host pool manager instance.
func New(
	reconcileInternal time.Duration,
	eventStreamHandler *eventstream.Handler,
	hostInfoOps objects.HostInfoOps,
	parentScope tally.Scope,
) HostPoolManager {
	// If provided reconcile interval is less or equal to zero,
	// use default reconcile interval.
	if reconcileInternal <= 0 {
		reconcileInternal = _defaultReconcileInterval
	}
	manager := &hostPoolManager{
		reconcileInternal:  reconcileInternal,
		eventStreamHandler: eventStreamHandler,
		hostInfoOps:        hostInfoOps,
		poolIndex:          make(map[string]hostpool.HostPool),
		hostToPoolMap:      make(map[string]string),
		lifecycle:          lifecycle.NewLifeCycle(),
		parentScope:        parentScope,
		metrics:            NewMetrics(parentScope),
	}

	// Register default host pool when constructing new host pool manager.
	manager.RegisterPool(common.DefaultHostPoolID)

	return manager
}

// GetPool gets a host pool from cache by given pool id.
// It returns error if a host pool with given pool id doesn't exist.
func (m *hostPoolManager) GetPool(poolID string) (hostpool.HostPool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pool, ok := m.poolIndex[poolID]
	if !ok {
		m.metrics.GetPoolErr.Inc(1)
		return nil, errors.Errorf("host pool %s not found", poolID)
	}
	return pool, nil
}

// Pools returns all host pools from cache.
func (m *hostPoolManager) Pools() map[string]hostpool.HostPool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pools := make(map[string]hostpool.HostPool)
	for id, pool := range m.poolIndex {
		pools[id] = pool
	}
	return pools
}

// GetPoolByHostname returns the host pool given host belongs to.
// It returns error if host doesn't exist in hostToPoolMap or
// the host pool with looked up host pool ID doesn't exist in poolIndex.
func (m *hostPoolManager) GetPoolByHostname(hostname string) (hostpool.HostPool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	poolID, ok := m.hostToPoolMap[hostname]
	if !ok {
		m.metrics.GetPoolByHostnameErr.Inc(1)
		return nil, errors.Errorf("host %s not found", hostname)
	}

	pool, ok := m.poolIndex[poolID]
	if !ok {
		// This shouldn't happen since host pool manager should ensure
		// poolIndex is always in-sync with hostToPoolMap.
		m.metrics.GetPoolByHostnameErr.Inc(1)
		return nil, errors.Errorf("host pool %s not found", poolID)
	}

	return pool, nil
}

// RegisterPool creates a host pool with given ID if not exists.
// If a host pool with given pool id already exists, it is a no-op.
// If a host pool with given pool id doesn't exist, it creates an empty host pool.
func (m *hostPoolManager) RegisterPool(poolID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.poolIndex[poolID]; !ok {
		m.poolIndex[poolID] = hostpool.New(poolID, m.parentScope)
		log.WithField(hostpool.HostPoolKey, poolID).
			Info("Registered new host pool")
	} else {
		log.WithField(hostpool.HostPoolKey, poolID).
			Warn("Host pool already registered")
	}
}

// updatePoolInHostInfo updates current & desired pool on given host
// in host_infos table.
func (m *hostPoolManager) updatePoolInHostInfo(hostname, poolID string) error {
	if len(hostname) == 0 {
		m.metrics.UpdateCurrentPoolErr.Inc(1)
		return errors.New("hostname is empty")
	}

	err := m.hostInfoOps.UpdatePool(
		context.Background(),
		hostname,
		poolID,
		poolID)
	if err != nil {
		m.metrics.UpdateCurrentPoolErr.Inc(1)
		return err
	}
	return nil
}

// DeregisterPool deletes existing host pool with given ID.
// It returns error if callers tries to delete default host pool.
// It returns error if default host pool doesn't exist.
// If a host pool with given pool id already exists, it deletes the pool
// and moves hosts in the deleted pool to default pool.
// If a host pool with given pool id doesn't exist, it is a no-op.
func (m *hostPoolManager) DeregisterPool(poolID string) error {
	if poolID == common.DefaultHostPoolID {
		m.metrics.DeregisterPoolErr.Inc(1)
		return yarpcerrors.InvalidArgumentErrorf(
			"can't delete %s host pool", common.DefaultHostPoolID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if pool, ok := m.poolIndex[poolID]; !ok {
		log.WithField(hostpool.HostPoolKey, poolID).
			Warn("Host pool not found")
	} else {
		// move hosts to default pool
		defaultPool, ok := m.poolIndex[common.DefaultHostPoolID]
		if !ok {
			m.metrics.DeregisterPoolErr.Inc(1)
			return yarpcerrors.InternalErrorf(
				"%s host pool not found", common.DefaultHostPoolID)
		}
		for h := range pool.Hosts() {
			err := m.updatePoolInHostInfo(h, common.DefaultHostPoolID)
			if err != nil {
				return yarpcerrors.InternalErrorf(
					"failed to update current pool of %s in db: %v",
					h,
					err,
				)
			}
			m.hostToPoolMap[h] = common.DefaultHostPoolID
			pool.Delete(h)
			defaultPool.Add(h)
			m.publishPoolEvent(h, common.DefaultHostPoolID)
		}
		delete(m.poolIndex, poolID)
		log.WithField(hostpool.HostPoolKey, poolID).
			Info("Deleted existing host pool")
	}

	return nil
}

// ChangeHostPool changes host pool of given host from source pool to
// destination pool.
// If either source pool or destination pool doesn't exist, it returns error.
// If host is not in source pool, fails the move attempt for that host.
// TODO: Add more implementation after required hostInfo store change is done.
func (m *hostPoolManager) ChangeHostPool(
	hostname, srcPoolID, destPoolID string,
) (err error) {
	m.mu.Lock()
	defer func() {
		if err != nil {
			m.metrics.ChangeHostPoolErr.Inc(1)
		}
		m.mu.Unlock()
	}()

	poolID, ok := m.hostToPoolMap[hostname]
	if !ok {
		err = yarpcerrors.NotFoundErrorf("host not found")
		return
	}
	if poolID != srcPoolID {
		err = yarpcerrors.InvalidArgumentErrorf("source pool mismatch")
		return
	}
	srcPool, ok := m.poolIndex[poolID]
	if !ok {
		err = yarpcerrors.InternalErrorf("src pool not found")
		return
	}
	if srcPoolID == destPoolID {
		return
	}
	destPool, ok := m.poolIndex[destPoolID]
	if !ok {
		err = yarpcerrors.InvalidArgumentErrorf("invalid dest pool")
		return
	}

	if err = m.updatePoolInHostInfo(hostname, destPoolID); err != nil {
		return
	}
	m.hostToPoolMap[hostname] = destPoolID
	srcPool.Delete(hostname)
	destPool.Add(hostname)

	if agentMap := host.GetAgentMap(); agentMap == nil {
		log.Warn("Failed to get agent-map in ChangeHostPool")
	} else {
		srcPool.RefreshCapacity(agentMap.HostCapacities)
		destPool.RefreshCapacity(agentMap.HostCapacities)
	}

	m.publishPoolEvent(hostname, destPoolID)
	return
}

// GetDesiredPool gets desired pool of given host in db.
func (m *hostPoolManager) GetDesiredPool(hostname string) (string, error) {
	if len(hostname) == 0 {
		m.metrics.GetDesirePoolErr.Inc(1)
		return "", errors.New("hostname is empty")
	}

	hostInfo, err := m.hostInfoOps.Get(context.Background(), hostname)
	if err != nil {
		m.metrics.GetDesirePoolErr.Inc(1)
		return "", err
	}

	return hostInfo.DesiredPool, nil
}

// UpdateDesiredPool updates desired pool of given host in db.
// It returns error if either hostname or poolID is empty.
func (m *hostPoolManager) UpdateDesiredPool(hostname, poolID string) error {
	if len(hostname) == 0 {
		m.metrics.UpdateDesiredPoolErr.Inc(1)
		return errors.New("hostname is empty")
	}

	if len(poolID) == 0 {
		m.metrics.UpdateDesiredPoolErr.Inc(1)
		return errors.New("poolID is empty")
	}

	err := m.hostInfoOps.UpdateDesiredPool(context.Background(), hostname, poolID)
	if err != nil {
		m.metrics.UpdateDesiredPoolErr.Inc(1)
		return err
	}
	return nil
}

func (m *hostPoolManager) publishPoolEvent(hostname, poolID string) {
	poolEvent := &pb_eventstream.Event{
		Type: pb_eventstream.Event_HOST_EVENT,
		HostEvent: &pb_host.HostEvent{
			Hostname: hostname,
			Type:     pb_host.HostEvent_TYPE_HOST_POOL,
			HostPoolEvent: &pb_host.HostPoolEvent{
				Pool: poolID,
			},
		},
	}
	m.eventStreamHandler.AddEvent(poolEvent)
}

// recover recovers host pool data from db.
// It returns error if failed to read host pool data from db.
// It skips host with no current pool, since it indicates host is not registered
// in cluster anymore.
func (m *hostPoolManager) recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	hostInfo, err := m.hostInfoOps.GetAll(context.Background())
	if err != nil {
		return err
	}
	for _, h := range hostInfo {
		if h.CurrentPool == "" {
			continue
		}
		m.hostToPoolMap[h.Hostname] = h.CurrentPool
		if pool, ok := m.poolIndex[h.CurrentPool]; !ok {
			newPool := hostpool.New(h.CurrentPool, m.parentScope)
			newPool.Add(h.Hostname)
			m.poolIndex[h.CurrentPool] = newPool
		} else {
			pool.Add(h.Hostname)
		}
	}
	return nil
}

// Start starts the host pool cache go routine that reconciles host pools.
// It runs periodical reconciliation.
// It returns error if failed to recover host pool data from db.
func (m *hostPoolManager) Start() error {
	if !m.lifecycle.Start() {
		log.Warn("Host pool manager is already started")
		return nil
	}

	log.Info("Starting host pool manager")

	// Recover host pool data from db.
	if err := m.recover(); err != nil {
		return err
	}

	// Start goroutine to periodically reconcile host pool cache.
	go func() {
		defer m.lifecycle.StopComplete()

		ticker := time.NewTicker(m.reconcileInternal)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.reconcile(); err != nil {
					log.Error(err)
				}
			case <-m.lifecycle.StopCh():
				return
			}
		}
	}()

	return nil
}

// Stop stops the host pool cache go routine that reconciles host pools.
// It stops periodical reconciliation.
func (m *hostPoolManager) Stop() {
	if !m.lifecycle.Stop() {
		log.Warn("Host pool manager is already stopped")
	}

	log.Info("Stopping host pool manager")

	// Clean up host pool manager in-memory cache
	m.mu.Lock()
	m.poolIndex = map[string]hostpool.HostPool{}
	m.hostToPoolMap = map[string]string{}
	// Release lock before Wait() to avoid deadlock with reconcile goroutine
	m.mu.Unlock()

	m.lifecycle.Wait()
	log.Info("Host pool manager stopped")
}

// reconcile reconciles host pool cache.
// It makes sure default host pool exists.
// It reconciles host pool cache with host index in AgentMap cache.
// It reconciles host pool cache with host pool data in database.
// It makes sure every host belongs to, and only belongs to ONE host pool.
func (m *hostPoolManager) reconcile() error {
	sw := m.metrics.ReconcileTime.Start()
	m.mu.Lock()

	defer func() {
		m.mu.Unlock()
		sw.Stop()
	}()

	// Create default host pool if not exists.
	if _, ok := m.poolIndex[common.DefaultHostPoolID]; !ok {
		m.poolIndex[common.DefaultHostPoolID] =
			hostpool.New(common.DefaultHostPoolID, m.parentScope)
		log.WithField(hostpool.HostPoolKey, common.DefaultHostPoolID).
			Info("Registered new host pool during reconciliation")
	}

	// Load agent map AgentMap cache.
	agentMap := host.GetAgentMap()
	if agentMap == nil {
		m.metrics.ReconcileErr.Inc(1)
		return errors.New("failed to load agent map")
	}
	registeredAgents := agentMap.RegisteredAgents

	// Loop through all pools in poolIndex to rebuild hostToPoolMap.
	// If a host is not in agent map, delete it from host cache.
	// If a host is not in hostToPoolMap snapshot, delete it from poolIndex.
	// If a host's host pool value in poolIndex is different from
	// its value in hostToPoolMap, use the pool value in hostToPoolMap
	// as its host pool.
	// If a host is in multiple pools in poolIndex, use the pool value in
	// hostToPoolMap as its host pool.
	newHostToPoolMap := map[string]string{}
	for poolID, pool := range m.poolIndex {
		for hostname := range pool.Hosts() {
			if _, ok := registeredAgents[hostname]; !ok {
				err := m.updatePoolInHostInfo(hostname, "")
				if err != nil {
					log.WithError(err).WithField("hostname", hostname).
						Error(_updateHostPoolErrMsg)
					continue
				}
				delete(m.hostToPoolMap, hostname)
				pool.Delete(hostname)
				m.publishPoolEvent(hostname, "")
				continue
			}

			prevPoolID, ok := m.hostToPoolMap[hostname]
			if !ok {
				err := m.updatePoolInHostInfo(hostname, "")
				if err != nil {
					log.WithError(err).WithField("hostname", hostname).
						Error(_updateHostPoolErrMsg)
					continue
				}
				pool.Delete(hostname)
				m.publishPoolEvent(hostname, "")
				continue
			}

			if prevPoolID == poolID {
				newHostToPoolMap[hostname] = poolID
				continue
			}

			err := m.updatePoolInHostInfo(hostname, prevPoolID)
			if err != nil {
				log.WithError(err).WithField("hostname", hostname).
					Error(_updateHostPoolErrMsg)
				continue
			}
			newHostToPoolMap[hostname] = prevPoolID
			pool.Delete(hostname)
			if _, ok = m.poolIndex[prevPoolID]; !ok {
				m.poolIndex[prevPoolID] =
					hostpool.New(prevPoolID, m.parentScope)
				log.WithField(hostpool.HostPoolKey, prevPoolID).
					Info("Registered new host pool " +
						"during reconciliation")
			}
			m.poolIndex[prevPoolID].Add(hostname)
		}
	}

	// Loop through all hosts in hostToPoolMap snapshot.
	// If a host is not in rebuild hostToPoolMap, add it to host cache.
	for hostname, poolID := range m.hostToPoolMap {
		if _, ok := newHostToPoolMap[hostname]; !ok {
			err := m.updatePoolInHostInfo(hostname, poolID)
			if err != nil {
				log.WithError(err).WithField("hostname", hostname).
					Error(_updateHostPoolErrMsg)
				continue
			}
			newHostToPoolMap[hostname] = poolID
			if _, ok = m.poolIndex[poolID]; !ok {
				m.poolIndex[poolID] = hostpool.New(poolID, m.parentScope)
				log.WithField(hostpool.HostPoolKey, poolID).
					Info("Registered new host pool " +
						"during reconciliation")
			}
			m.poolIndex[poolID].Add(hostname)
		}
	}

	// Loop through all registered agents in agent map.
	// If any registered agent not in host pool cache,
	// add it to default host pool.
	for hostname := range registeredAgents {
		if _, ok := newHostToPoolMap[hostname]; !ok {
			err := m.updatePoolInHostInfo(hostname, common.DefaultHostPoolID)
			if err != nil {
				log.WithError(err).WithField("hostname", hostname).
					Error(_updateHostPoolErrMsg)
				continue
			}
			newHostToPoolMap[hostname] = common.DefaultHostPoolID
			// No need to check existence since reconciliation ensures
			// default host pool always exists at the beginning of each loop.
			m.poolIndex[common.DefaultHostPoolID].Add(hostname)
			m.publishPoolEvent(hostname, common.DefaultHostPoolID)
		}
	}

	m.hostToPoolMap = newHostToPoolMap

	m.refreshPoolCapacity()
	m.refreshMetrics()

	return nil
}

// refreshMetrics refreshes metrics of host pool cache and each host pool in the cache.
func (m *hostPoolManager) refreshMetrics() {
	// Refresh metrics of each host pool in the host pool cache.
	for _, pool := range m.poolIndex {
		pool.RefreshMetrics()
	}

	// Refresh host pool cache metrics.
	m.metrics.TotalHosts.Update(float64(len(m.hostToPoolMap)))
	m.metrics.TotalPools.Update(float64(len(m.poolIndex)))
}

// refreshPoolCapacity recalculates the capacity of each host-pool.
func (m *hostPoolManager) refreshPoolCapacity() {
	agentMap := host.GetAgentMap()
	if agentMap == nil {
		log.Warn("Failed to load agent-map in refreshPoolCapacity")
		return
	}
	hostCapacities := agentMap.HostCapacities
	for _, pool := range m.poolIndex {
		pool.RefreshCapacity(hostCapacities)
	}
}

// GetHostPoolLabelValues creates a LabelValues for host pool of a host.
func GetHostPoolLabelValues(
	manager HostPoolManager,
	hostname string,
) (constraints.LabelValues, error) {
	lv := make(constraints.LabelValues)

	pool, err := manager.GetPoolByHostname(hostname)
	if err != nil {
		return lv, errors.Wrapf(
			err,
			"error when getting host pool of host %s",
			hostname,
		)
	}

	lv[common.HostPoolKey] = map[string]uint32{pool.ID(): 1}
	return lv, nil
}
