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
	"sync"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
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
	DeregisterPool(poolID string)

	// ChangeHostPool changes host pool of given host from source pool to
	// destination pool.
	ChangeHostPool(host string, srcPool, destPool string) ([]string, error)

	// Start starts the host pool cache go routine that reconciles host pools.
	Start()

	// Stop stops the host pool cache go routine that reconciles host pools.
	Stop()
}

// hostPoolManager implements HostPoolManager interface.
// it ensures:
// - host pool cache is consistent with db.
// - host pool cache is consistent with host cache in host manager.
// - every host in the cluster belongs to, and only belongs to ONE host pool.
// TODO: Add reference to offer pool/host cache.
// TODO: Add storage client.
// TODO: Add metrics instrumentation where needed.
type hostPoolManager struct {
	mu sync.RWMutex

	// poolIndex is map from host pool id to host pool
	poolIndex map[string]hostpool.HostPool
	// hostToPoolMap is map from hostname to id of host pool it belongs to
	hostToPoolMap map[string]string
}

// New returns a host pool manager instance.
// TODO: Decide if we need to register a list of pre-configured
//  host pools at start-up.
func New() HostPoolManager {
	manager := &hostPoolManager{
		poolIndex:     make(map[string]hostpool.HostPool),
		hostToPoolMap: make(map[string]string),
	}

	// Always register default host pool when constructing new host pool manager.
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
		return nil, errors.Errorf("host %s not found", hostname)
	}

	pool, ok := m.poolIndex[poolID]
	if !ok {
		// Ideally this shouldn't happen since host pool manager should ensure
		// poolIndex is always in-sync with hostToPoolMap.
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
		m.poolIndex[poolID] = hostpool.New(poolID)
		log.WithField(hostpool.HostPoolKey, poolID).
			Info("Registered new host pool")
	} else {
		log.WithField(hostpool.HostPoolKey, poolID).
			Warn("Host pool already registered")
	}
}

// DeregisterPool deletes existing host pool with given ID.
// If a host pool with given pool id already exists, it deletes the pool.
// If a host pool with given pool id doesn't exist, it is a no-op.
func (m *hostPoolManager) DeregisterPool(poolID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.poolIndex[poolID]; !ok {
		log.WithField(hostpool.HostPoolKey, poolID).
			Warn("Host pool not found")
	} else {
		delete(m.poolIndex, poolID)
		log.WithField(hostpool.HostPoolKey, poolID).
			Info("Deleted existing host pool")
	}
}

// ChangeHostPool changes host pool of given host from source pool to destination pool.
// If either source pool or destination pool doesn't exist, it returns error.
// If host is not in source pool, fails the move attempt for that host.
// TODO: Add implementation after required hostInfo store change is done.
func (m *hostPoolManager) ChangeHostPool(host string, srcPool, destPool string) ([]string, error) {
	return nil, yarpcerrors.UnimplementedErrorf("change host pool is not supported")
}

// Start starts the host pool cache go routine that reconciles host pools.
// It runs periodical reconciliation.
// TODO: Add implementation after required hostInfo store change is done.
func (m *hostPoolManager) Start() {
	log.Error("not implemented")
}

// Stop stops the host pool cache go routine that reconciles host pools.
// It stops periodical reconciliation.
// TODO: Add implementation after required hostInfo store change is done.
func (m *hostPoolManager) Stop() {
	// Clean up host pool manager in-memory cache
	m.mu.Lock()
	defer m.mu.Unlock()

	m.poolIndex = map[string]hostpool.HostPool{}
	m.hostToPoolMap = map[string]string{}

	log.Error("not implemented")
}

// reconcile reconciles host pool cache.
// It reconciles host pool cache with host index in offerPool/hostCache.
// It reconciles host pool cache with host pool data in database.
// It makes sure every host in the cluster belongs to, and only belongs to ONE host pool.
// TODO: Add implementation after required hostInfo store change is done.
func (m *hostPoolManager) reconcile() error {
	return yarpcerrors.UnimplementedErrorf("host pool reconciliation is not implemented")
}
