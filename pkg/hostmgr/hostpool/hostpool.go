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

package hostpool

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// HostPool represents a set of hosts as a virtual host pool
// And provides abstraction to operate a host pool.
type HostPool interface {
	// ID returns host pool id.
	ID() string

	// Hosts returns all hosts in the pool as a map from hostname to host summary.
	// TODO: Consider what host data host pool needs to track.
	Hosts() map[string]struct{}

	// Add adds given host to the pool.
	Add(host string)

	// Delete deletes given host from the pool.
	Delete(host string)

	// Cleanup deletes all hosts from the pool.
	Cleanup()
}

// hostPool implements HostPool interface.
// TODO: Add metrics instrumentation where needed.
type hostPool struct {
	mu sync.RWMutex

	// id is the host pool id.
	id string
	// hosts contains all hosts belong to the host pool.
	hosts map[string]struct{}
}

// New returns a new hostPool instance.
func New(id string) HostPool {
	return &hostPool{
		id:    id,
		hosts: make(map[string]struct{}),
	}
}

// ID returns host pool id.
func (hp *hostPool) ID() string {
	return hp.id
}

// Hosts returns all hosts in the pool as a map from hostname to host summary.
func (hp *hostPool) Hosts() map[string]struct{} {
	hp.mu.RLock()
	defer hp.mu.RUnlock()

	hosts := make(map[string]struct{})
	for host := range hp.hosts {
		hosts[host] = struct{}{}
	}
	return hosts
}

// Add adds given host to the pool.
func (hp *hostPool) Add(host string) {
	poolID := hp.ID()

	hp.mu.Lock()
	defer hp.mu.Unlock()

	// Log warning since it might indicates data inconsistency in callers
	if _, ok := hp.hosts[host]; ok {
		log.WithFields(log.Fields{HostnameKey: host, HostPoolKey: poolID}).
			Warn("Host already in host pool")
	}

	// Insert host into map regardless to ensure idempotency
	hp.hosts[host] = struct{}{}
	log.WithField(HostnameKey, host).WithField(HostPoolKey, poolID).
		Debug("Added host to host pool")
}

// Delete deletes given host from the pool.
func (hp *hostPool) Delete(host string) {
	poolID := hp.ID()

	hp.mu.Lock()
	defer hp.mu.Unlock()

	// Log warning since it might indicates data inconsistency in callers
	if _, ok := hp.hosts[host]; !ok {
		log.WithFields(log.Fields{HostnameKey: host, HostPoolKey: poolID}).
			Warn("Host not found in host pool")
	}

	// Delete host from map regardless to ensure idempotency
	delete(hp.hosts, host)
	log.WithFields(log.Fields{HostnameKey: host, HostPoolKey: poolID}).
		Debug("Deleted host from host pool")
}

// Cleanup deletes all hosts from the pool.
func (hp *hostPool) Cleanup() {
	poolID := hp.ID()

	hp.mu.Lock()
	defer hp.mu.Unlock()

	hp.hosts = map[string]struct{}{}
	log.WithField(HostPoolKey, poolID).Info("Deleted all hosts from host pool")
}
