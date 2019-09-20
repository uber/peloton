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

	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// HostPool represents a set of hosts as a virtual host pool
// And provides abstraction to operate a host pool.
type HostPool interface {
	// ID returns host pool id.
	ID() string

	// Hosts returns all hosts in the pool as a map from hostname to host summary.
	Hosts() map[string]struct{}

	// Add adds given host to the pool.
	Add(host string)

	// Delete deletes given host from the pool.
	Delete(host string)

	// Cleanup deletes all hosts from the pool.
	Cleanup()

	// RefreshMetrics refreshes metrics of the host pool.
	RefreshMetrics()

	// Get capacity of the pool.
	Capacity() host.ResourceCapacity

	// Recalculate host-pool capacity from given host capacities.
	RefreshCapacity(hostCapacities map[string]*host.ResourceCapacity)
}

// hostPool implements HostPool interface.
type hostPool struct {
	mu sync.RWMutex

	// id is the host pool id.
	id string

	// hosts contains all hosts belong to the host pool.
	hosts map[string]struct{}

	// Metrics.
	metrics *Metrics

	// Capacity of the pool.
	capacity host.ResourceCapacity
}

// New returns a new hostPool instance.
func New(id string, parentScope tally.Scope) HostPool {
	return &hostPool{
		id:      id,
		hosts:   make(map[string]struct{}),
		metrics: NewMetrics(parentScope, id),
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

	if _, ok := hp.hosts[host]; !ok {
		hp.hosts[host] = struct{}{}
		log.WithField(HostnameKey, host).WithField(HostPoolKey, poolID).
			Debug("Added host to host pool")
	}
}

// Delete deletes given host from the pool.
func (hp *hostPool) Delete(host string) {
	poolID := hp.ID()

	hp.mu.Lock()
	defer hp.mu.Unlock()

	if _, ok := hp.hosts[host]; ok {
		delete(hp.hosts, host)
		log.WithFields(log.Fields{HostnameKey: host, HostPoolKey: poolID}).
			Debug("Deleted host from host pool")
	}
}

// Cleanup deletes all hosts from the pool.
func (hp *hostPool) Cleanup() {
	poolID := hp.ID()

	hp.mu.Lock()
	defer hp.mu.Unlock()

	hp.hosts = map[string]struct{}{}
	log.WithField(HostPoolKey, poolID).Info("Deleted all hosts from host pool")
}

// RefreshMetrics refreshes metrics of the host pool.
func (hp *hostPool) RefreshMetrics() {
	hp.mu.RLock()
	defer hp.mu.RUnlock()

	hp.metrics.TotalHosts.Update(float64(len(hp.hosts)))
	hp.metrics.PhysicalCapacity.Update(hp.capacity.Physical)
	hp.metrics.SlackCapacity.Update(hp.capacity.Slack)
}

// Capacity returns the resource capacity of the host pool.
func (hp *hostPool) Capacity() host.ResourceCapacity {
	hp.mu.RLock()
	defer hp.mu.RUnlock()

	return hp.capacity
}

// RefreshCapacity updates the stored capacity of the pool.
func (hp *hostPool) RefreshCapacity(
	hostCapacities map[string]*host.ResourceCapacity,
) {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	physical := scalar.Resources{}
	slack := scalar.Resources{}
	for host := range hp.hosts {
		if cap, ok := hostCapacities[host]; ok {
			physical = physical.Add(cap.Physical)
			slack = slack.Add(cap.Slack)
		}
	}
	hp.capacity.Physical = physical
	hp.capacity.Slack = slack
}
