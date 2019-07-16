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

package models_v1

import (
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/v0"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/v1"
)

// lease implements the models.Offer interface. Internally it keeps the API
// object gotten from host manager.
type lease struct {
	// hostLease is the lease object that we received from the hostmanager API.
	hostLease *hostmgr.HostLease

	// The tasks running on this host.
	tasks []*resmgr.Task
}

var _ models.Offer = lease{}

// NewOffer returns a new models.Offer from the HostLease of v1alpha API.
func NewOffer(hostLease *hostmgr.HostLease, tasks []*resmgr.Task) models.Offer {
	return lease{
		hostLease: hostLease,
		tasks:     tasks,
	}
}

// ID returns the ID of the lease.
func (l lease) ID() string {
	return l.hostLease.GetLeaseId().GetValue()
}

// Hostname returns the hostname of the owner of the lease.
func (l lease) Hostname() string {
	return l.hostLease.GetHostSummary().GetHostname()
}

// AgentID returns the hostname of the lease.
func (l lease) AgentID() string {
	return l.Hostname()
}

// GetAvailableResources returns the available resources that this lease
// gives control over.
func (l lease) GetAvailableResources() (scalar.Resources, uint64) {
	res := l.hostLease.GetHostSummary().GetResources()
	return scalar.Resources{
		CPU:  res.Cpu,
		Mem:  res.MemMb,
		Disk: res.DiskMb,
		GPU:  res.Gpu,
	}, l.countFreePorts()
}

// ToMimirGroup returns the mimir placement group so that the placement
// strategy can place tasks on that group.
func (l lease) ToMimirGroup() *placement.Group {
	res, ports := l.GetAvailableResources()
	labels := map[string]string{}
	for _, label := range l.hostLease.GetHostSummary().GetLabels() {
		labels[label.Key] = label.Value
	}
	group := mimir_v1.CreateGroup(l.Hostname(), res, ports, labels)
	for _, task := range l.tasks {
		entity := mimir_v0.TaskToEntity(task, true)
		group.Entities.Add(entity)
	}
	group.Update()
	return group
}

// AvailablePortRanges returns the list of available port ranges in this lease.
func (l lease) AvailablePortRanges() map[*models.PortRange]struct{} {
	ranges := l.hostLease.GetHostSummary().GetAvailablePorts()
	result := map[*models.PortRange]struct{}{}
	for _, r := range ranges {
		result[models.NewPortRange(r.Begin, r.End)] = struct{}{}
	}
	return result
}

func (l lease) countFreePorts() uint64 {
	ranges := l.hostLease.GetHostSummary().GetAvailablePorts()
	var total uint64
	for _, r := range ranges {
		total += r.End - r.Begin + 1
	}
	return total
}
