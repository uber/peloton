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

package models_v0

import (
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/models"
)

// NewHostOffers will create a placement host from a host manager host and all the resource manager tasks on it.
func NewHostOffers(hostOffer *hostsvc.HostOffer, tasks []*resmgr.Task, claimed time.Time) *HostOffers {
	return &HostOffers{
		Offer:   hostOffer,
		Tasks:   tasks,
		Claimed: claimed,
	}
}

// HostOffers represents a Peloton host and the tasks running
// on it and a Mimir placement group also be obtained from it.
type HostOffers struct {
	// host offer of the host.
	Offer *hostsvc.HostOffer `json:"offer"`
	// tasks running on the host.
	Tasks []*resmgr.Task `json:"tasks"`
	// Claimed is the time when the host was acquired from the host manager.
	Claimed time.Time `json:"claimed"`
	// data is used by placement strategies to transfer state between calls to the
	// place once method.
	data interface{}
	lock sync.Mutex
}

// Make sure that that HostOffers satisfies the plugins interface.
var _ models.Offer = &HostOffers{}

// ID returns the ID of the offer.
func (host *HostOffers) ID() string {
	return host.Offer.GetId().GetValue()
}

// AgentID returns the AgentID of this offer.
func (host *HostOffers) AgentID() string {
	return host.Offer.GetAgentId().GetValue()
}

// Hostname returns the hostname that this offer belongs to.
func (host *HostOffers) Hostname() string {
	return host.Offer.Hostname
}

// GetOffer returns the host offer of the host.
func (host *HostOffers) GetOffer() *hostsvc.HostOffer {
	return host.Offer
}

// GetTasks returns the tasks of the host.
func (host *HostOffers) GetTasks() []*resmgr.Task {
	return host.Tasks
}

// SetData will set the data transfer object on the host.
func (host *HostOffers) SetData(data interface{}) {
	host.lock.Lock()
	defer host.lock.Unlock()
	host.data = data
}

// Data will return the data transfer object of the host.
func (host *HostOffers) Data() interface{} {
	host.lock.Lock()
	defer host.lock.Unlock()
	return host.data
}

// AvailablePortRanges returns the available port ranges.
func (host *HostOffers) AvailablePortRanges() map[*models.PortRange]struct{} {
	availablePortRanges := map[*models.PortRange]struct{}{}
	for _, resource := range host.GetOffer().GetResources() {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			r := models.NewPortRange(*portRange.Begin, *portRange.End)
			availablePortRanges[r] = struct{}{}
		}
	}
	return availablePortRanges
}

// Age will return the age of the host, which is the time since it was dequeued from the host manager.
func (host *HostOffers) Age(now time.Time) time.Duration {
	return now.Sub(host.Claimed)
}

// GetAvailablePortCount returns the total number of ports available in
// this host's offers.
func (host *HostOffers) GetAvailablePortCount() uint64 {
	var ports uint64
	for _, resource := range host.GetOffer().GetResources() {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			ports += portRange.GetEnd() - portRange.GetBegin() + 1
		}
	}
	return ports
}

// GetAvailableResources returns the available resources of this host offer.
func (host *HostOffers) GetAvailableResources() (scalar.Resources, uint64) {
	res := scalar.FromMesosResources(host.GetOffer().GetResources())
	ports := host.GetAvailablePortCount()
	return res, ports
}
