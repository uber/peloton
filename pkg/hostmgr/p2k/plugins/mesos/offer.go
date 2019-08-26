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

package mesos

import (
	"sync"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	hostmgrscalar "github.com/uber/peloton/pkg/hostmgr/scalar"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"
)

type offerManager struct {
	sync.RWMutex

	// map hostname -> offers
	offers map[string]*mesosOffers
}

type mesosOffers struct {
	// mesos offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
}

// AddOffers add offers into offerManager.offers, and returns the hosts
// updated
func (m *offerManager) AddOffers(offers []*mesos.Offer) []string {
	// TODO: handle timed offers, similar to offerPool.AddOffers
	m.Lock()
	defer m.Unlock()

	var hostUpdated []string
	for _, offer := range offers {
		hostUpdated = append(hostUpdated, offer.GetHostname())

		if _, ok := m.offers[offer.GetHostname()]; !ok {
			m.offers[offer.GetHostname()] =
				&mesosOffers{unreservedOffers: make(map[string]*mesos.Offer)}
		}

		mesosOffers := m.offers[offer.GetHostname()]

		// filter out revocable resources whose type we don't recognize
		offerID := offer.GetId().GetValue()

		offer.Resources, _ = hostmgrscalar.FilterMesosResources(
			offer.Resources,
			func(r *mesos.Resource) bool {
				if r.GetRevocable() == nil {
					return true
				}
				return hmutil.IsSlackResourceType(r.GetName(), nil)
			})

		mesosOffers.unreservedOffers[offerID] = offer
	}

	return hostUpdated
}

func (m *offerManager) RemoveOffer(offerID string) []string {
	// TODO: handle remove offer
	return nil
}

// GetOffers returns offers on a host, result is a map of offerID -> offer
func (m *offerManager) GetOffers(hostname string) map[string]*mesos.Offer {
	m.RLock()
	defer m.RUnlock()

	mesosOffers, ok := m.offers[hostname]
	if !ok {
		return nil
	}

	return mesosOffers.unreservedOffers
}

func (m *offerManager) RemoveOfferForHost(hostname string) {
	m.Lock()
	defer m.Unlock()

	if mesosOffer, ok := m.offers[hostname]; ok {
		mesosOffer.unreservedOffers = make(map[string]*mesos.Offer)
	}
}

func (m *offerManager) GetResources(hostname string) *peloton.Resources {
	m.RLock()
	defer m.RUnlock()

	mesosOffers, ok := m.offers[hostname]
	if !ok {
		return &peloton.Resources{}
	}

	resources := hmutil.GetResourcesFromOffers(mesosOffers.unreservedOffers)
	return &peloton.Resources{
		Cpu:    resources.GetCPU(),
		MemMb:  resources.GetMem(),
		DiskMb: resources.GetDisk(),
		Gpu:    resources.GetGPU(),
	}
}

func (m *offerManager) Clear() {

}
