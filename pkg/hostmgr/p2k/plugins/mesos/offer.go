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
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	hostmgrscalar "github.com/uber/peloton/pkg/hostmgr/scalar"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"

	log "github.com/sirupsen/logrus"
)

var _slackResources = []string{"cpus"}

type offerManager struct {
	sync.RWMutex

	// map hostname -> hostToOffers
	hostToOffers map[string]*mesosOffers

	// map offerID -> offer, which include all of the offers
	offers map[string]*timedOffer

	// Time to hold offer in offer manager
	offerHoldTime time.Duration
}

func newOfferManager(offerHoldTime time.Duration) *offerManager {
	return &offerManager{
		hostToOffers:  make(map[string]*mesosOffers),
		offers:        make(map[string]*timedOffer),
		offerHoldTime: offerHoldTime,
	}
}

type mesosOffers struct {
	// mesos offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
}

type timedOffer struct {
	expiration time.Time
	hostname   string
}

// AddOffers add hostToOffers into offerManager.hostToOffers, and returns the set
// of hosts updated
func (m *offerManager) AddOffers(offers []*mesos.Offer) map[string]struct{} {
	m.Lock()
	defer m.Unlock()

	hostUpdated := make(map[string]struct{})
	for _, offer := range offers {
		hostUpdated[offer.GetHostname()] = struct{}{}

		if _, ok := m.hostToOffers[offer.GetHostname()]; !ok {
			m.hostToOffers[offer.GetHostname()] =
				&mesosOffers{unreservedOffers: make(map[string]*mesos.Offer)}
		}

		mesosOffers := m.hostToOffers[offer.GetHostname()]

		// filter out revocable resources whose type we don't recognize
		offerID := offer.GetId().GetValue()

		offer.Resources, _ = hostmgrscalar.FilterMesosResources(
			offer.Resources,
			func(r *mesos.Resource) bool {
				if r.GetRevocable() == nil {
					return true
				}
				return hmutil.IsSlackResourceType(r.GetName(), _slackResources)
			})

		mesosOffers.unreservedOffers[offerID] = offer

		// add the offer to the timedOffer map
		m.offers[offerID] = &timedOffer{
			hostname:   offer.GetHostname(),
			expiration: time.Now().Add(m.offerHoldTime),
		}
	}

	return hostUpdated
}

// RemoveOffer remove the offer specified with offerID in the
// offerManager.
// It returns the host that has the name, and an empty string
// if no host info is provided.
func (m *offerManager) RemoveOffer(offerID string) string {
	m.Lock()
	defer m.Unlock()

	timedOffer, ok := m.offers[offerID]
	if !ok {
		log.WithField("offer_id", offerID).
			Error("RemoveOffer: cannot find timed offers with offerID")
		return ""
	}

	mesosOffers, ok := m.hostToOffers[timedOffer.hostname]
	if !ok {
		log.WithField("offer_id", offerID).
			WithField("hostname", timedOffer.hostname).
			Error("RemoveOffer: cannot find any mesos offer on host")
		return ""
	}

	offer, ok := mesosOffers.unreservedOffers[offerID]
	if !ok {
		log.WithField("offer_id", offerID).
			WithField("hostname", timedOffer.hostname).
			Error("RemoveOffer: cannot find the specified mesos offer on host")
		return ""
	}

	delete(mesosOffers.unreservedOffers, offerID)
	delete(m.offers, offerID)

	// all of the offers on the host is removed,
	// delete the entry from m.hostToOffers
	if len(mesosOffers.unreservedOffers) == 0 {
		delete(m.hostToOffers, timedOffer.hostname)
	}

	return offer.GetHostname()
}

// GetOffers returns hostToOffers on a host, result is a map of offerID -> offer
func (m *offerManager) GetOffers(hostname string) map[string]*mesos.Offer {
	m.RLock()
	defer m.RUnlock()

	mesosOffers, ok := m.hostToOffers[hostname]
	if !ok {
		return nil
	}

	return mesosOffers.unreservedOffers
}

func (m *offerManager) RemoveOfferForHost(hostname string) {
	m.Lock()
	defer m.Unlock()

	if mesosOffer, ok := m.hostToOffers[hostname]; ok {
		for offerID := range mesosOffer.unreservedOffers {
			delete(m.offers, offerID)
		}

		mesosOffer.unreservedOffers = make(map[string]*mesos.Offer)
	}
}

func (m *offerManager) GetResources(hostname string) hostmgrscalar.Resources {
	m.RLock()
	defer m.RUnlock()

	mesosOffers, ok := m.hostToOffers[hostname]
	if !ok {
		return hostmgrscalar.Resources{}
	}

	// TODO: separate slack and non slack available resources.
	return hmutil.GetResourcesFromOffers(mesosOffers.unreservedOffers)
}

func (m *offerManager) Clear() {
	m.Lock()
	m.Unlock()

	m.hostToOffers = make(map[string]*mesosOffers)
	m.offers = make(map[string]*timedOffer)
}
