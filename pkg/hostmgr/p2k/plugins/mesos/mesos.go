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
	"context"
	"sync"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgrmesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	hostmgrscalar "github.com/uber/peloton/pkg/hostmgr/scalar"
	hmutil "github.com/uber/peloton/pkg/hostmgr/util"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
)

const (
	unreservedRole = "*"
)

// MesosManager implements the plugin for the Mesos cluster manager.
type MesosManager struct {
	// dispatcher for yarpcer
	d *yarpc.Dispatcher

	// Pod events channel.
	podEventCh chan<- *scalar.PodEvent

	// Host events channel.
	hostEventCh chan<- *scalar.HostEvent

	offerManager *offerManager
}

func NewMesosManager(
	d *yarpc.Dispatcher,
	podEventCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) *MesosManager {
	return &MesosManager{
		d:            d,
		podEventCh:   podEventCh,
		hostEventCh:  hostEventCh,
		offerManager: &offerManager{offers: make(map[string]*mesosOffers)},
	}
}

// Start the plugin.
func (m *MesosManager) Start() {
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_OFFERS:  m.Offers,
		sched.Event_RESCIND: m.Rescind,
	}

	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(m.d, hostmgrmesos.ServiceName, mpb.Procedure(name, hdl))
	}
}

// Stop the plugin.
func (m *MesosManager) Stop() {
}

// LaunchPod launches a pod on a host.
func (m *MesosManager) LaunchPod(
	podSpec *pbpod.PodSpec,
	podID,
	hostname string,
) error {
	// TODO: fill in implementation
	return nil
}

// KillPod kills a pod on a host.
func (m *MesosManager) KillPod(podID string) error {
	// TODO: fill in implementation
	return nil
}

// AckPodEvent is only implemented by mesos plugin. For K8s this is a noop.
func (m *MesosManager) AckPodEvent(ctx context.Context, event *scalar.PodEvent) {
	// TODO: fill in implementation
}

// ReconcileHosts will return the current state of hosts in the cluster.
func (m *MesosManager) ReconcileHosts() ([]*scalar.HostInfo, error) {
	// TODO: fill in implementation
	return nil, nil
}

// Offers is the mesos callback that sends the offers from master
// TODO: add metrics similar to what offerpool has
func (m *MesosManager) Offers(ctx context.Context, body *sched.Event) error {
	event := body.GetOffers()
	log.WithField("event", event).Info("MesosManager: processing Offer event")

	hosts := m.offerManager.addOffers(event.Offers)
	for _, host := range hosts {
		resources := m.offerManager.getResources(host)
		evt := scalar.BuildHostEventFromResource(host, resources, scalar.UpdateHostAvailableRes)
		m.hostEventCh <- evt
	}

	return nil
}

// Rescind offers
func (m *MesosManager) Rescind(ctx context.Context, body *sched.Event) error {
	event := body.GetRescind()
	log.WithField("event", event).Info("OfferManager: processing Rescind event")
	m.offerManager.removeOffer(event.GetOfferId().GetValue())
	return nil
}

type offerManager struct {
	sync.RWMutex

	// map hostname -> offers
	offers map[string]*mesosOffers
}

type mesosOffers struct {
	// mesos offerID -> unreserved offer
	unreservedOffers map[string]*mesos.Offer
}

// addOffers add offers into offerManager.offers, and returns the hosts
// updated
func (m *offerManager) addOffers(offers []*mesos.Offer) []string {
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

func (m *offerManager) removeOffer(offerID string) []string {
	// TODO: handle remove offer
	return nil
}

func (m *offerManager) getResources(hostname string) *peloton.Resources {
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
