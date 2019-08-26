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
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/hostmgr/factory/task"
	hostmgrmesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// MesosManager implements the plugin for the Mesos cluster manager.
type MesosManager struct {
	// dispatcher for yarpc
	d *yarpc.Dispatcher

	// Pod events channel.
	podEventCh chan<- *scalar.PodEvent

	// Host events channel.
	hostEventCh chan<- *scalar.HostEvent

	offerManager *offerManager

	frameworkInfoProvider hostmgrmesos.FrameworkInfoProvider

	schedulerClient mpb.SchedulerClient

	metrics *metrics

	once sync.Once

	agentSyncer *agentSyncer
}

func NewMesosManager(
	d *yarpc.Dispatcher,
	frameworkInfoProvider hostmgrmesos.FrameworkInfoProvider,
	schedulerClient mpb.SchedulerClient,
	operatorClient mpb.MasterOperatorClient,
	agentInfoRefreshInterval time.Duration,
	scope tally.Scope,
	podEventCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) *MesosManager {
	return &MesosManager{
		d:                     d,
		metrics:               newMetrics(scope.SubScope("mesos_manager")),
		frameworkInfoProvider: frameworkInfoProvider,
		schedulerClient:       schedulerClient,
		podEventCh:            podEventCh,
		hostEventCh:           hostEventCh,
		offerManager:          &offerManager{offers: make(map[string]*mesosOffers)},
		once:                  sync.Once{},
		agentSyncer: newAgentSyncer(
			operatorClient,
			hostEventCh,
			agentInfoRefreshInterval,
		),
	}
}

// Start the plugin.
func (m *MesosManager) Start() error {
	m.once.Do(func() {
		procedures := map[sched.Event_Type]interface{}{
			sched.Event_OFFERS:  m.Offers,
			sched.Event_RESCIND: m.Rescind,
		}

		for typ, hdl := range procedures {
			name := typ.String()
			mpb.Register(m.d, hostmgrmesos.ServiceName, mpb.Procedure(name, hdl))
		}
	})

	m.agentSyncer.Start()

	return nil
}

// Stop the plugin.
func (m *MesosManager) Stop() {
	m.agentSyncer.Stop()
	m.offerManager.Clear()
}

// LaunchPods launch a list of pods on a host.
func (m *MesosManager) LaunchPods(
	ctx context.Context,
	pods []*models.LaunchablePod,
	hostname string,
) error {
	var offerIds []*mesos.OfferID
	var mesosResources []*mesos.Resource
	var mesosTasks []*mesos.TaskInfo
	var mesosTaskIds []string

	offers := m.offerManager.GetOffers(hostname)

	for _, offer := range offers {
		offerIds = append(offerIds, offer.GetId())
		mesosResources = append(mesosResources, offer.GetResources()...)
	}

	if len(offerIds) == 0 {
		return yarpcerrors.InternalErrorf("no offer found to launch pods on %s", hostname)
	}

	builder := task.NewBuilder(mesosResources)
	// assume only one agent on a host,
	// i.e. agentID is the same for all offers from the same host
	agentID := offers[offerIds[0].GetValue()].GetAgentId()

	for _, pod := range pods {
		launchableTask, err := convertPodSpecToLaunchableTask(pod.PodId, pod.Spec)
		if err != nil {
			return err
		}

		mesosTask, err := builder.Build(launchableTask)
		if err != nil {
			return err
		}
		mesosTask.AgentId = agentID
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, mesosTask.GetTaskId().GetValue())
	}

	callType := sched.Call_ACCEPT
	opType := mesos.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: m.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: offerIds,
			Operations: []*mesos.Offer_Operation{
				{
					Type: &opType,
					Launch: &mesos.Offer_Operation_Launch{
						TaskInfos: mesosTasks,
					},
				},
			},
		},
	}

	msid := m.frameworkInfoProvider.GetMesosStreamID(ctx)
	err := m.schedulerClient.Call(msid, msg)

	if err != nil {
		m.metrics.LaunchPodFail.Inc(1)
	} else {
		// call to mesos is successful,
		// remove the offers so no new task would be placed
		m.offerManager.RemoveOfferForHost(hostname)
		m.metrics.LaunchPod.Inc(1)
	}

	return err
}

// KillPod kills a pod on a host.
func (m *MesosManager) KillPod(ctx context.Context, podID string) error {
	callType := sched.Call_KILL
	msg := &sched.Call{
		FrameworkId: m.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Kill: &sched.Call_Kill{
			TaskId: &mesos.TaskID{Value: &podID},
		},
	}

	err := m.schedulerClient.Call(
		m.frameworkInfoProvider.GetMesosStreamID(ctx),
		msg,
	)

	if err != nil {
		m.metrics.KillPodFail.Inc(1)
	} else {
		m.metrics.KillPod.Inc(1)
	}

	return err
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

	hosts := m.offerManager.AddOffers(event.Offers)
	for _, host := range hosts {
		resources := m.offerManager.GetResources(host)
		evt := scalar.BuildHostEventFromResource(host, resources, scalar.UpdateHostAvailableRes)
		m.hostEventCh <- evt
	}

	return nil
}

// Rescind offers
func (m *MesosManager) Rescind(ctx context.Context, body *sched.Event) error {
	event := body.GetRescind()
	log.WithField("event", event).Info("OfferManager: processing Rescind event")
	m.offerManager.RemoveOffer(event.GetOfferId().GetValue())
	return nil
}

func convertPodSpecToLaunchableTask(id *peloton.PodID, spec *pbpod.PodSpec) (*hostsvc.LaunchableTask, error) {
	config, err := api.ConvertPodSpecToTaskConfig(spec)
	if err != nil {
		return nil, err
	}

	taskId := id.GetValue()
	return &hostsvc.LaunchableTask{
		// TODO: handle dynamic ports
		TaskId: &mesos.TaskID{Value: &taskId},
		Config: config,
		Id:     &v0peloton.TaskID{Value: spec.GetPodName().GetValue()},
	}, nil
}
