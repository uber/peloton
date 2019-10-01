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
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/factory/task"
	hostmgrmesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	hmscalar "github.com/uber/peloton/pkg/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

const mesosTaskUpdateAckChanSize = 1000

// MesosManager implements the plugin for the Mesos cluster manager.
type MesosManager struct {
	// dispatcher for yarpc
	d *yarpc.Dispatcher

	lf lifecycle.LifeCycle

	// Pod events channel. This channel is used to send pod events up stream to the event stream.
	podEventCh chan<- *scalar.PodEvent

	// Host events channel. This channel is used to convert host offers and capacity information to host cache.
	hostEventCh chan<- *scalar.HostEvent

	offerManager *offerManager

	frameworkInfoProvider hostmgrmesos.FrameworkInfoProvider

	schedulerClient mpb.SchedulerClient

	updateAckConcurrency int

	// ackChannel buffers the pod events to be acknowledged. AckPodEvent adds an event to be acked to this channel.
	// ackPodEventWorker consumes this event and sends an ack back to Mesos.
	ackChannel chan *scalar.PodEvent

	// Map to store outstanding mesos task status update acknowledgements
	// used to dedupe same event.
	ackStatusMap sync.Map

	metrics *metrics

	once sync.Once

	agentSyncer *agentSyncer

	// Map to store agentID to hostname.
	// When a task update mesos event comes, it only as agent ID with it.
	// However, peloton requires hostname to decide which populate hostsummary
	// and other cache. As a result, mesos plugin needs to maintain this map
	// by digesting host agent info, and looks up corresponding hostname with
	// the agentID when an event comes in.
	agentIDToHostname sync.Map
}

func NewMesosManager(
	d *yarpc.Dispatcher,
	frameworkInfoProvider hostmgrmesos.FrameworkInfoProvider,
	schedulerClient mpb.SchedulerClient,
	operatorClient mpb.MasterOperatorClient,
	agentInfoRefreshInterval time.Duration,
	offerHoldTime time.Duration,
	scope tally.Scope,
	podEventCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) *MesosManager {
	return &MesosManager{
		d:                     d,
		lf:                    lifecycle.NewLifeCycle(),
		metrics:               newMetrics(scope.SubScope("mesos_manager")),
		frameworkInfoProvider: frameworkInfoProvider,
		schedulerClient:       schedulerClient,
		podEventCh:            podEventCh,
		hostEventCh:           hostEventCh,
		offerManager:          newOfferManager(offerHoldTime),
		ackChannel:            make(chan *scalar.PodEvent, mesosTaskUpdateAckChanSize),
		once:                  sync.Once{},
		agentSyncer: newAgentSyncer(
			operatorClient,
			agentInfoRefreshInterval,
		),
	}
}

// Start the plugin.
func (m *MesosManager) Start() error {
	if !m.lf.Start() {
		// already started,
		// skip the action
		return nil
	}
	//TODO: remove comment after MesosManager takes over mesos callback
	//m.once.Do(func() {
	//	procedures := map[sched.Event_Type]interface{}{
	//		sched.Event_OFFERS:  m.Offers,
	//		sched.Event_RESCIND: m.Rescind,
	//		sched.Event_UPDATE:  m.Update,
	//	}
	//
	//	for typ, hdl := range procedures {
	//		name := typ.String()
	//		mpb.Register(m.d, hostmgrmesos.ServiceName, mpb.Procedure(name, hdl))
	//	}
	//})

	m.agentSyncer.Start()
	m.startProcessAgentInfo(m.agentSyncer.AgentCh())
	m.startAsyncProcessTaskUpdates()
	return nil
}

// Stop the plugin.
func (m *MesosManager) Stop() {
	if !m.lf.Stop() {
		// already stopped,
		// skip the action
		return
	}

	m.agentSyncer.Stop()
	m.offerManager.Clear()
}

// LaunchPods launch a list of pods on a host.
func (m *MesosManager) LaunchPods(
	ctx context.Context,
	pods []*models.LaunchablePod,
	hostname string,
) ([]*models.LaunchablePod, error) {
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
		return nil, yarpcerrors.InternalErrorf("no offer found to launch pods on %s", hostname)
	}

	builder := task.NewBuilder(mesosResources)
	// assume only one agent on a host,
	// i.e. agentID is the same for all offers from the same host
	agentID := offers[offerIds[0].GetValue()].GetAgentId()

	for _, pod := range pods {
		launchableTask, err := convertPodSpecToLaunchableTask(pod.PodId, pod.Spec, pod.Ports)
		if err != nil {
			return nil, err
		}

		mesosTask, err := builder.Build(launchableTask)
		if err != nil {
			return nil, err
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
		// Decline offers upon launch failure in a best effort manner,
		// because peloton no longer holds the offers in host summary.
		// When declining offer is called,
		// if launch does not go through, mesos would send new offers with the
		// resources.
		// If launch does go through, this call should not affect launched task.
		// It is still a best effort way to clean offers up, peloton still
		// rely on offer expiration to clean up the offers left behind.
		m.offerManager.RemoveOfferForHost(hostname)
		m.declineOffers(ctx, offerIds)
		m.metrics.LaunchPodFail.Inc(1)
		return nil, err
	}
	// call to mesos is successful,
	// remove the offers so no new task would be placed
	m.offerManager.RemoveOfferForHost(hostname)
	m.metrics.LaunchPod.Inc(1)
	return pods, nil
}

// declineOffers calls mesos master to decline list of offers
func (m *MesosManager) declineOffers(
	ctx context.Context,
	offerIDs []*mesos.OfferID) error {

	callType := sched.Call_DECLINE
	msg := &sched.Call{
		FrameworkId: m.frameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Decline: &sched.Call_Decline{
			OfferIds: offerIDs,
		},
	}
	msid := m.frameworkInfoProvider.GetMesosStreamID(ctx)
	err := m.schedulerClient.Call(msid, msg)
	if err != nil {
		// Ideally, we assume that Mesos has offer_timeout configured,
		// so in the event that offer declining call fails, offers
		// should eventually be invalidated by Mesos.
		log.WithError(err).
			WithField("call", msg).
			Warn("Failed to decline offers.")
		m.metrics.DeclineOffersFail.Inc(1)
		return err
	}

	m.metrics.DeclineOffers.Inc(int64(len(offerIDs)))

	return nil
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
func (m *MesosManager) AckPodEvent(
	event *scalar.PodEvent,
) {
	// Add this to the mesos task status update ack channel and handle it asynchronously.
	m.ackChannel <- event
}

// startAsyncProcessTaskUpdates concurrently process task status update events
// ready to ACK iff uuid is not nil.
func (m *MesosManager) startAsyncProcessTaskUpdates() {
	for i := 0; i < m.updateAckConcurrency; i++ {
		go m.ackPodEventWorker()
	}
}

func (m *MesosManager) ackPodEventWorker() {
	for pe := range m.ackChannel {
		// dedupe event.
		if pe.EventID == "" {
			continue
		}

		if _, ok := m.ackStatusMap.Load(pe.EventID); ok {
			m.metrics.TaskUpdateAckDeDupe.Inc(1)
			continue
		}

		// This is a new event to be acknowledged. Add it to the dedupe map of acks.
		m.ackStatusMap.Store(pe.EventID, struct{}{})

		// if ack failed at mesos master then agent will re-sent.
		if err := m.acknowledgeTaskUpdate(
			context.Background(),
			pe,
		); err != nil {
			log.WithField("pod_event", pe.Event).
				WithError(err).
				Error("Failed to acknowledgeTaskUpdate")
		}
		// Once acked, delete this from dedupe map.
		m.ackStatusMap.Delete(pe.EventID)
	}
}

// acknowledgeTaskUpdate, ACK task status update events
// thru POST scheduler client call to Mesos Master.
func (m *MesosManager) acknowledgeTaskUpdate(
	ctx context.Context,
	e *scalar.PodEvent) error {
	pe := e.Event
	m.metrics.TaskUpdateAck.Inc(1)
	callType := sched.Call_ACKNOWLEDGE
	msid := hostmgrmesos.GetSchedulerDriver().GetMesosStreamID(ctx)
	agentIDStr := pe.GetAgentId()
	taskIdStr := pe.GetPodId().GetValue()

	msg := &sched.Call{
		FrameworkId: hostmgrmesos.GetSchedulerDriver().GetFrameworkID(ctx),
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			AgentId: &mesos.AgentID{Value: &agentIDStr},
			TaskId:  &mesos.TaskID{Value: &taskIdStr},
			Uuid:    []byte(e.EventID),
		},
	}
	if err := m.schedulerClient.Call(msid, msg); err != nil {
		return err
	}

	log.WithField("task_status", pe).Debug("Acked task update")

	return nil
}

func (m *MesosManager) startProcessAgentInfo(
	agentCh <-chan []*mesosmaster.Response_GetAgents_Agent,
) {
	// The first batch needs to be populated in sync,
	// so after MesosManager starts and begins to receive mesos events,
	// MesosManager would have the agentIDToHostname ready
	m.processAgentHostMap(<-agentCh)

	go func() {
		for {
			select {
			case agents := <-agentCh:
				m.processAgentHostMap(agents)
			case <-m.lf.StopCh():
				return
			}
		}
	}()
}

func (m *MesosManager) processAgentHostMap(
	agents []*mesosmaster.Response_GetAgents_Agent,
) {
	for _, agent := range agents {
		agentID := agent.GetAgentInfo().GetId().GetValue()
		hostname := agent.GetAgentInfo().GetHostname()
		m.agentIDToHostname.Store(agentID, hostname)
		for _, agent := range agents {
			capacity := models.HostResources{
				NonSlack: hmscalar.FromMesosResources(agent.GetTotalResources()),
			}
			m.hostEventCh <- scalar.BuildHostEventFromResource(
				hostname,
				models.HostResources{},
				capacity,
				scalar.UpdateAgent,
			)
		}
	}
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
	for host := range hosts {
		// TODO: extract slack and non slack resources from offer manager.
		availableResources := models.HostResources{
			NonSlack: m.offerManager.GetResources(host),
		}
		evt := scalar.BuildHostEventFromResource(
			host,
			availableResources,
			models.HostResources{},
			scalar.UpdateHostAvailableRes,
		)
		m.hostEventCh <- evt
	}

	return nil
}

// Rescind offers
func (m *MesosManager) Rescind(ctx context.Context, body *sched.Event) error {
	event := body.GetRescind()
	log.WithField("event", event).Info("OfferManager: processing Rescind event")
	host := m.offerManager.RemoveOffer(event.GetOfferId().GetValue())

	if len(host) != 0 {
		availableResources := models.HostResources{
			NonSlack: m.offerManager.GetResources(host),
		}
		evt := scalar.BuildHostEventFromResource(
			host,
			availableResources,
			models.HostResources{},
			scalar.UpdateHostAvailableRes,
		)
		m.hostEventCh <- evt
	}

	return nil
}

// Update is the Mesos callback on mesos task status updates
func (m *MesosManager) Update(ctx context.Context, body *sched.Event) error {
	taskUpdate := body.GetUpdate()

	// Todo implement watch processor notifications.

	hostname, ok := m.agentIDToHostname.Load(
		taskUpdate.GetStatus().GetAgentId().GetValue())
	if !ok {
		// Hostname is not found, maybe the agent info is not
		// populated yet. Return directly and wait for mesos
		// to resend the event.
		m.metrics.AgentIDToHostnameMissing.Inc(1)
		log.WithField("agent_id",
			taskUpdate.GetStatus().GetAgentId().GetValue(),
		).Warn("cannot find hostname for agent_id")
		return nil
	}

	// Update the metrics in go routine to unblock API callback
	m.podEventCh <- buildPodEventFromMesosTaskStatus(taskUpdate, hostname.(string))
	m.metrics.TaskUpdateCounter.Inc(1)
	taskStateCounter := m.metrics.scope.Counter(
		"task_state_" + taskUpdate.GetStatus().GetState().String())
	taskStateCounter.Inc(1)
	return nil
}

func convertPodSpecToLaunchableTask(
	id *peloton.PodID,
	spec *pbpod.PodSpec,
	ports map[string]uint32,
) (*hostsvc.LaunchableTask, error) {
	config, err := api.ConvertPodSpecToTaskConfig(spec)
	if err != nil {
		return nil, err
	}

	taskId := id.GetValue()
	return &hostsvc.LaunchableTask{
		TaskId: &mesos.TaskID{Value: &taskId},
		Config: config,
		Id:     &v0peloton.TaskID{Value: spec.GetPodName().GetValue()},
		Ports:  ports,
	}, nil
}

func buildPodEventFromMesosTaskStatus(
	evt *sched.Event_Update,
	hostname string,
) *scalar.PodEvent {
	healthy := pbpod.HealthState_HEALTH_STATE_UNHEALTHY.String()
	if evt.GetStatus().GetHealthy() {
		healthy = pbpod.HealthState_HEALTH_STATE_HEALTHY.String()
	}
	taskState := util.MesosStateToPelotonState(evt.GetStatus().GetState())
	return &scalar.PodEvent{
		Event: &pbpod.PodEvent{
			PodId:       &peloton.PodID{Value: evt.GetStatus().GetTaskId().GetValue()},
			ActualState: api.ConvertTaskStateToPodState(taskState).String(),
			Timestamp: util.FormatTime(
				evt.GetStatus().GetTimestamp(),
				time.RFC3339Nano,
			),
			AgentId:  evt.GetStatus().GetAgentId().GetValue(),
			Hostname: hostname,
			Message:  evt.GetStatus().GetMessage(),
			Reason:   evt.GetStatus().GetReason().String(),
			Healthy:  healthy,
		},
		EventType: scalar.UpdatePod,
		EventID:   string(evt.GetStatus().GetUuid()),
	}
}
