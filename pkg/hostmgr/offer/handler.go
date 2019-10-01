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

package offer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/cirbuf"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	"github.com/uber/peloton/pkg/hostmgr/config"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	mesosplugins "github.com/uber/peloton/pkg/hostmgr/p2k/plugins/mesos"
	"github.com/uber/peloton/pkg/hostmgr/prune"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	uatomic "github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	_heldHostPrunerName      = "heldHostPruner"
	_placingHostPrunerName   = "placingHostPruner"
	_binPackingRefresherName = "binPackingRefresher"

	_poolMetricsRefresh            = "poolMetricsRefresh"
	_taskStatusUpdateStreamRefresh = "taskStatusUpdateStreamRefresh"
	_poolMetricsRefreshPeriod      = 10 * time.Second
	_notifyResourceManagerPeriod   = 10 * time.Second
)

// EventHandler defines the interface for offer event handler that is
// called by leader election callbacks
type EventHandler interface {
	// Start starts the offer event handler, after which the handler will be
	// ready to process process offer events from an Mesos inbound.
	// Offers sent to the handler before `Start()` could be silently discarded.
	Start() error

	// Stop stops the offer event handlers and clears cached offers in pool.
	// Offers sent to the handler after `Stop()` could be silently discarded.
	Stop() error

	// GetOfferPool returns the underlying Pool holding the offers.
	GetOfferPool() offerpool.Pool

	// Get the handler for eventstream
	GetEventStreamHandler() *eventstream.Handler

	// SetHostPoolManager set host pool manager in the event handler.
	// It should be called during event handler initialization.
	SetHostPoolManager(manager manager.HostPoolManager)
}

// Singleton event handler for offers and mesos status update events
var handler *eventHandler

// eventHandler has handlers for mesos callbacks for offers,
// status updates and more.
type eventHandler struct {
	eventStreamHandler *eventstream.Handler
	schedulerclient    mpb.SchedulerClient
	watchProcessor     watchevent.WatchProcessor

	offerPool   offerpool.Pool
	offerPruner Pruner

	updateAckConcurrency int

	// Buffers the mesos task status updates to be acknowledged
	ackChannel chan *mesos.TaskStatus

	// Map to store outstanding mesos task status update acknowledgements
	// used to dedupe same event
	ackStatusMap sync.Map

	// Temporary measure to pass mesos events into mesos plugin,
	mesosPlugin *mesosplugins.MesosManager

	metrics *Metrics
}

// Represents eventstream to persist mesos status update events,
// which are consumed by Job Manager and Resource Manager.
// Once both clients process the mesos status update event, it is purged
// from eventstream and acknowledgement is sent to Mesos Master.
var eventStreamHandler *eventstream.EventHandler

// eventForwarder is the struct to forward status update events to
// resource manager. It implements eventstream.EventHandler and it
// forwards the events to remote in the OnEvents function.
type eventForwarder struct {
	// client to send NotifyTaskUpdatesRequest
	client resmgrsvc.ResourceManagerServiceYARPCClient
	// Tracking the progress returned from remote side
	progress *uint64
}

// GetEventProgress returns the event forward progress
func (f *eventForwarder) GetEventProgress() uint64 {
	return atomic.LoadUint64(f.progress)
}

// InitEventHandler initializes the event handler for offers
func InitEventHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	schedulerClient mpb.SchedulerClient,
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient,
	backgroundMgr background.Manager,
	ranker binpacking.Ranker,
	hostMgrConfig config.Config,
	processor watchevent.WatchProcessor,
	hostPoolManager manager.HostPoolManager,
	mesosPlugin *mesosplugins.MesosManager,
) {

	if handler != nil {
		log.Warning("Offer event handler has already been initialized")
		return
	}

	metrics := offerpool.NewMetrics(parent)
	pool := offerpool.NewOfferPool(
		time.Duration(hostMgrConfig.OfferHoldTimeSec)*time.Second,
		schedulerClient,
		metrics,
		hostmgr_mesos.GetSchedulerDriver(),
		hostMgrConfig.ScarceResourceTypes,
		hostMgrConfig.SlackResourceTypes,
		ranker,
		hostMgrConfig.HostPlacingOfferStatusTimeout,
		processor,
		hostPoolManager,
	)

	placingHostPruner := prune.NewPlacingHostPruner(
		pool,
		parent.SubScope(_placingHostPrunerName),
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _placingHostPrunerName,
			Func:   placingHostPruner.Prune,
			Period: hostMgrConfig.HostPruningPeriodSec,
		},
	)

	heldHostPruner := prune.NewHeldHostPruner(
		pool,
		parent.SubScope(_heldHostPrunerName),
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _heldHostPrunerName,
			Func:   heldHostPruner.Prune,
			Period: hostMgrConfig.HeldHostPruningPeriodSec,
		},
	)

	binPackingRefresher := offerpool.NewRefresher(
		pool,
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _binPackingRefresherName,
			Func:   binPackingRefresher.Refresh,
			Period: hostMgrConfig.BinPackingRefreshIntervalSec,
		},
	)

	backgroundMgr.RegisterWorks(
		background.Work{
			Name: _poolMetricsRefresh,
			Func: func(_ *uatomic.Bool) {
				pool.RefreshGaugeMaps()
			},
			Period: _poolMetricsRefreshPeriod,
		},
	)

	//TODO: refactor OfferPruner as a background worker
	handler = &eventHandler{
		schedulerclient: schedulerClient,
		watchProcessor:  processor,
		offerPool:       pool,
		offerPruner: NewOfferPruner(
			pool,
			time.Duration(hostMgrConfig.OfferPruningPeriodSec)*time.Second,
			metrics),
		metrics:              NewMetrics(parent),
		ackChannel:           make(chan *mesos.TaskStatus, hostMgrConfig.TaskUpdateBufferSize),
		updateAckConcurrency: hostMgrConfig.TaskUpdateAckConcurrency,
		mesosPlugin:          mesosPlugin,
	}
	handler.eventStreamHandler = initEventStreamHandler(
		d,
		handler,
		hostMgrConfig.TaskUpdateBufferSize,
		parent.SubScope("EventStreamHandler"))
	initResMgrEventForwarder(
		handler.eventStreamHandler,
		resmgrClient,
		parent.SubScope("ResourceManagerClient"))
	handler.startAsyncProcessTaskUpdates()
	backgroundMgr.RegisterWorks(
		background.Work{
			Name: _taskStatusUpdateStreamRefresh,
			Func: func(_ *uatomic.Bool) {
				handler.UpdateCounters()
			},
			Period: _poolMetricsRefreshPeriod,
		},
	)

	procedures := map[sched.Event_Type]interface{}{
		sched.Event_OFFERS:                handler.Offers,
		sched.Event_INVERSE_OFFERS:        handler.InverseOffers,
		sched.Event_RESCIND:               handler.Rescind,
		sched.Event_RESCIND_INVERSE_OFFER: handler.RescindInverseOffer,
		sched.Event_UPDATE:                handler.Update,
	}

	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, hostmgr_mesos.ServiceName, mpb.Procedure(name, hdl))
	}
}

// initResMgrEventForwarder, creates an event stream client to push
// mesos task status update events to Resource Manager from Host Manager.
func initResMgrEventForwarder(
	eventStreamHandler *eventstream.Handler,
	client resmgrsvc.ResourceManagerServiceYARPCClient,
	scope tally.Scope) {
	var progress uint64
	eventForwarder := &eventForwarder{
		client:   client,
		progress: &progress,
	}
	eventstream.NewLocalEventStreamClient(
		common.PelotonResourceManager,
		eventStreamHandler,
		eventForwarder,
		scope,
	)
}

// initEventStreamHandler initializes two event streams for communicating
// task status updates with Job Manager & Resource Manager.
// Job Manager: pulls task status update events from event stream.
// Resource Manager: Host Manager call event stream client
// to push task status update events.
func initEventStreamHandler(
	d *yarpc.Dispatcher,
	purgedEventsProcessor eventstream.PurgedEventsProcessor,
	bufferSize int,
	scope tally.Scope) *eventstream.Handler {
	eventStreamHandler := eventstream.NewEventStreamHandler(
		bufferSize,
		[]string{common.PelotonJobManager, common.PelotonResourceManager},
		purgedEventsProcessor,
		scope,
	)

	d.Register(pb_eventstream.BuildEventStreamServiceYARPCProcedures(eventStreamHandler))

	return eventStreamHandler
}

// GetEventHandler returns the handler for Mesos offer events. This
// function assumes the handler has been initialized as part of the
// InitEventHandler function.
// TODO: We should start a study of https://github.com/uber-common/inject
// and see whether we feel comfortable of using it.
func GetEventHandler() EventHandler {
	if handler == nil {
		log.Fatal("Offer event handler is not initialized")
	}
	return handler
}

// Get the event stream handler.
func (h *eventHandler) GetEventStreamHandler() *eventstream.Handler {
	return handler.eventStreamHandler
}

// SetHostPoolManager set host pool manager in the event handler.
// It should be called during event handler initialization.
func (h *eventHandler) SetHostPoolManager(manager manager.HostPoolManager) {
	h.offerPool.SetHostPoolManager(manager)
}

// Offers is the mesos callback that sends the offers from master
func (h *eventHandler) Offers(ctx context.Context, body *sched.Event) error {
	event := body.GetOffers()
	for _, offer := range event.Offers {
		log.WithFields(log.Fields{
			"offer_id": offer.GetId().GetValue(),
			"hostname": offer.GetHostname(),
		}).Info("offers received")
	}
	h.offerPool.AddOffers(ctx, event.Offers)
	// temporary measure to hook mesos plugins
	h.mesosPlugin.Offers(ctx, body)
	return nil
}

// InverseOffers is the mesos callback that sends the InverseOffers from master
func (h *eventHandler) InverseOffers(ctx context.Context, body *sched.Event) error {

	event := body.GetInverseOffers()
	log.WithField("event", event).
		Debug("OfferManager: processing InverseOffers event")

	// TODO: Handle inverse offers from Mesos
	return nil
}

// Rescind offers
func (h *eventHandler) Rescind(ctx context.Context, body *sched.Event) error {

	event := body.GetRescind()
	log.WithField("event", event).Debug("OfferManager: processing Rescind event")
	h.offerPool.RescindOffer(event.OfferId)
	// temporary measure to hook mesos plugins
	h.mesosPlugin.Rescind(ctx, body)
	return nil
}

// RescindInverseOffer rescinds a inverse offer
func (h *eventHandler) RescindInverseOffer(ctx context.Context, body *sched.Event) error {

	event := body.GetRescindInverseOffer()
	log.WithField("event", event).
		Debug("OfferManager: processing RescindInverseOffer event")

	// TODO: handle rescind inverse offer events
	return nil
}

// Update is the Mesos callback on mesos state updates
func (h *eventHandler) Update(ctx context.Context, body *sched.Event) error {
	var err error
	var event *pb_eventstream.Event
	taskUpdate := body.GetUpdate()

	defer func() {
		if err == nil {
			h.watchProcessor.NotifyEventChange(event)
		}

		// Update the metrics in go routine to unblock API callback
		go func() {
			h.metrics.taskUpdateCounter.Inc(1)
			taskStateCounter := h.metrics.scope.Counter(
				"task_state_" + taskUpdate.GetStatus().GetState().String())
			taskStateCounter.Inc(1)
		}()
	}()

	event = &pb_eventstream.Event{
		MesosTaskStatus: taskUpdate.GetStatus(),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
	}
	err = h.eventStreamHandler.AddEvent(event)
	if err != nil {
		log.WithError(err).
			WithField("status_update", taskUpdate.GetStatus()).
			Error("Cannot add status update")
	}

	h.offerPool.UpdateTasksOnHost(
		taskUpdate.GetStatus().GetTaskId().GetValue(),
		util.MesosStateToPelotonState(taskUpdate.GetStatus().GetState()),
		nil)

	// temporary measure to pass mesos event into plugin
	h.mesosPlugin.Update(ctx, body)

	// If buffer is full, AddStatusUpdate would fail and peloton would not
	// ack the status update and mesos master would resend the status update.
	// Return nil otherwise the framework would disconnect with the mesos master
	return nil
}

// OnV0Event callback
func (f *eventForwarder) OnV0Event(event *pb_eventstream.Event) {
	//Not implemented
}

// OnV0Events callback. In this callback, a batch of events are forwarded to
// resource manager.
func (f *eventForwarder) OnV0Events(events []*pb_eventstream.Event) {
	if len(events) == 0 {
		return
	}

	var response *resmgrsvc.NotifyTaskUpdatesResponse
	request := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}
	b := backoff.NewRetrier(backoff.NewRetryPolicy(
		1,
		_notifyResourceManagerPeriod))
	for {
		var err error
		response, err = f.notifyResourceManager(request)
		if err == nil || !backoff.CheckRetry(b) {
			break
		}
	}

	if response.PurgeOffset > 0 {
		atomic.StoreUint64(f.progress, response.PurgeOffset)
	}
	if response.Error != nil {
		log.WithField("notify_task_updates_response_error", response.Error).
			Error("NotifyTaskUpdatesRequest failed")
	}
}

func (f *eventForwarder) notifyResourceManager(
	request *resmgrsvc.NotifyTaskUpdatesRequest) (
	*resmgrsvc.NotifyTaskUpdatesResponse, error) {

	ctx, cancelFunc := context.WithTimeout(
		context.Background(),
		_notifyResourceManagerPeriod)
	defer cancelFunc()

	return f.client.NotifyTaskUpdates(ctx, request)
}

// EventPurged is for implementing PurgedEventsProcessor interface.
func (h *eventHandler) EventPurged(events []*cirbuf.CircularBufferItem) {
	for _, e := range events {
		event, ok := e.Value.(*pb_eventstream.Event)
		if !ok {
			// should never happen
			continue
		}

		if event.GetType() != pb_eventstream.Event_MESOS_TASK_STATUS {
			continue
		}
		uid := uuid.UUID(event.GetMesosTaskStatus().GetUuid()).String()
		if uid == "" {
			continue
		}

		_, ok = h.ackStatusMap.Load(uid)
		if ok {
			h.metrics.taskUpdateAckDeDupe.Inc(1)
			continue
		}
		h.ackStatusMap.Store(uid, struct{}{})
		h.ackChannel <- event.GetMesosTaskStatus()
	}
}

// startAsyncProcessTaskUpdates concurrently process task status update events
// ready to ACK iff uuid is not nil.
func (h *eventHandler) startAsyncProcessTaskUpdates() {
	for i := 0; i < h.updateAckConcurrency; i++ {
		go func() {
			for taskStatus := range h.ackChannel {
				uid := uuid.UUID(taskStatus.GetUuid()).String()
				// once acked, delete from map
				// if ack failed at mesos master then agent will re-send
				h.ackStatusMap.Delete(uid)

				if err := h.acknowledgeTaskUpdate(
					context.Background(),
					taskStatus); err != nil {
					log.WithField("task_status", *taskStatus).
						WithError(err).
						Error("Failed to acknowledgeTaskUpdate")
				}
			}
		}()
	}
}

// acknowledgeTaskUpdate, ACK task status update events
// thru POST scheduler client call to Mesos Master.
func (h *eventHandler) acknowledgeTaskUpdate(
	ctx context.Context,
	taskStatus *mesos.TaskStatus) error {

	h.metrics.taskUpdateAck.Inc(1)
	callType := sched.Call_ACKNOWLEDGE
	msid := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID(ctx)
	msg := &sched.Call{
		FrameworkId: hostmgr_mesos.GetSchedulerDriver().GetFrameworkID(ctx),
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			AgentId: taskStatus.AgentId,
			TaskId:  taskStatus.TaskId,
			Uuid:    taskStatus.Uuid,
		},
	}
	if err := h.schedulerclient.Call(msid, msg); err != nil {
		return err
	}

	log.WithField("task_status", *taskStatus).Debug("Acked task update")

	return nil
}

// UpdateCounters tracks the count for task status update & ack count.
func (h *eventHandler) UpdateCounters() {
	h.metrics.taskAckChannelSize.Update(float64(len(h.ackChannel)))
	var length float64
	h.ackStatusMap.Range(func(key, _ interface{}) bool {
		length++
		return true
	})
	h.metrics.taskAckMapSize.Update(length)
}

// Pool returns the underlying OfferPool.
func (h *eventHandler) GetOfferPool() offerpool.Pool {
	return h.offerPool
}

// Start runs startup related procedures
func (h *eventHandler) Start() error {
	// Start offer pruner
	h.offerPruner.Start()

	// TODO: add error handling
	return nil
}

// Stop runs shutdown related procedures
func (h *eventHandler) Stop() error {
	// Clean up all existing offers
	h.offerPool.Clear()
	// Stop offer pruner
	h.offerPruner.Stop()

	// TODO: add error handling
	return nil
}
