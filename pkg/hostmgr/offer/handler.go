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
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	"github.com/uber/peloton/pkg/hostmgr/prune"
	"github.com/uber/peloton/pkg/hostmgr/reservation/cleaner"
	"github.com/uber/peloton/pkg/storage"
)

const (
	_heldHostPrunerName          = "heldHostPruner"
	_placingHostPrunerName       = "placingHostPruner"
	_binPackingRefresherName     = "binPackingRefresher"
	_resourceCleanerName         = "resourceCleaner"
	_resourceCleanerPeriod       = 15 * time.Minute
	_resourceCleanerInitialDelay = 15 * time.Minute

	_poolMetricsRefresh       = "poolMetricsRefresh"
	_poolMetricsRefreshPeriod = 10 * time.Second
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
}

// eventHandler is the handler for Mesos Offer events
type eventHandler struct {
	offerPool   offerpool.Pool
	offerPruner Pruner
	metrics     *offerpool.Metrics
}

// Singleton event handler for offers
var handler *eventHandler

// InitEventHandler initializes the event handler for offers
func InitEventHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	offerHoldTime time.Duration,
	offerPruningPeriod time.Duration,
	schedulerClient mpb.SchedulerClient,
	volumeStore storage.PersistentVolumeStore,
	backgroundMgr background.Manager,
	placingHostPruningPeriodSec time.Duration,
	heldHostPruningPeriodSec time.Duration,
	scarceResourceTypes []string,
	slackResourceTypes []string,
	ranker binpacking.Ranker,
	binPackingRefreshIntervalSec time.Duration,
	hostPlacingOfferStatusTimeout time.Duration) {

	if handler != nil {
		log.Warning("Offer event handler has already been initialized")
		return
	}
	metrics := offerpool.NewMetrics(parent)
	pool := offerpool.NewOfferPool(
		offerHoldTime,
		schedulerClient,
		metrics,
		hostmgr_mesos.GetSchedulerDriver(),
		volumeStore,
		scarceResourceTypes,
		slackResourceTypes,
		ranker,
		hostPlacingOfferStatusTimeout,
	)

	placingHostPruner := prune.NewPlacingHostPruner(
		pool,
		parent.SubScope(_placingHostPrunerName),
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _placingHostPrunerName,
			Func:   placingHostPruner.Prune,
			Period: placingHostPruningPeriodSec,
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
			Period: heldHostPruningPeriodSec,
		},
	)

	binPackingRefresher := offerpool.NewRefresher(
		pool,
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _binPackingRefresherName,
			Func:   binPackingRefresher.Refresh,
			Period: binPackingRefreshIntervalSec,
		},
	)

	resourceCleaner := cleaner.NewCleaner(
		pool,
		parent.SubScope(_resourceCleanerName),
		volumeStore,
		schedulerClient,
		hostmgr_mesos.GetSchedulerDriver(),
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:         _resourceCleanerName,
			Func:         resourceCleaner.Run,
			Period:       _resourceCleanerPeriod,
			InitialDelay: _resourceCleanerInitialDelay,
		},

		background.Work{
			Name: _poolMetricsRefresh,
			Func: func(_ *atomic.Bool) {
				pool.RefreshGaugeMaps()
			},
			Period: _poolMetricsRefreshPeriod,
		},
	)
	//TODO: refactor OfferPruner as a background worker
	handler = &eventHandler{
		offerPool:   pool,
		offerPruner: NewOfferPruner(pool, offerPruningPeriod, metrics),
		metrics:     metrics,
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_OFFERS:                handler.Offers,
		sched.Event_INVERSE_OFFERS:        handler.InverseOffers,
		sched.Event_RESCIND:               handler.Rescind,
		sched.Event_RESCIND_INVERSE_OFFER: handler.RescindInverseOffer,
	}

	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, hostmgr_mesos.ServiceName, mpb.Procedure(name, hdl))
	}
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

// Offers is the mesos callback that sends the offers from master
func (h *eventHandler) Offers(ctx context.Context, body *sched.Event) error {
	event := body.GetOffers()
	log.WithField("event", event).Debug("OfferManager: processing Offers event")
	h.offerPool.AddOffers(ctx, event.Offers)

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
