package offer

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/common/background"
	"code.uber.internal/infra/peloton/hostmgr/binpacking"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"
	"code.uber.internal/infra/peloton/hostmgr/prune"
	"code.uber.internal/infra/peloton/hostmgr/reservation/cleaner"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

const (
	_hostPrunerName              = "hostpruner"
	_resourceCleanerName         = "resourceCleaner"
	_resourceCleanerPeriod       = 15 * time.Minute
	_resourceCleanerInitialDelay = 15 * time.Minute

	_poolMetricsRefresh       = "poolMetricsRefresh"
	_poolMetricsRefreshPeriod = 60 * time.Second
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
	hostPruningPeriodSec time.Duration,
	scarceResourceTypes []string,
	slackResourceTypes []string,
	ranker binpacking.Ranker) {

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
	)
	hostPruner := prune.NewHostPruner(
		pool,
		parent.SubScope(_hostPrunerName),
	)
	backgroundMgr.RegisterWorks(
		background.Work{
			Name:   _hostPrunerName,
			Func:   hostPruner.Prune,
			Period: hostPruningPeriodSec,
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

	h.metrics.OfferEvents.Inc(1)

	return nil
}

// InverseOffers is the mesos callback that sends the InverseOffers from master
func (h *eventHandler) InverseOffers(ctx context.Context, body *sched.Event) error {

	event := body.GetInverseOffers()
	log.WithField("event", event).
		Debug("OfferManager: processing InverseOffers event")

	h.metrics.InverseOfferEvents.Inc(1)

	// TODO: Handle inverse offers from Mesos
	return nil
}

// Rescind offers
func (h *eventHandler) Rescind(ctx context.Context, body *sched.Event) error {

	event := body.GetRescind()
	log.WithField("event", event).Debug("OfferManager: processing Rescind event")
	h.offerPool.RescindOffer(event.OfferId)

	h.metrics.RescindEvents.Inc(1)

	return nil
}

// RescindInverseOffer rescinds a inverse offer
func (h *eventHandler) RescindInverseOffer(ctx context.Context, body *sched.Event) error {

	event := body.GetRescindInverseOffer()
	log.WithField("event", event).
		Debug("OfferManager: processing RescindInverseOffer event")

	h.metrics.RescindInverseOfferEvents.Inc(1)
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
