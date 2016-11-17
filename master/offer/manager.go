package offer

import (
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"go.uber.org/yarpc"
	sched "mesos/v1/scheduler"
)

func InitManager(d yarpc.Dispatcher, offerHoldTime time.Duration, offerPruningPeriod time.Duration) *OfferManager {
	pool := NewOfferPool(d, offerHoldTime)
	m := OfferManager{
		offerPool:   pool,
		offerPruner: NewOfferPruner(pool, offerPruningPeriod, d),
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_OFFERS:                m.Offers,
		sched.Event_INVERSE_OFFERS:        m.InverseOffers,
		sched.Event_RESCIND:               m.Rescind,
		sched.Event_RESCIND_INVERSE_OFFER: m.RescindInverseOffer,
	}

	for typ, hdl := range procedures {
		name := typ.String()
		mjson.Register(d, mesos.ServiceName, mjson.Procedure(name, hdl))
	}
	return &m
}

type OfferManager struct {
	offerPool   OfferPool
	offerPruner OfferPruner
}

func (m *OfferManager) Offers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetOffers()
	log.WithField("event", event).Debug("OfferManager: processing Offers event")
	m.offerPool.AddOffers(event.Offers)

	return nil
}

func (m *OfferManager) InverseOffers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetInverseOffers()
	log.WithField("event", event).Debug("OfferManager: processing InverseOffers event")

	// TODO: Handle inverse offers from Mesos
	return nil
}

func (m *OfferManager) Rescind(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescind()
	log.WithField("event", event).Debug("OfferManager: processing Rescind event")
	m.offerPool.RescindOffer(event.OfferId)

	return nil
}

func (m *OfferManager) RescindInverseOffer(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescindInverseOffer()
	log.WithField("event", event).Debug("OfferManager: processing RescindInverseOffer event")
	return nil
}

// Start runs startup related procedures
func (m *OfferManager) Start() {
	// Start offer pruner
	m.offerPruner.Start()
}

// Stop runs shutdown related procedures
func (m *OfferManager) Stop() {
	// Clean up all existing offers
	m.offerPool.CleanupOffers()
	// Stop offer pruner
	m.offerPruner.Stop()
}
