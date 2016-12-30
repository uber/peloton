package offer

import (
	"time"

	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	sched "mesos/v1/scheduler"
)

// InitManager inits the offer manager
func InitManager(d yarpc.Dispatcher, offerHoldTime time.Duration, offerPruningPeriod time.Duration, client mpb.Client) *Manager {
	pool := NewOfferPool(d, offerHoldTime, client)
	m := Manager{
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
		mpb.Register(d, mesos.ServiceName, mpb.Procedure(name, hdl))
	}
	return &m
}

// Manager is the offer manager
type Manager struct {
	offerPool   Pool
	offerPruner Pruner
}

// Offers is the mesos callback that sends the offers from master
func (m *Manager) Offers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetOffers()
	log.WithField("event", event).Debug("OfferManager: processing Offers event")
	m.offerPool.AddOffers(event.Offers)

	return nil
}

// InverseOffers is the mesos callback that sends the InverseOffers from master
func (m *Manager) InverseOffers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetInverseOffers()
	log.WithField("event", event).Debug("OfferManager: processing InverseOffers event")

	// TODO: Handle inverse offers from Mesos
	return nil
}

// Rescind offers
func (m *Manager) Rescind(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescind()
	log.WithField("event", event).Debug("OfferManager: processing Rescind event")
	m.offerPool.RescindOffer(event.OfferId)

	return nil
}

// RescindInverseOffer rescinds a inverse offer
func (m *Manager) RescindInverseOffer(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescindInverseOffer()
	log.WithField("event", event).Debug("OfferManager: processing RescindInverseOffer event")
	return nil
}

// Start runs startup related procedures
func (m *Manager) Start() {
	// Start offer pruner
	m.offerPruner.Start()
}

// Stop runs shutdown related procedures
func (m *Manager) Stop() {
	// Clean up all existing offers
	m.offerPool.CleanupOffers()
	// Stop offer pruner
	m.offerPruner.Stop()
}
