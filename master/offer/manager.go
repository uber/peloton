package offer

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"github.com/yarpc/yarpc-go"

	sched "mesos/v1/scheduler"
)

func InitManager(d yarpc.Dispatcher, offerQueue util.OfferQueue) {
	m := offerManager{
		offerPool:  NewOfferPool(d),
		offerQueue: offerQueue,
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
}

type offerManager struct {
	offerPool OfferPool

	// TODO: to be removed after switching to offer pool
	offerQueue util.OfferQueue
}

func (m *offerManager) Offers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetOffers()
	log.WithField("event", event).Debug("OfferManager: processing Offers event")
	m.offerPool.AddOffers(event.Offers)

	// TODO: to be removed after switching to offer pool
	for _, offer := range event.Offers {
		m.offerQueue.PutOffer(offer)
	}

	return nil
}

func (m *offerManager) InverseOffers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetInverseOffers()
	log.WithField("event", event).Debug("OfferManager: processing InverseOffers event")

	// TODO: Handle inverse offers from Mesos
	return nil
}

func (m *offerManager) Rescind(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescind()
	log.WithField("event", event).Debug("OfferManager: processing Rescind event")
	m.offerPool.RescindOffer(event.OfferId)

	return nil
}

func (m *offerManager) RescindInverseOffer(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	event := body.GetRescindInverseOffer()
	log.WithField("event", event).Debug("OfferManager: processing RescindInverseOffer event")
	return nil
}
