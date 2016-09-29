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
	m := offerManager{}
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
	m.offerQueue = offerQueue
}

type offerManager struct {
	offerQueue util.OfferQueue
}

func (m *offerManager) Offers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	offers := body.GetOffers()
	log.WithField("params", offers).Debug("OfferManager.Offers called")
	for _, offer := range offers.Offers {
		m.offerQueue.PutOffer(offer)
	}
	return nil
}

func (m *offerManager) InverseOffers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	inverseOffers := body.GetInverseOffers()
	log.WithField("params", inverseOffers).Debug("OfferManager.InverseOffers called")
	return nil
}

func (m *offerManager) Rescind(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	rescind := body.GetRescind()
	log.WithField("params", rescind).Debug("OfferManager.Rescind called")
	return nil
}

func (m *offerManager) RescindInverseOffer(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	rescind := body.GetRescindInverseOffer()
	log.WithField("params", rescind).Debug("OfferManager.RescindInverseOffers called")
	return nil
}
