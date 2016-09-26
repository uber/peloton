package offer

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	myarpc "code.uber.internal/infra/peloton/yarpc"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"fmt"
	"github.com/yarpc/yarpc-go"
	mesos_v1 "mesos/v1"
	sched "mesos/v1/scheduler"
)

func InitManager(d yarpc.Dispatcher, caller myarpc.Caller) {
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
	m.caller = caller
}

type offerManager struct {
	caller myarpc.Caller
}

func (m *offerManager) Offers(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	offers := body.GetOffers()
	launchTestTask(m.caller, offers)
	log.WithField("params", offers).Debug("OfferManager.Offers called")
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

// TEMPORARY TEST CODE
// TODO: remove later after integration test to launch task is in place. Also this should not be put int production
var taskIndex = 0

func launchTestTask(caller myarpc.Caller, offers *sched.Event_Offers) {
	callType := sched.Call_ACCEPT
	var offerIds []*mesos_v1.OfferID
	for _, offer := range offers.Offers {
		offerIds = append(offerIds, offer.Id)
	}

	var taskName = "test_task_0"
	var cmdVal = "ls /tmp"
	var shell = true
	var taskId = fmt.Sprintf("test_task_whatever_%v", taskIndex)
	taskIndex++
	if taskIndex > 10 {
		return
	}
	var rs []*mesos_v1.Resource
	resources := offers.Offers[0].Resources
	for _, r := range resources {
		if *r.Type != mesos_v1.Value_RANGES {
			rs = append(rs, r)
		}
	}
	opType := mesos_v1.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: offers.Offers[0].FrameworkId,
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: offerIds,
			Operations: []*mesos_v1.Offer_Operation{
				&mesos_v1.Offer_Operation{
					Type: &opType,
					Launch: &mesos_v1.Offer_Operation_Launch{
						TaskInfos: []*mesos_v1.TaskInfo{
							&mesos_v1.TaskInfo{
								Name: &taskName,
								TaskId: &mesos_v1.TaskID{
									Value: &taskId,
								},
								AgentId:   offers.Offers[0].AgentId,
								Resources: rs,
								Command: &mesos_v1.CommandInfo{
									Shell: &shell,
									Value: &cmdVal,
								},
							},
						},
					},
				},
			},
		},
	}
	caller.SendPbRequest(msg)
}
