package mesos

import (
	"github.com/yarpc/yarpc-go"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"

	sched "mesos/v1/scheduler"
)

func InitManager(d yarpc.Dispatcher) {
	m := mesosManager{}

	procedures := map[sched.Event_Type]interface{} {
		sched.Event_SUBSCRIBED:           m.Subscribed,
		sched.Event_MESSAGE:              m.Message,
		sched.Event_FAILURE:              m.Failure,
		sched.Event_ERROR:                m.Error,
		sched.Event_HEARTBEAT:            m.Heartbeat,
		sched.Event_UNKNOWN:              m.Unknown,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mjson.Register(d, ServiceName, mjson.Procedure(name, hdl))
	}
}

type mesosManager struct {
}

func (m *mesosManager) Subscribed(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	subscribed := body.GetSubscribed()
	log.WithField("params", subscribed).Debug("mesosManager: subscribed called")
	return nil
}

func (m *mesosManager) Message(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	msg := body.GetMessage()
	log.WithField("params", msg).Debug("mesosManager: message called")
	return nil
}

func (m *mesosManager) Failure(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	failure := body.GetFailure()
	log.WithField("params", failure).Debug("mesosManager: failure called")
	return nil
}

func (m *mesosManager) Error(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	err := body.GetError()
	log.WithField("params", err).Debug("mesosManager: error called")
	return nil
}

func (m *mesosManager) Heartbeat(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	log.Infof("mesosManager: heartbeat called")
	return nil
}

func (m *mesosManager) Unknown(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	log.Infof("mesosManager: unknown event called")
	return nil
}
