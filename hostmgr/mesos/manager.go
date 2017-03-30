package mesos

import (
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"

	sched "mesos/v1/scheduler"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

// InitManager initializes the mesosManager
func InitManager(
	d yarpc.Dispatcher,
	mesosConfig *Config,
	store storage.FrameworkInfoStore) {

	m := mesosManager{
		store:       store,
		mesosConfig: mesosConfig,
	}

	procedures := map[sched.Event_Type]interface{}{
		sched.Event_SUBSCRIBED: m.Subscribed,
		sched.Event_MESSAGE:    m.Message,
		sched.Event_FAILURE:    m.Failure,
		sched.Event_ERROR:      m.Error,
		sched.Event_HEARTBEAT:  m.Heartbeat,
		sched.Event_UNKNOWN:    m.Unknown,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, ServiceName, mpb.Procedure(name, hdl))
	}
}

// mesosManager is a handler for Mesos scheduler API events.
type mesosManager struct {
	store       storage.FrameworkInfoStore
	mesosConfig *Config
}

func (m *mesosManager) Subscribed(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	subscribed := body.GetSubscribed()
	log.WithField("subscribed", subscribed).
		Info("mesosManager: subscribed called")
	frameworkID := subscribed.GetFrameworkId().GetValue()
	name := m.mesosConfig.Framework.Name
	err := m.store.SetMesosFrameworkID(name, frameworkID)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"framework_id": frameworkID,
			"name":         name,
		}).Error("Failed to SetMesosFrameworkId")
	}
	return err
}

func (m *mesosManager) Message(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	msg := body.GetMessage()
	log.WithField("msg", msg).Debug("mesosManager: message called")
	return nil
}

func (m *mesosManager) Failure(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	failure := body.GetFailure()
	log.WithField("failure", failure).Debug("mesosManager: failure called")
	return nil
}

func (m *mesosManager) Error(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	err := body.GetError()
	log.WithField("error", err).Debug("mesosManager: error called")
	return nil
}

func (m *mesosManager) Heartbeat(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	log.Debug("mesosManager: heartbeat called")
	return nil
}

func (m *mesosManager) Unknown(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	log.WithField("event", body).Info("mesosManager: unknown event called")
	return nil
}
