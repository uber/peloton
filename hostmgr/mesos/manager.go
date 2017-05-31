package mesos

import (
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"

	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

// InitManager initializes the mesosManager
func InitManager(
	d *yarpc.Dispatcher,
	mesosConfig *Config,
	store storage.FrameworkInfoStore) {

	m := mesosManager{
		store:         store,
		frameworkName: mesosConfig.Framework.Name,
	}

	for name, hdl := range getCallbacks(&m) {
		mpb.Register(d, ServiceName, mpb.Procedure(name, hdl))
	}
}

// mesosManager is a handler for Mesos scheduler API events.
type mesosManager struct {
	store         storage.FrameworkInfoStore
	frameworkName string
}

type schedulerEventCallback func(*sched.Event) error

// getCallbacks returns a map from name to usable callbacks
// which can be registered to yarpc.Dispatcher.
func getCallbacks(m *mesosManager) map[string]schedulerEventCallback {
	procedures := make(map[string]schedulerEventCallback)
	callbacks := map[sched.Event_Type]schedulerEventCallback{
		sched.Event_SUBSCRIBED: m.Subscribed,
		sched.Event_MESSAGE:    m.Message,
		sched.Event_FAILURE:    m.Failure,
		sched.Event_ERROR:      m.Error,
		sched.Event_HEARTBEAT:  m.Heartbeat,
		sched.Event_UNKNOWN:    m.Unknown,
	}
	for typ, hdl := range callbacks {
		name := typ.String()
		procedures[name] = hdl
	}
	return procedures
}

func (m *mesosManager) Subscribed(body *sched.Event) error {

	subscribed := body.GetSubscribed()
	log.WithField("subscribed", subscribed).
		Info("mesosManager: subscribed called")
	frameworkID := subscribed.GetFrameworkId().GetValue()
	err := m.store.SetMesosFrameworkID(m.frameworkName, frameworkID)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"framework_id": frameworkID,
			"name":         m.frameworkName,
		}).Error("Failed to SetMesosFrameworkId")
	}
	return err
}

func (m *mesosManager) Message(body *sched.Event) error {

	msg := body.GetMessage()
	log.WithField("msg", msg).Debug("mesosManager: message called")
	return nil
}

func (m *mesosManager) Failure(body *sched.Event) error {

	failure := body.GetFailure()
	log.WithField("failure", failure).Debug("mesosManager: failure called")
	return nil
}

func (m *mesosManager) Error(body *sched.Event) error {

	err := body.GetError()
	log.WithField("error", err).Debug("mesosManager: error called")
	return nil
}

func (m *mesosManager) Heartbeat(body *sched.Event) error {

	log.Debug("mesosManager: heartbeat called")
	return nil
}

func (m *mesosManager) Unknown(body *sched.Event) error {

	log.WithField("event", body).Info("mesosManager: unknown event called")
	return nil
}
