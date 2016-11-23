package mhttp

import (
	"net/http"
	"reflect"
)

// MesosDriver is an inteface used by Inbound to subscribe to a Mesos
// service endpoint such as scheduler, executor, master etc.
type MesosDriver interface {
	// Returns the name of Mesos driver such as scheduler or executor
	Name() string

	// Returns the Mesos endpoint to be connected to
	Endpoint() string

	// Returns the Type of Mesos event message such as
	// mesos.v1.scheduler.Event or mesos.v1.executor.Event
	EventDataType() reflect.Type

	// Returns a subscribe Call message to be sent to Mesos for
	// setting up an event stream connection
	PrepareSubscribeRequest(mesosMasterHostPort string) (*http.Request, error)

	// Invoked after the subscription to Mesos is done
	PostSubscribe(mesosStreamId string)

	// GetContentEncoding returns the http content encoding of the mesos HTTP traffic
	GetContentEncoding() string
}
