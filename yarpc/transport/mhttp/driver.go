package mhttp

import (
	"reflect"

	"github.com/golang/protobuf/proto"
)

// EventType is an interface for auto-generated event types from
// Mesos HTTP API such as mesos/v1/scheduler.proto
type EventType interface {
	String() string
}

// Event is an interface for auto-generated events from Mesos HTTP API
// such as mesos/v1/scheduler.proto
type MesosEvent interface {
	GetType() EventType
}

// MesosDriver is an inteface used by Inbound to subscribe to a Mesos
// service endpoint such as scheduler, executor, master etc.
type MesosDriver interface {
	// Returns the name of Mesos driver such as scheduler or executor
	Name() string

	// Returns the Mesos endpoint to be connected to
	Endpoint() string

	// Returns a subscribe Call message to be sent to Mesos for
	// setting up an event stream connection
	Subscribe() proto.Message

	// Returns the Type of Mesos event message such as
	// mesos.v1.scheduler.Event or mesos.v1.executor.Event
	EventDataType() reflect.Type
}
