package mpb

import (
	"fmt"
	"reflect"

	"context"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
)

const _getTypeMethod = "GetType"

var _invalidMethod = reflect.Value{}

// EventType is an interface for auto-generated event types from
// Mesos HTTP API such as mesos/v1/scheduler.proto
type EventType interface {
	String() string
}

// MesosEvent is an interface for auto-generated events from Mesos HTTP API
// such as mesos/v1/scheduler.proto
type MesosEvent interface {
	GetType() EventType
}

// MesosEventReader decodes a Mesos Event object from RecordIO frame
type MesosEventReader struct {
	Event reflect.Value // Decoded Mesos event
	Type  EventType     // Mesos event type such as offers, rescind etc
}

// NewMesosEventReader creates a new MesosEventReader
func NewMesosEventReader(data []byte, typ reflect.Type, contentType string) (
	*MesosEventReader, error) {

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Wrong mesos event type: %s", typ)
	}

	// Decode the RecordIO frame to Protobuf event
	event := reflect.New(typ)
	if err := UnmarshalPbMessage(data, event, contentType); err != nil {
		return nil, err
	}

	// Get the event type of the Mesos event
	method := event.MethodByName(_getTypeMethod)
	if method == _invalidMethod {
		return nil, fmt.Errorf(
			"Event object does not have method %s",
			_getTypeMethod)
	}
	result := method.Call([]reflect.Value{})[0]
	eventType, ok := result.Interface().(EventType)
	if !ok {
		return nil, fmt.Errorf(
			"Result %v is not of event type %v", result, eventType)
	}

	// TODO: Decode the nested event object using reflect.MethodByName
	reader := &MesosEventReader{Event: event, Type: eventType}
	return reader, nil
}

func (r MesosEventReader) Read(p []byte) (_ int, _ error) {
	// TODO: make Request.Body in YARPC to be interface{}
	// instead of io.Reader
	panic("mesosEventReader does not support Read method")
}

// mpbHandler adapts a Mesos event handler into a transport-level
// Handler.
//
// The wrapped function must already be in the correct format:
//
// 	f(reqMeta yarpc.ReqMeta, body $reqBody) error
type mpbHandler struct {
	handler reflect.Value
}

func (h mpbHandler) Handle(
	ctx context.Context,
	treq *transport.Request,
	rw transport.ResponseWriter) error {

	reader := treq.Body.(*MesosEventReader)
	meta := reflect.ValueOf(reqMeta{req: treq})
	results := h.handler.Call([]reflect.Value{meta, reader.Event})

	if err := results[0].Interface(); err != nil {
		return err.(error)
	}

	return nil
}

type reqMeta struct {
	req *transport.Request
}

func (r reqMeta) Caller() string {
	return r.req.Caller
}

func (r reqMeta) Encoding() transport.Encoding {
	return r.req.Encoding
}

func (r reqMeta) Headers() yarpc.Headers {
	return yarpc.Headers(r.req.Headers)
}

func (r reqMeta) Procedure() string {
	return r.req.Procedure
}

func (r reqMeta) Service() string {
	return r.req.Service
}
