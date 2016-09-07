package mhttp

import (
	"fmt"
	"reflect"
	"encoding/json"
	"bytes"

	"github.com/yarpc/yarpc-go/transport"
	"golang.org/x/net/context"
)

var httpOptions transport.Options

// handler adapts a transport.Handler into a handler for net/http.
type handler struct {
	Handler       transport.Handler
	Service       string
	Caller        string
	EventDataType reflect.Type
}

// Handle a record IO frame by unmarshal the mesos event and invoke
// transport.handler
func (h handler) HandleRecordIO(data []byte) error {

	body, err := newMesosEventReader(data, h.EventDataType)
	if err != nil {
		return err
	}

	// We will decode Procedure later in encoding layer so that we can
	// void unmarshal twice.
	treq := &transport.Request{
		Caller:    h.Caller,
		Service:   h.Service,
		Procedure: body.Type.String(),
		Encoding:  transport.Encoding("mesos"),
		Headers:   transport.NewHeaders(),
		Body:      body,
	}

	ctx := context.Background()

	// TODO: make request.Validator public in yarpc
	// v := request.Validator{Request: treq}
	// treq, err := v.Validate(ctx)
	// if err != nil {
	//	  return err
	// }

	// TODO: capture and handle panic
	return h.Handler.Handle(ctx, httpOptions, treq, newResponseWriter())
}

// mesosEventReader decodes a Mesos Event object from RecordIO frame
type MesosEventReader struct {
	Event      reflect.Value  // Decoded Mesos event
	Type       EventType      // Mesos event type such as offers, rescind etc
}

func newMesosEventReader(data []byte, typ reflect.Type) (*MesosEventReader, error) {
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Wrong mesos event type: %s", typ)
	}

	// Decode the RecordIO frame to Probuf event
	event := reflect.New(typ)
	err := json.NewDecoder(bytes.NewReader(data)).Decode(event.Interface())
	if err != nil {
		return nil, err
	}

	// Get the event type of the Mesos event
	method := event.MethodByName("GetType")
	result := method.Call([]reflect.Value{})[0]
	eventType := result.Interface().(EventType)

	// TODO: Decode the nested event object using reflect.MethodByName
	reader := &MesosEventReader{Event: event, Type: eventType}
	return reader, nil
}

func (r MesosEventReader) Read(p []byte) (n int, err error) {
	// TODO: make Request.Body in YARPC to be interface{} instead of io.Reader
	panic("mesosEventReader does not support Read method")
}

// responseWriter implements a dummy transport.ResponseWriter
// since Mesos events are one-way.
type responseWriter struct {
}

func newResponseWriter() responseWriter {
	return responseWriter{}
}

func (rw responseWriter) Write(s []byte) (int, error) {
	// Nothing to do.
	panic("Mesos event does not support response")
}

func (rw responseWriter) AddHeaders(h transport.Headers) {
	// Nothing to do.
	panic("Mesos event does not support response headers")
}

func (responseWriter) SetApplicationError() {
	// Nothing to do.
	panic("Mesos event does not support application error")
}
