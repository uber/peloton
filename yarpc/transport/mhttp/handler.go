package mhttp

import (
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"go.uber.org/yarpc/transport"
	"golang.org/x/net/context"
	"reflect"
)

// handler adapts a transport.Handler into a handler for net/http.
type handler struct {
	ServiceDetail transport.ServiceDetail
	Service       string
	Caller        string
	EventDataType reflect.Type
	ContentType   string
}

// Handle a record IO frame by unmarshal the mesos event and invoke
// transport.handler
func (h handler) HandleRecordIO(data []byte) error {
	body, err := mpb.NewMesosEventReader(data, h.EventDataType, h.ContentType)
	if err != nil {
		return err
	}

	// We will decode Procedure later in encoding layer so that we can
	// void unmarshal twice.
	procedure := body.Type.String()
	treq := &transport.Request{
		Caller:    h.Caller,
		Service:   h.Service,
		Procedure: procedure,
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
	var handlerSpec transport.HandlerSpec
	handlerSpec, err = h.ServiceDetail.Registry.Choose(ctx, treq)
	if err != nil {
		return err
	}
	// TODO: capture and handle panic
	return handlerSpec.Unary().Handle(ctx, treq, newResponseWriter())
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
