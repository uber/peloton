package mjson

import (
	"reflect"

	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"golang.org/x/net/context"
)

// mjsonHandler adapts a Mesos event handler into a transport-level
// Handler.
//
// The wrapped function must already be in the correct format:
//
// 	f(reqMeta yarpc.ReqMeta, body $reqBody) error
type mjsonHandler struct {
	handler reflect.Value
}

func (h mjsonHandler) Handle(
	ctx context.Context,
	_ transport.Options,
	treq *transport.Request,
	rw transport.ResponseWriter) error {

	reader := treq.Body.(*mhttp.MesosEventReader)
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
