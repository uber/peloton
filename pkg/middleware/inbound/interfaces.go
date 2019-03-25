package inbound

import "go.uber.org/yarpc/api/middleware"

// DispatcherInboundMiddleWare implements the union of
// all inbound middleware interface, so all of the peloton
// inbound requests can utilize the middleware
type DispatcherInboundMiddleWare interface {
	middleware.UnaryInbound
	middleware.OnewayInbound
	middleware.StreamInbound
}
