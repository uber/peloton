// Code generated by thriftrw-plugin-yarpc
// @generated

package auroraadminfx

import (
	auroraadminserver "github.com/uber/peloton/.gen/thrift/aurora/api/auroraadminserver"
	fx "go.uber.org/fx"
	transport "go.uber.org/yarpc/api/transport"
	thrift "go.uber.org/yarpc/encoding/thrift"
)

// ServerParams defines the dependencies for the AuroraAdmin server.
type ServerParams struct {
	fx.In

	Handler auroraadminserver.Interface
}

// ServerResult defines the output of AuroraAdmin server module. It provides the
// procedures of a AuroraAdmin handler to an Fx application.
//
// The procedures are provided to the "yarpcfx" value group. Dig 1.2 or newer
// must be used for this feature to work.
type ServerResult struct {
	fx.Out

	Procedures []transport.Procedure `group:"yarpcfx"`
}

// Server provides procedures for AuroraAdmin to an Fx application. It expects a
// auroraadminfx.Interface to be present in the container.
//
// 	fx.Provide(
// 		func(h *MyAuroraAdminHandler) auroraadminserver.Interface {
// 			return h
// 		},
// 		auroraadminfx.Server(),
// 	)
func Server(opts ...thrift.RegisterOption) interface{} {
	return func(p ServerParams) ServerResult {
		procedures := auroraadminserver.New(p.Handler, opts...)
		return ServerResult{Procedures: procedures}
	}
}
