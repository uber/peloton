// Code generated by protoc-gen-yarpc-go
// source: peloton/api/v1alpha/host/svc/host_svc.proto
// DO NOT EDIT!

package svc

import (
	"context"
	"io/ioutil"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/fx"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/encoding/protobuf/reflection"
)

var _ = ioutil.NopCloser

// HostServiceYARPCClient is the YARPC client-side interface for the HostService service.
type HostServiceYARPCClient interface {
	QueryHosts(context.Context, *QueryHostsRequest, ...yarpc.CallOption) (*QueryHostsResponse, error)
	StartMaintenance(context.Context, *StartMaintenanceRequest, ...yarpc.CallOption) (*StartMaintenanceResponse, error)
	CompleteMaintenance(context.Context, *CompleteMaintenanceRequest, ...yarpc.CallOption) (*CompleteMaintenanceResponse, error)
}

// NewHostServiceYARPCClient builds a new YARPC client for the HostService service.
func NewHostServiceYARPCClient(clientConfig transport.ClientConfig, options ...protobuf.ClientOption) HostServiceYARPCClient {
	return &_HostServiceYARPCCaller{protobuf.NewStreamClient(
		protobuf.ClientParams{
			ServiceName:  "peloton.api.v1alpha.host.svc.HostService",
			ClientConfig: clientConfig,
			Options:      options,
		},
	)}
}

// HostServiceYARPCServer is the YARPC server-side interface for the HostService service.
type HostServiceYARPCServer interface {
	QueryHosts(context.Context, *QueryHostsRequest) (*QueryHostsResponse, error)
	StartMaintenance(context.Context, *StartMaintenanceRequest) (*StartMaintenanceResponse, error)
	CompleteMaintenance(context.Context, *CompleteMaintenanceRequest) (*CompleteMaintenanceResponse, error)
}

// BuildHostServiceYARPCProcedures prepares an implementation of the HostService service for YARPC registration.
func BuildHostServiceYARPCProcedures(server HostServiceYARPCServer) []transport.Procedure {
	handler := &_HostServiceYARPCHandler{server}
	return protobuf.BuildProcedures(
		protobuf.BuildProceduresParams{
			ServiceName: "peloton.api.v1alpha.host.svc.HostService",
			UnaryHandlerParams: []protobuf.BuildProceduresUnaryHandlerParams{
				{
					MethodName: "QueryHosts",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.QueryHosts,
							NewRequest: newHostServiceServiceQueryHostsYARPCRequest,
						},
					),
				},
				{
					MethodName: "StartMaintenance",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.StartMaintenance,
							NewRequest: newHostServiceServiceStartMaintenanceYARPCRequest,
						},
					),
				},
				{
					MethodName: "CompleteMaintenance",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.CompleteMaintenance,
							NewRequest: newHostServiceServiceCompleteMaintenanceYARPCRequest,
						},
					),
				},
			},
			OnewayHandlerParams: []protobuf.BuildProceduresOnewayHandlerParams{},
			StreamHandlerParams: []protobuf.BuildProceduresStreamHandlerParams{},
		},
	)
}

// FxHostServiceYARPCClientParams defines the input
// for NewFxHostServiceYARPCClient. It provides the
// paramaters to get a HostServiceYARPCClient in an
// Fx application.
type FxHostServiceYARPCClientParams struct {
	fx.In

	Provider yarpc.ClientConfig
}

// FxHostServiceYARPCClientResult defines the output
// of NewFxHostServiceYARPCClient. It provides a
// HostServiceYARPCClient to an Fx application.
type FxHostServiceYARPCClientResult struct {
	fx.Out

	Client HostServiceYARPCClient

	// We are using an fx.Out struct here instead of just returning a client
	// so that we can add more values or add named versions of the client in
	// the future without breaking any existing code.
}

// NewFxHostServiceYARPCClient provides a HostServiceYARPCClient
// to an Fx application using the given name for routing.
//
//  fx.Provide(
//    svc.NewFxHostServiceYARPCClient("service-name"),
//    ...
//  )
func NewFxHostServiceYARPCClient(name string, options ...protobuf.ClientOption) interface{} {
	return func(params FxHostServiceYARPCClientParams) FxHostServiceYARPCClientResult {
		return FxHostServiceYARPCClientResult{
			Client: NewHostServiceYARPCClient(params.Provider.ClientConfig(name), options...),
		}
	}
}

// FxHostServiceYARPCProceduresParams defines the input
// for NewFxHostServiceYARPCProcedures. It provides the
// paramaters to get HostServiceYARPCServer procedures in an
// Fx application.
type FxHostServiceYARPCProceduresParams struct {
	fx.In

	Server HostServiceYARPCServer
}

// FxHostServiceYARPCProceduresResult defines the output
// of NewFxHostServiceYARPCProcedures. It provides
// HostServiceYARPCServer procedures to an Fx application.
//
// The procedures are provided to the "yarpcfx" value group.
// Dig 1.2 or newer must be used for this feature to work.
type FxHostServiceYARPCProceduresResult struct {
	fx.Out

	Procedures     []transport.Procedure `group:"yarpcfx"`
	ReflectionMeta reflection.ServerMeta `group:"yarpcfx"`
}

// NewFxHostServiceYARPCProcedures provides HostServiceYARPCServer procedures to an Fx application.
// It expects a HostServiceYARPCServer to be present in the container.
//
//  fx.Provide(
//    svc.NewFxHostServiceYARPCProcedures(),
//    ...
//  )
func NewFxHostServiceYARPCProcedures() interface{} {
	return func(params FxHostServiceYARPCProceduresParams) FxHostServiceYARPCProceduresResult {
		return FxHostServiceYARPCProceduresResult{
			Procedures: BuildHostServiceYARPCProcedures(params.Server),
			ReflectionMeta: reflection.ServerMeta{
				ServiceName:     "peloton.api.v1alpha.host.svc.HostService",
				FileDescriptors: yarpcFileDescriptorClosure929be394fb5675b4,
			},
		}
	}
}

type _HostServiceYARPCCaller struct {
	streamClient protobuf.StreamClient
}

func (c *_HostServiceYARPCCaller) QueryHosts(ctx context.Context, request *QueryHostsRequest, options ...yarpc.CallOption) (*QueryHostsResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "QueryHosts", request, newHostServiceServiceQueryHostsYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*QueryHostsResponse)
	if !ok {
		return nil, protobuf.CastError(emptyHostServiceServiceQueryHostsYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_HostServiceYARPCCaller) StartMaintenance(ctx context.Context, request *StartMaintenanceRequest, options ...yarpc.CallOption) (*StartMaintenanceResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "StartMaintenance", request, newHostServiceServiceStartMaintenanceYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*StartMaintenanceResponse)
	if !ok {
		return nil, protobuf.CastError(emptyHostServiceServiceStartMaintenanceYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_HostServiceYARPCCaller) CompleteMaintenance(ctx context.Context, request *CompleteMaintenanceRequest, options ...yarpc.CallOption) (*CompleteMaintenanceResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "CompleteMaintenance", request, newHostServiceServiceCompleteMaintenanceYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*CompleteMaintenanceResponse)
	if !ok {
		return nil, protobuf.CastError(emptyHostServiceServiceCompleteMaintenanceYARPCResponse, responseMessage)
	}
	return response, err
}

type _HostServiceYARPCHandler struct {
	server HostServiceYARPCServer
}

func (h *_HostServiceYARPCHandler) QueryHosts(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *QueryHostsRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*QueryHostsRequest)
		if !ok {
			return nil, protobuf.CastError(emptyHostServiceServiceQueryHostsYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.QueryHosts(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_HostServiceYARPCHandler) StartMaintenance(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *StartMaintenanceRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*StartMaintenanceRequest)
		if !ok {
			return nil, protobuf.CastError(emptyHostServiceServiceStartMaintenanceYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.StartMaintenance(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_HostServiceYARPCHandler) CompleteMaintenance(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *CompleteMaintenanceRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*CompleteMaintenanceRequest)
		if !ok {
			return nil, protobuf.CastError(emptyHostServiceServiceCompleteMaintenanceYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.CompleteMaintenance(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func newHostServiceServiceQueryHostsYARPCRequest() proto.Message {
	return &QueryHostsRequest{}
}

func newHostServiceServiceQueryHostsYARPCResponse() proto.Message {
	return &QueryHostsResponse{}
}

func newHostServiceServiceStartMaintenanceYARPCRequest() proto.Message {
	return &StartMaintenanceRequest{}
}

func newHostServiceServiceStartMaintenanceYARPCResponse() proto.Message {
	return &StartMaintenanceResponse{}
}

func newHostServiceServiceCompleteMaintenanceYARPCRequest() proto.Message {
	return &CompleteMaintenanceRequest{}
}

func newHostServiceServiceCompleteMaintenanceYARPCResponse() proto.Message {
	return &CompleteMaintenanceResponse{}
}

var (
	emptyHostServiceServiceQueryHostsYARPCRequest           = &QueryHostsRequest{}
	emptyHostServiceServiceQueryHostsYARPCResponse          = &QueryHostsResponse{}
	emptyHostServiceServiceStartMaintenanceYARPCRequest     = &StartMaintenanceRequest{}
	emptyHostServiceServiceStartMaintenanceYARPCResponse    = &StartMaintenanceResponse{}
	emptyHostServiceServiceCompleteMaintenanceYARPCRequest  = &CompleteMaintenanceRequest{}
	emptyHostServiceServiceCompleteMaintenanceYARPCResponse = &CompleteMaintenanceResponse{}
)

var yarpcFileDescriptorClosure929be394fb5675b4 = [][]byte{
	// peloton/api/v1alpha/host/svc/host_svc.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x93, 0x4f, 0x4b, 0xc3, 0x40,
		0x10, 0xc5, 0x29, 0x05, 0xa1, 0x53, 0x10, 0x5d, 0x0f, 0x96, 0x58, 0x41, 0xd2, 0x8b, 0x20, 0x6c,
		0xb4, 0xe2, 0xdf, 0x9b, 0xd6, 0x83, 0x1e, 0x3c, 0x98, 0x1e, 0x44, 0x2f, 0x65, 0x1b, 0xa6, 0x24,
		0xd0, 0xee, 0xac, 0x99, 0x6d, 0xc0, 0xa3, 0x57, 0x2f, 0x7e, 0x65, 0xe9, 0x26, 0xa5, 0x62, 0x9b,
		0x60, 0xbd, 0x0d, 0xe1, 0xbd, 0xdf, 0xbe, 0xbc, 0x61, 0xe0, 0xc8, 0xe0, 0x98, 0x2c, 0xe9, 0x40,
		0x99, 0x24, 0xc8, 0x4e, 0xd4, 0xd8, 0xc4, 0x2a, 0x88, 0x89, 0x6d, 0xc0, 0x59, 0xe4, 0x86, 0x01,
		0x67, 0x91, 0x34, 0x29, 0x59, 0x12, 0xed, 0x42, 0x2c, 0x95, 0x49, 0x64, 0x21, 0x96, 0x33, 0x8d,
		0xe4, 0x2c, 0xf2, 0x3a, 0xa5, 0x28, 0x27, 0x71, 0x08, 0xff, 0x05, 0xb6, 0x9f, 0xa6, 0x98, 0xbe,
		0xdf, 0x13, 0x5b, 0x0e, 0xf1, 0x6d, 0x8a, 0x6c, 0xc5, 0x1d, 0x34, 0xf3, 0x97, 0xac, 0xb2, 0xc8,
		0xad, 0xda, 0x41, 0xfd, 0x70, 0xb3, 0xdb, 0x91, 0xa5, 0xaf, 0xcd, 0xcc, 0xfd, 0x99, 0x36, 0x84,
		0x78, 0x3e, 0xb2, 0xff, 0x0c, 0xe2, 0x27, 0x9a, 0x0d, 0x69, 0x46, 0x71, 0x03, 0x4e, 0x33, 0x48,
		0xf4, 0x88, 0x72, 0x74, 0xb3, 0xeb, 0x57, 0xa3, 0x1f, 0xf4, 0x88, 0xc2, 0x46, 0x5c, 0x4c, 0xec,
		0x5f, 0xc0, 0x6e, 0xdf, 0xaa, 0xd4, 0x3e, 0xaa, 0x44, 0x5b, 0xd4, 0x4a, 0x47, 0x38, 0x4f, 0xde,
		0x06, 0xa7, 0xd3, 0x6a, 0x52, 0xe4, 0x6e, 0x84, 0x8b, 0x0f, 0xbe, 0x07, 0xad, 0x65, 0x63, 0x9e,
		0xcb, 0xbf, 0x06, 0xaf, 0x47, 0x13, 0x33, 0x46, 0x8b, 0x6b, 0x73, 0xf7, 0x61, 0x6f, 0xa5, 0x37,
		0x47, 0x77, 0xbf, 0xea, 0xd0, 0x74, 0x15, 0x61, 0x9a, 0x25, 0x11, 0x0a, 0x02, 0x58, 0x14, 0x23,
		0x02, 0x59, 0xb5, 0x45, 0xb9, 0xb4, 0x1d, 0xef, 0xf8, 0xef, 0x86, 0xa2, 0xf3, 0x8f, 0x1a, 0x6c,
		0xfd, 0xfe, 0x71, 0x71, 0x56, 0x8d, 0x29, 0x69, 0xd8, 0x3b, 0x5f, 0xd7, 0x56, 0x64, 0xf8, 0xac,
		0xc1, 0xce, 0x8a, 0x92, 0xc4, 0x65, 0x35, 0xaf, 0x7c, 0x27, 0xde, 0xd5, 0x3f, 0x9c, 0x79, 0x98,
		0xdb, 0x1e, 0x74, 0x22, 0x9a, 0x54, 0xfb, 0xcd, 0xf0, 0xb5, 0x5d, 0x75, 0x8c, 0xc3, 0x0d, 0x77,
		0x41, 0xa7, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0xf0, 0x55, 0x8b, 0xff, 0xb3, 0x03, 0x00, 0x00,
	},
	// peloton/api/v1alpha/host/host.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x52, 0x2e, 0x48, 0xcd, 0xc9,
		0x2f, 0xc9, 0xcf, 0xd3, 0x4f, 0x2c, 0xc8, 0xd4, 0x2f, 0x33, 0x4c, 0xcc, 0x29, 0xc8, 0x48, 0xd4,
		0xcf, 0xc8, 0x2f, 0x2e, 0x01, 0x13, 0x7a, 0x05, 0x45, 0xf9, 0x25, 0xf9, 0x42, 0x12, 0x50, 0x45,
		0x7a, 0x89, 0x05, 0x99, 0x7a, 0x50, 0x45, 0x7a, 0x20, 0x79, 0xa5, 0x42, 0x2e, 0x0e, 0x8f, 0xfc,
		0xe2, 0x12, 0xcf, 0xbc, 0xb4, 0x7c, 0x21, 0x29, 0x2e, 0x0e, 0x90, 0x58, 0x5e, 0x62, 0x6e, 0xaa,
		0x04, 0xa3, 0x02, 0xa3, 0x06, 0x67, 0x10, 0x9c, 0x2f, 0xc4, 0xc7, 0xc5, 0x94, 0x59, 0x20, 0xc1,
		0x04, 0x16, 0x65, 0xca, 0x2c, 0x10, 0xb2, 0xe4, 0x62, 0x2d, 0x2e, 0x49, 0x2c, 0x49, 0x95, 0x60,
		0x56, 0x60, 0xd4, 0xe0, 0x33, 0x52, 0xd6, 0xc3, 0x65, 0x83, 0x1e, 0xc8, 0xf8, 0x60, 0x90, 0xd2,
		0x20, 0x88, 0x0e, 0xad, 0x29, 0x8c, 0x5c, 0x9c, 0x70, 0x41, 0x21, 0x31, 0x2e, 0x21, 0x0f, 0xff,
		0xe0, 0x90, 0xf8, 0xe0, 0x10, 0xc7, 0x10, 0xd7, 0x78, 0x4f, 0xbf, 0x30, 0x47, 0x1f, 0x4f, 0x17,
		0x01, 0x06, 0x34, 0xf1, 0x50, 0x3f, 0x6f, 0x3f, 0xff, 0x70, 0x3f, 0x01, 0x46, 0x21, 0x41, 0x2e,
		0x5e, 0x64, 0xf1, 0x00, 0x01, 0x26, 0x21, 0x71, 0x2e, 0x61, 0x24, 0x21, 0x97, 0x20, 0x47, 0x4f,
		0x3f, 0x4f, 0x3f, 0x77, 0x01, 0x66, 0x34, 0x33, 0xc0, 0x12, 0xae, 0x2e, 0x02, 0x2c, 0x42, 0xc2,
		0x5c, 0xfc, 0xc8, 0xe2, 0x20, 0x83, 0x59, 0x93, 0xd8, 0xc0, 0x41, 0x65, 0x0c, 0x08, 0x00, 0x00,
		0xff, 0xff, 0x4e, 0x65, 0x49, 0x18, 0x51, 0x01, 0x00, 0x00,
	},
}

func init() {
	yarpc.RegisterClientBuilder(
		func(clientConfig transport.ClientConfig, structField reflect.StructField) HostServiceYARPCClient {
			return NewHostServiceYARPCClient(clientConfig, protobuf.ClientBuilderOptions(clientConfig, structField)...)
		},
	)
}
