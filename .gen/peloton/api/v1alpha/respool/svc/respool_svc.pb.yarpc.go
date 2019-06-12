// Code generated by protoc-gen-yarpc-go
// source: peloton/api/v1alpha/respool/svc/respool_svc.proto
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

// ResourcePoolServiceYARPCClient is the YARPC client-side interface for the ResourcePoolService service.
type ResourcePoolServiceYARPCClient interface {
	CreateResourcePool(context.Context, *CreateResourcePoolRequest, ...yarpc.CallOption) (*CreateResourcePoolResponse, error)
	GetResourcePool(context.Context, *GetResourcePoolRequest, ...yarpc.CallOption) (*GetResourcePoolResponse, error)
	DeleteResourcePool(context.Context, *DeleteResourcePoolRequest, ...yarpc.CallOption) (*DeleteResourcePoolResponse, error)
	UpdateResourcePool(context.Context, *UpdateResourcePoolRequest, ...yarpc.CallOption) (*UpdateResourcePoolResponse, error)
	LookupResourcePoolID(context.Context, *LookupResourcePoolIDRequest, ...yarpc.CallOption) (*LookupResourcePoolIDResponse, error)
	QueryResourcePools(context.Context, *QueryResourcePoolsRequest, ...yarpc.CallOption) (*QueryResourcePoolsResponse, error)
}

// NewResourcePoolServiceYARPCClient builds a new YARPC client for the ResourcePoolService service.
func NewResourcePoolServiceYARPCClient(clientConfig transport.ClientConfig, options ...protobuf.ClientOption) ResourcePoolServiceYARPCClient {
	return &_ResourcePoolServiceYARPCCaller{protobuf.NewStreamClient(
		protobuf.ClientParams{
			ServiceName:  "peloton.api.v1alpha.respool.ResourcePoolService",
			ClientConfig: clientConfig,
			Options:      options,
		},
	)}
}

// ResourcePoolServiceYARPCServer is the YARPC server-side interface for the ResourcePoolService service.
type ResourcePoolServiceYARPCServer interface {
	CreateResourcePool(context.Context, *CreateResourcePoolRequest) (*CreateResourcePoolResponse, error)
	GetResourcePool(context.Context, *GetResourcePoolRequest) (*GetResourcePoolResponse, error)
	DeleteResourcePool(context.Context, *DeleteResourcePoolRequest) (*DeleteResourcePoolResponse, error)
	UpdateResourcePool(context.Context, *UpdateResourcePoolRequest) (*UpdateResourcePoolResponse, error)
	LookupResourcePoolID(context.Context, *LookupResourcePoolIDRequest) (*LookupResourcePoolIDResponse, error)
	QueryResourcePools(context.Context, *QueryResourcePoolsRequest) (*QueryResourcePoolsResponse, error)
}

// BuildResourcePoolServiceYARPCProcedures prepares an implementation of the ResourcePoolService service for YARPC registration.
func BuildResourcePoolServiceYARPCProcedures(server ResourcePoolServiceYARPCServer) []transport.Procedure {
	handler := &_ResourcePoolServiceYARPCHandler{server}
	return protobuf.BuildProcedures(
		protobuf.BuildProceduresParams{
			ServiceName: "peloton.api.v1alpha.respool.ResourcePoolService",
			UnaryHandlerParams: []protobuf.BuildProceduresUnaryHandlerParams{
				{
					MethodName: "CreateResourcePool",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.CreateResourcePool,
							NewRequest: newResourcePoolServiceServiceCreateResourcePoolYARPCRequest,
						},
					),
				},
				{
					MethodName: "GetResourcePool",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.GetResourcePool,
							NewRequest: newResourcePoolServiceServiceGetResourcePoolYARPCRequest,
						},
					),
				},
				{
					MethodName: "DeleteResourcePool",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.DeleteResourcePool,
							NewRequest: newResourcePoolServiceServiceDeleteResourcePoolYARPCRequest,
						},
					),
				},
				{
					MethodName: "UpdateResourcePool",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.UpdateResourcePool,
							NewRequest: newResourcePoolServiceServiceUpdateResourcePoolYARPCRequest,
						},
					),
				},
				{
					MethodName: "LookupResourcePoolID",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.LookupResourcePoolID,
							NewRequest: newResourcePoolServiceServiceLookupResourcePoolIDYARPCRequest,
						},
					),
				},
				{
					MethodName: "QueryResourcePools",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:     handler.QueryResourcePools,
							NewRequest: newResourcePoolServiceServiceQueryResourcePoolsYARPCRequest,
						},
					),
				},
			},
			OnewayHandlerParams: []protobuf.BuildProceduresOnewayHandlerParams{},
			StreamHandlerParams: []protobuf.BuildProceduresStreamHandlerParams{},
		},
	)
}

// FxResourcePoolServiceYARPCClientParams defines the input
// for NewFxResourcePoolServiceYARPCClient. It provides the
// paramaters to get a ResourcePoolServiceYARPCClient in an
// Fx application.
type FxResourcePoolServiceYARPCClientParams struct {
	fx.In

	Provider yarpc.ClientConfig
}

// FxResourcePoolServiceYARPCClientResult defines the output
// of NewFxResourcePoolServiceYARPCClient. It provides a
// ResourcePoolServiceYARPCClient to an Fx application.
type FxResourcePoolServiceYARPCClientResult struct {
	fx.Out

	Client ResourcePoolServiceYARPCClient

	// We are using an fx.Out struct here instead of just returning a client
	// so that we can add more values or add named versions of the client in
	// the future without breaking any existing code.
}

// NewFxResourcePoolServiceYARPCClient provides a ResourcePoolServiceYARPCClient
// to an Fx application using the given name for routing.
//
//  fx.Provide(
//    svc.NewFxResourcePoolServiceYARPCClient("service-name"),
//    ...
//  )
func NewFxResourcePoolServiceYARPCClient(name string, options ...protobuf.ClientOption) interface{} {
	return func(params FxResourcePoolServiceYARPCClientParams) FxResourcePoolServiceYARPCClientResult {
		return FxResourcePoolServiceYARPCClientResult{
			Client: NewResourcePoolServiceYARPCClient(params.Provider.ClientConfig(name), options...),
		}
	}
}

// FxResourcePoolServiceYARPCProceduresParams defines the input
// for NewFxResourcePoolServiceYARPCProcedures. It provides the
// paramaters to get ResourcePoolServiceYARPCServer procedures in an
// Fx application.
type FxResourcePoolServiceYARPCProceduresParams struct {
	fx.In

	Server ResourcePoolServiceYARPCServer
}

// FxResourcePoolServiceYARPCProceduresResult defines the output
// of NewFxResourcePoolServiceYARPCProcedures. It provides
// ResourcePoolServiceYARPCServer procedures to an Fx application.
//
// The procedures are provided to the "yarpcfx" value group.
// Dig 1.2 or newer must be used for this feature to work.
type FxResourcePoolServiceYARPCProceduresResult struct {
	fx.Out

	Procedures     []transport.Procedure `group:"yarpcfx"`
	ReflectionMeta reflection.ServerMeta `group:"yarpcfx"`
}

// NewFxResourcePoolServiceYARPCProcedures provides ResourcePoolServiceYARPCServer procedures to an Fx application.
// It expects a ResourcePoolServiceYARPCServer to be present in the container.
//
//  fx.Provide(
//    svc.NewFxResourcePoolServiceYARPCProcedures(),
//    ...
//  )
func NewFxResourcePoolServiceYARPCProcedures() interface{} {
	return func(params FxResourcePoolServiceYARPCProceduresParams) FxResourcePoolServiceYARPCProceduresResult {
		return FxResourcePoolServiceYARPCProceduresResult{
			Procedures: BuildResourcePoolServiceYARPCProcedures(params.Server),
			ReflectionMeta: reflection.ServerMeta{
				ServiceName:     "peloton.api.v1alpha.respool.ResourcePoolService",
				FileDescriptors: yarpcFileDescriptorClosureee7d00ee7eff1a2d,
			},
		}
	}
}

type _ResourcePoolServiceYARPCCaller struct {
	streamClient protobuf.StreamClient
}

func (c *_ResourcePoolServiceYARPCCaller) CreateResourcePool(ctx context.Context, request *CreateResourcePoolRequest, options ...yarpc.CallOption) (*CreateResourcePoolResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "CreateResourcePool", request, newResourcePoolServiceServiceCreateResourcePoolYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*CreateResourcePoolResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceCreateResourcePoolYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_ResourcePoolServiceYARPCCaller) GetResourcePool(ctx context.Context, request *GetResourcePoolRequest, options ...yarpc.CallOption) (*GetResourcePoolResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "GetResourcePool", request, newResourcePoolServiceServiceGetResourcePoolYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*GetResourcePoolResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceGetResourcePoolYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_ResourcePoolServiceYARPCCaller) DeleteResourcePool(ctx context.Context, request *DeleteResourcePoolRequest, options ...yarpc.CallOption) (*DeleteResourcePoolResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "DeleteResourcePool", request, newResourcePoolServiceServiceDeleteResourcePoolYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*DeleteResourcePoolResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceDeleteResourcePoolYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_ResourcePoolServiceYARPCCaller) UpdateResourcePool(ctx context.Context, request *UpdateResourcePoolRequest, options ...yarpc.CallOption) (*UpdateResourcePoolResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "UpdateResourcePool", request, newResourcePoolServiceServiceUpdateResourcePoolYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*UpdateResourcePoolResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceUpdateResourcePoolYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_ResourcePoolServiceYARPCCaller) LookupResourcePoolID(ctx context.Context, request *LookupResourcePoolIDRequest, options ...yarpc.CallOption) (*LookupResourcePoolIDResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "LookupResourcePoolID", request, newResourcePoolServiceServiceLookupResourcePoolIDYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*LookupResourcePoolIDResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceLookupResourcePoolIDYARPCResponse, responseMessage)
	}
	return response, err
}

func (c *_ResourcePoolServiceYARPCCaller) QueryResourcePools(ctx context.Context, request *QueryResourcePoolsRequest, options ...yarpc.CallOption) (*QueryResourcePoolsResponse, error) {
	responseMessage, err := c.streamClient.Call(ctx, "QueryResourcePools", request, newResourcePoolServiceServiceQueryResourcePoolsYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*QueryResourcePoolsResponse)
	if !ok {
		return nil, protobuf.CastError(emptyResourcePoolServiceServiceQueryResourcePoolsYARPCResponse, responseMessage)
	}
	return response, err
}

type _ResourcePoolServiceYARPCHandler struct {
	server ResourcePoolServiceYARPCServer
}

func (h *_ResourcePoolServiceYARPCHandler) CreateResourcePool(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *CreateResourcePoolRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*CreateResourcePoolRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceCreateResourcePoolYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.CreateResourcePool(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_ResourcePoolServiceYARPCHandler) GetResourcePool(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *GetResourcePoolRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*GetResourcePoolRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceGetResourcePoolYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.GetResourcePool(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_ResourcePoolServiceYARPCHandler) DeleteResourcePool(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *DeleteResourcePoolRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*DeleteResourcePoolRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceDeleteResourcePoolYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.DeleteResourcePool(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_ResourcePoolServiceYARPCHandler) UpdateResourcePool(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *UpdateResourcePoolRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*UpdateResourcePoolRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceUpdateResourcePoolYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.UpdateResourcePool(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_ResourcePoolServiceYARPCHandler) LookupResourcePoolID(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *LookupResourcePoolIDRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*LookupResourcePoolIDRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceLookupResourcePoolIDYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.LookupResourcePoolID(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func (h *_ResourcePoolServiceYARPCHandler) QueryResourcePools(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *QueryResourcePoolsRequest
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*QueryResourcePoolsRequest)
		if !ok {
			return nil, protobuf.CastError(emptyResourcePoolServiceServiceQueryResourcePoolsYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.QueryResourcePools(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func newResourcePoolServiceServiceCreateResourcePoolYARPCRequest() proto.Message {
	return &CreateResourcePoolRequest{}
}

func newResourcePoolServiceServiceCreateResourcePoolYARPCResponse() proto.Message {
	return &CreateResourcePoolResponse{}
}

func newResourcePoolServiceServiceGetResourcePoolYARPCRequest() proto.Message {
	return &GetResourcePoolRequest{}
}

func newResourcePoolServiceServiceGetResourcePoolYARPCResponse() proto.Message {
	return &GetResourcePoolResponse{}
}

func newResourcePoolServiceServiceDeleteResourcePoolYARPCRequest() proto.Message {
	return &DeleteResourcePoolRequest{}
}

func newResourcePoolServiceServiceDeleteResourcePoolYARPCResponse() proto.Message {
	return &DeleteResourcePoolResponse{}
}

func newResourcePoolServiceServiceUpdateResourcePoolYARPCRequest() proto.Message {
	return &UpdateResourcePoolRequest{}
}

func newResourcePoolServiceServiceUpdateResourcePoolYARPCResponse() proto.Message {
	return &UpdateResourcePoolResponse{}
}

func newResourcePoolServiceServiceLookupResourcePoolIDYARPCRequest() proto.Message {
	return &LookupResourcePoolIDRequest{}
}

func newResourcePoolServiceServiceLookupResourcePoolIDYARPCResponse() proto.Message {
	return &LookupResourcePoolIDResponse{}
}

func newResourcePoolServiceServiceQueryResourcePoolsYARPCRequest() proto.Message {
	return &QueryResourcePoolsRequest{}
}

func newResourcePoolServiceServiceQueryResourcePoolsYARPCResponse() proto.Message {
	return &QueryResourcePoolsResponse{}
}

var (
	emptyResourcePoolServiceServiceCreateResourcePoolYARPCRequest    = &CreateResourcePoolRequest{}
	emptyResourcePoolServiceServiceCreateResourcePoolYARPCResponse   = &CreateResourcePoolResponse{}
	emptyResourcePoolServiceServiceGetResourcePoolYARPCRequest       = &GetResourcePoolRequest{}
	emptyResourcePoolServiceServiceGetResourcePoolYARPCResponse      = &GetResourcePoolResponse{}
	emptyResourcePoolServiceServiceDeleteResourcePoolYARPCRequest    = &DeleteResourcePoolRequest{}
	emptyResourcePoolServiceServiceDeleteResourcePoolYARPCResponse   = &DeleteResourcePoolResponse{}
	emptyResourcePoolServiceServiceUpdateResourcePoolYARPCRequest    = &UpdateResourcePoolRequest{}
	emptyResourcePoolServiceServiceUpdateResourcePoolYARPCResponse   = &UpdateResourcePoolResponse{}
	emptyResourcePoolServiceServiceLookupResourcePoolIDYARPCRequest  = &LookupResourcePoolIDRequest{}
	emptyResourcePoolServiceServiceLookupResourcePoolIDYARPCResponse = &LookupResourcePoolIDResponse{}
	emptyResourcePoolServiceServiceQueryResourcePoolsYARPCRequest    = &QueryResourcePoolsRequest{}
	emptyResourcePoolServiceServiceQueryResourcePoolsYARPCResponse   = &QueryResourcePoolsResponse{}
)

var yarpcFileDescriptorClosureee7d00ee7eff1a2d = [][]byte{
	// peloton/api/v1alpha/respool/svc/respool_svc.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xd4, 0x56, 0x4d, 0x6f, 0xd4, 0x30,
		0x10, 0x55, 0xa0, 0x2a, 0x65, 0x10, 0x20, 0x5c, 0x04, 0x5d, 0xef, 0x4a, 0x85, 0x9c, 0x40, 0x88,
		0xac, 0xda, 0x22, 0x3e, 0x8e, 0xfd, 0x90, 0xaa, 0x45, 0x1c, 0x4a, 0x80, 0x0b, 0x97, 0x25, 0x38,
		0x43, 0x13, 0x88, 0xd6, 0x26, 0x4e, 0x22, 0x21, 0x71, 0xe5, 0xc4, 0x95, 0x5f, 0xc1, 0x2f, 0xe0,
		0xca, 0x3f, 0x43, 0xf1, 0x4e, 0xaa, 0x6d, 0xe3, 0x24, 0xa4, 0x62, 0x0f, 0x9c, 0x76, 0x65, 0xfb,
		0xbd, 0x79, 0xf3, 0x32, 0xcf, 0x32, 0x6c, 0x29, 0x4c, 0x64, 0x26, 0x67, 0xe3, 0x40, 0xc5, 0xe3,
		0x62, 0x2b, 0x48, 0x54, 0x14, 0x8c, 0x53, 0xd4, 0x4a, 0xca, 0x64, 0xac, 0x0b, 0x51, 0xfd, 0x9f,
		0xea, 0x42, 0x78, 0x2a, 0x95, 0x99, 0x64, 0x43, 0x82, 0x78, 0x81, 0x8a, 0x3d, 0x82, 0x78, 0x74,
		0x8c, 0xdf, 0xb5, 0xf1, 0x55, 0x00, 0x83, 0xe7, 0xf7, 0xdb, 0x4a, 0xd2, 0xef, 0xfc, 0xa8, 0xfb,
		0xd3, 0x81, 0xc1, 0x7e, 0x8a, 0x41, 0x86, 0x3e, 0x6a, 0x99, 0xa7, 0x02, 0x8f, 0xa4, 0x4c, 0x7c,
		0xfc, 0x9c, 0xa3, 0xce, 0xd8, 0x73, 0x80, 0x4a, 0x5d, 0x1c, 0x6e, 0x38, 0x77, 0x9c, 0x7b, 0x57,
		0xb6, 0x1f, 0x78, 0x36, 0x75, 0xd5, 0xda, 0x22, 0xcb, 0xe4, 0xc0, 0xbf, 0x4c, 0xf0, 0x49, 0xc8,
		0x76, 0x61, 0x45, 0x2b, 0x14, 0x1b, 0x17, 0x0c, 0xcb, 0x43, 0xaf, 0xa5, 0xc7, 0x53, 0x2c, 0xaf,
		0x14, 0x0a, 0xdf, 0x40, 0xdd, 0x08, 0xb8, 0x4d, 0xab, 0x56, 0x72, 0xa6, 0xf1, 0x5f, 0x8a, 0x75,
		0x7f, 0x38, 0x70, 0xeb, 0x10, 0xb3, 0x65, 0x7b, 0xe2, 0xc1, 0x7a, 0x3c, 0x13, 0x49, 0x1e, 0xe2,
		0x54, 0x44, 0x71, 0x12, 0x4e, 0xcb, 0x75, 0x6d, 0x2c, 0x5a, 0xf3, 0x6f, 0xd0, 0xd6, 0x7e, 0xb9,
		0x53, 0x62, 0xb5, 0xfb, 0xcb, 0x81, 0xdb, 0x35, 0x59, 0xd4, 0xfe, 0x21, 0x5c, 0x22, 0x62, 0x12,
		0xf5, 0xf7, 0x16, 0x4f, 0x66, 0x1f, 0xa4, 0x5f, 0xa1, 0xd9, 0x6b, 0xb8, 0x36, 0x17, 0x43, 0x0b,
		0xa5, 0x9e, 0x8b, 0xfd, 0xf9, 0xae, 0x1a, 0x12, 0x9f, 0x38, 0xdc, 0x63, 0x18, 0x1c, 0x60, 0x82,
		0x4b, 0x9f, 0x33, 0x77, 0x04, 0xdc, 0x56, 0x68, 0xee, 0x92, 0x99, 0xf7, 0x37, 0x2a, 0xfc, 0x3f,
		0xe6, 0x7d, 0x04, 0xdc, 0xa6, 0x95, 0x5a, 0x79, 0x07, 0xc3, 0x17, 0x52, 0x7e, 0xca, 0xd5, 0x19,
		0x0d, 0xd4, 0xcb, 0x2e, 0xac, 0xa8, 0x20, 0x8b, 0x7a, 0x0f, 0xc3, 0x51, 0x90, 0x45, 0xbe, 0x81,
		0xba, 0x1f, 0x61, 0x64, 0xaf, 0xb0, 0x84, 0xc4, 0x0d, 0x61, 0xf0, 0x32, 0xc7, 0xf4, 0xcb, 0xe2,
		0x09, 0x4d, 0xbd, 0xb8, 0xc7, 0xc0, 0x6d, 0x9b, 0x24, 0x63, 0x02, 0x6b, 0x27, 0xa3, 0xea, 0x9c,
		0x67, 0x54, 0x4f, 0xe0, 0xdb, 0xbf, 0x57, 0x61, 0xfd, 0xd4, 0xc7, 0xc0, 0xb4, 0x88, 0x05, 0xb2,
		0x6f, 0x0e, 0xb0, 0xfa, 0xd5, 0xc3, 0x1e, 0xb7, 0xd6, 0x69, 0xbc, 0x57, 0xf9, 0x93, 0xde, 0x38,
		0x6a, 0xf5, 0x2b, 0x5c, 0x3f, 0x93, 0x7f, 0xb6, 0xd3, 0xca, 0x65, 0xbf, 0xc4, 0xf8, 0xa3, 0x7e,
		0x20, 0xaa, 0x5e, 0xba, 0x50, 0xcf, 0x56, 0x87, 0x0b, 0x8d, 0xa9, 0xef, 0x70, 0xa1, 0x39, 0xc4,
		0x46, 0x47, 0x3d, 0x18, 0x1d, 0x3a, 0x1a, 0x53, 0xdf, 0xa1, 0xa3, 0x39, 0x81, 0xec, 0xbb, 0x03,
		0x37, 0x6d, 0x01, 0x61, 0x4f, 0x5b, 0x19, 0x5b, 0x52, 0xcb, 0x9f, 0x9d, 0x03, 0xb9, 0xe0, 0x4a,
		0x3d, 0x25, 0x1d, 0xae, 0x34, 0x66, 0xae, 0xc3, 0x95, 0xe6, 0x38, 0xee, 0xed, 0xc1, 0x66, 0x1b,
		0x52, 0x17, 0xe2, 0xed, 0x66, 0xc7, 0x9b, 0xe8, 0xfd, 0xaa, 0x79, 0x9d, 0xec, 0xfc, 0x09, 0x00,
		0x00, 0xff, 0xff, 0x76, 0x96, 0x6d, 0x12, 0x3d, 0x09, 0x00, 0x00,
	},
	// peloton/api/v1alpha/peloton.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x93, 0xcd, 0x6e, 0xd3, 0x40,
		0x10, 0xc7, 0x95, 0x26, 0x6e, 0x93, 0x29, 0x20, 0xb4, 0xe2, 0x10, 0x25, 0x8a, 0x5a, 0x8c, 0x8a,
		0x40, 0x42, 0x6b, 0x01, 0x37, 0x2e, 0x88, 0xa8, 0x48, 0x14, 0x21, 0x1a, 0x99, 0x2a, 0x07, 0x2e,
		0xd5, 0x38, 0x9e, 0xa4, 0x2b, 0xec, 0xec, 0x62, 0xaf, 0xad, 0xfa, 0xc8, 0x8b, 0xf1, 0x6c, 0xc8,
		0xfb, 0x21, 0xe8, 0xc1, 0xa6, 0xb7, 0xf9, 0xf8, 0xcd, 0x7f, 0x77, 0x76, 0x66, 0xe1, 0xa9, 0xa2,
		0x4c, 0x6a, 0xb9, 0x8f, 0x50, 0x89, 0xa8, 0x7e, 0x8d, 0x99, 0xba, 0xc1, 0xc8, 0xc5, 0xb8, 0x2a,
		0xa4, 0x96, 0x6c, 0xee, 0x5d, 0x54, 0x82, 0x3b, 0x84, 0xbb, 0xd8, 0xec, 0x64, 0x27, 0xe5, 0x2e,
		0xa3, 0xc8, 0xa0, 0x49, 0xb5, 0x8d, 0xb4, 0xc8, 0xa9, 0xd4, 0x98, 0x2b, 0x5b, 0x1d, 0x2e, 0x20,
		0xf8, 0x2c, 0x93, 0x8b, 0x73, 0xf6, 0x04, 0x82, 0x1a, 0xb3, 0x8a, 0xa6, 0x83, 0xd3, 0xc1, 0x8b,
		0x49, 0x6c, 0x9d, 0xf0, 0x04, 0x8e, 0x56, 0x32, 0xfd, 0x8a, 0x39, 0x75, 0x00, 0x0b, 0x08, 0x56,
		0x32, 0xed, 0xac, 0x3f, 0x85, 0xf1, 0x5a, 0x66, 0x55, 0x4e, 0x9d, 0xc4, 0x73, 0x78, 0x14, 0x53,
		0x29, 0xab, 0x62, 0x43, 0x2b, 0x29, 0xb3, 0x3e, 0xa5, 0x6f, 0xb4, 0x29, 0x48, 0x77, 0x12, 0xcf,
		0xe0, 0xf8, 0x93, 0x2c, 0xf5, 0xe5, 0x76, 0x4b, 0x45, 0x27, 0x74, 0x06, 0x0f, 0x3f, 0xee, 0xb5,
		0xd0, 0xcd, 0x9a, 0x8a, 0x52, 0xc8, 0x7d, 0xe7, 0x69, 0x70, 0xa9, 0xf0, 0x67, 0x45, 0xe7, 0xa8,
		0x91, 0x31, 0x18, 0xa5, 0xa8, 0xd1, 0x21, 0xc6, 0x0e, 0x7f, 0x0d, 0x60, 0x1c, 0x53, 0x2d, 0x8c,
		0xc8, 0x14, 0x8e, 0x6a, 0xab, 0x67, 0x98, 0x51, 0xec, 0x5d, 0xb6, 0x00, 0xd8, 0x14, 0x84, 0x9a,
		0xd2, 0x6b, 0xd4, 0xd3, 0x03, 0x93, 0x9c, 0xb8, 0xc8, 0x07, 0xdd, 0xa6, 0x2b, 0x95, 0xfa, 0xf4,
		0xd0, 0xa6, 0x5d, 0xe4, 0x6e, 0x3a, 0x69, 0xa6, 0x23, 0x73, 0xbc, 0x4f, 0x2f, 0x9b, 0x30, 0x82,
		0xe0, 0x0b, 0x26, 0x94, 0xb1, 0xc7, 0x30, 0xfc, 0x41, 0x8d, 0xbb, 0x5f, 0x6b, 0xfe, 0x6d, 0xeb,
		0xe0, 0xdf, 0xb6, 0x76, 0x30, 0xb9, 0x12, 0x39, 0xc5, 0xb8, 0xdf, 0x11, 0x7b, 0x05, 0xc3, 0x5c,
		0xd8, 0x0b, 0x1f, 0xbf, 0x99, 0x71, 0xbb, 0x29, 0xdc, 0x6f, 0x0a, 0xbf, 0xf2, 0x9b, 0x12, 0xb7,
		0x98, 0xa1, 0xf1, 0xd6, 0xc8, 0xfd, 0x8f, 0xc6, 0xdb, 0xf0, 0xf7, 0x00, 0x0e, 0xed, 0xb8, 0xd8,
		0x12, 0x26, 0xa5, 0xb1, 0xae, 0x45, 0xea, 0x0e, 0x3b, 0xe3, 0x3d, 0x3b, 0xcb, 0xfd, 0x98, 0xe3,
		0xb1, 0xad, 0xbb, 0x48, 0xdb, 0x01, 0x28, 0xd4, 0x37, 0xae, 0x19, 0x63, 0xb3, 0xf7, 0xbe, 0xc3,
		0xa1, 0xd1, 0x7c, 0x79, 0x0f, 0x4d, 0xbe, 0x6e, 0x0b, 0xdc, 0x63, 0xcc, 0xe6, 0x10, 0x18, 0xff,
		0xce, 0x78, 0x1f, 0xd8, 0xf1, 0x2e, 0xdf, 0x41, 0xdf, 0xbf, 0xfa, 0x3e, 0xef, 0xf9, 0x97, 0xc9,
		0xa1, 0x79, 0x95, 0xb7, 0x7f, 0x02, 0x00, 0x00, 0xff, 0xff, 0x3c, 0xac, 0xb8, 0xb5, 0xbd, 0x03,
		0x00, 0x00,
	},
	// google/protobuf/timestamp.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4f, 0xcf, 0xcf, 0x4f,
		0xcf, 0x49, 0xd5, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x2f, 0xc9, 0xcc, 0x4d,
		0x2d, 0x2e, 0x49, 0xcc, 0x2d, 0xd0, 0x03, 0x0b, 0x09, 0xf1, 0x43, 0x14, 0xe8, 0xc1, 0x14, 0x28,
		0x59, 0x73, 0x71, 0x86, 0xc0, 0xd4, 0x08, 0x49, 0x70, 0xb1, 0x17, 0xa7, 0x26, 0xe7, 0xe7, 0xa5,
		0x14, 0x4b, 0x30, 0x2a, 0x30, 0x6a, 0x30, 0x07, 0xc1, 0xb8, 0x42, 0x22, 0x5c, 0xac, 0x79, 0x89,
		0x79, 0xf9, 0xc5, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0xac, 0x41, 0x10, 0x8e, 0x53, 0x1d, 0x97, 0x70,
		0x72, 0x7e, 0xae, 0x1e, 0x9a, 0x99, 0x4e, 0x7c, 0x70, 0x13, 0x03, 0x40, 0x42, 0x01, 0x8c, 0x51,
		0xda, 0xe9, 0x99, 0x25, 0x19, 0xa5, 0x49, 0x7a, 0xc9, 0xf9, 0xb9, 0xfa, 0xe9, 0xf9, 0x39, 0x89,
		0x79, 0xe9, 0x08, 0x27, 0x16, 0x94, 0x54, 0x16, 0xa4, 0x16, 0x23, 0x5c, 0xfa, 0x83, 0x91, 0x71,
		0x11, 0x13, 0xb3, 0x7b, 0x80, 0xd3, 0x2a, 0x26, 0x39, 0x77, 0x88, 0xc9, 0x01, 0x50, 0xb5, 0x7a,
		0xe1, 0xa9, 0x39, 0x39, 0xde, 0x79, 0xf9, 0xe5, 0x79, 0x21, 0x20, 0x3d, 0x49, 0x6c, 0x60, 0x43,
		0x8c, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0xbc, 0x77, 0x4a, 0x07, 0xf7, 0x00, 0x00, 0x00,
	},
	// peloton/api/v1alpha/respool/respool.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x55, 0xdd, 0x72, 0x12, 0x4b,
		0x10, 0x3e, 0x1b, 0x7e, 0x4e, 0x68, 0xce, 0x31, 0xd4, 0x94, 0x55, 0xae, 0x46, 0x13, 0xc4, 0xb2,
		0x24, 0x31, 0x92, 0x12, 0xef, 0xbc, 0x92, 0x10, 0x42, 0xd6, 0xa2, 0x02, 0x35, 0x6c, 0x62, 0xe1,
		0xcd, 0xd6, 0xb8, 0x3b, 0xc2, 0x9a, 0x61, 0x67, 0x6a, 0x76, 0xc1, 0xe4, 0xb5, 0xbc, 0xf0, 0x0d,
		0x7c, 0x2f, 0x6b, 0x66, 0x17, 0xd8, 0x10, 0x8a, 0x18, 0xaf, 0xd8, 0xf9, 0xba, 0xbf, 0x9e, 0xee,
		0xaf, 0xbb, 0x19, 0xd8, 0x13, 0x94, 0xf1, 0x88, 0x07, 0x87, 0x44, 0xf8, 0x87, 0xd3, 0xb7, 0x84,
		0x89, 0x11, 0x39, 0x94, 0x34, 0x14, 0x9c, 0xb3, 0xd9, 0x6f, 0x4d, 0x48, 0x1e, 0x71, 0xb4, 0x9d,
		0xb8, 0xd6, 0x88, 0xf0, 0x6b, 0x89, 0x6b, 0x2d, 0x71, 0x79, 0xf2, 0x7c, 0x55, 0x9c, 0x19, 0x41,
		0xf3, 0x2b, 0x55, 0x28, 0x61, 0x1a, 0xf2, 0x89, 0x74, 0x69, 0x8f, 0x73, 0xd6, 0x23, 0xd1, 0x08,
		0x3d, 0x84, 0xdc, 0x94, 0xb0, 0x09, 0x35, 0x8d, 0xb2, 0x51, 0x2d, 0xe0, 0xf8, 0x50, 0xf9, 0x61,
		0xc0, 0x7f, 0x33, 0xd7, 0xbe, 0xa0, 0x2e, 0x42, 0x90, 0xbd, 0xf4, 0x03, 0x2f, 0xf1, 0xd2, 0xdf,
		0xa8, 0x0c, 0x45, 0x49, 0x43, 0x2a, 0xa7, 0x24, 0xf2, 0x79, 0x60, 0x6e, 0x94, 0x8d, 0xaa, 0x81,
		0xd3, 0x90, 0x0a, 0xce, 0xfc, 0xb1, 0x1f, 0x99, 0x19, 0x6d, 0x8b, 0x0f, 0x0a, 0x0d, 0x47, 0x44,
		0x52, 0x33, 0x1b, 0xa3, 0xfa, 0x80, 0x3e, 0x40, 0x36, 0xba, 0x16, 0xd4, 0xcc, 0x95, 0x8d, 0xea,
		0x83, 0xfa, 0x41, 0x6d, 0x4d, 0xad, 0x35, 0xbc, 0xb8, 0xc3, 0xbe, 0x16, 0x14, 0x6b, 0x66, 0xe5,
		0x57, 0xf6, 0x66, 0x7d, 0x3a, 0xf1, 0x06, 0x6c, 0x4a, 0x3a, 0xf5, 0x43, 0x95, 0xa1, 0x4a, 0xbe,
		0x58, 0x7f, 0xb9, 0x32, 0xf4, 0x0c, 0xc3, 0x89, 0x33, 0x9e, 0xd3, 0x54, 0xed, 0x01, 0x19, 0x53,
		0x5d, 0x60, 0x01, 0xeb, 0x6f, 0xb4, 0x0b, 0x45, 0xfe, 0x3d, 0xf0, 0x83, 0xa1, 0x13, 0x51, 0x32,
		0xd6, 0xf5, 0x15, 0x30, 0xc4, 0x90, 0x4d, 0xc9, 0x58, 0x39, 0x30, 0x8f, 0x08, 0x67, 0x28, 0xf9,
		0x44, 0x84, 0x66, 0xb6, 0x9c, 0x51, 0x0e, 0x0a, 0x6a, 0x6b, 0x44, 0xa9, 0xe7, 0xd1, 0xd0, 0x95,
		0xbe, 0xd0, 0xea, 0xe5, 0x74, 0x84, 0x34, 0x84, 0xda, 0x50, 0x90, 0x49, 0x39, 0xa1, 0x99, 0x2f,
		0x67, 0xaa, 0xc5, 0xfa, 0xde, 0x5d, 0xb2, 0xcc, 0x3b, 0x86, 0x17, 0x5c, 0xd4, 0x84, 0xbc, 0x20,
		0x92, 0x06, 0x91, 0xf9, 0xaf, 0x56, 0xe0, 0xf5, 0x1d, 0x0a, 0x2c, 0x24, 0xb4, 0x8e, 0x71, 0x42,
		0x45, 0x2d, 0xc8, 0x0b, 0xce, 0x7c, 0xf7, 0xda, 0xdc, 0xd4, 0x1d, 0x7a, 0xb3, 0x36, 0x95, 0xbe,
		0x3b, 0xa2, 0xde, 0x84, 0xf9, 0xc1, 0xb0, 0xa7, 0x49, 0x38, 0x21, 0xa3, 0x4f, 0x50, 0x72, 0x79,
		0x10, 0x49, 0xce, 0x18, 0x95, 0x4e, 0x3c, 0x1d, 0x05, 0x9d, 0xd5, 0xfa, 0x96, 0x37, 0xe7, 0xa4,
		0x8e, 0xe2, 0xe0, 0x2d, 0xf7, 0x26, 0x80, 0x4e, 0xa1, 0x18, 0x32, 0xe2, 0x5e, 0x26, 0x31, 0x41,
		0xc7, 0x7c, 0xb5, 0x3e, 0x49, 0xe5, 0x1f, 0x87, 0x83, 0x70, 0xfe, 0x5d, 0xa9, 0xc3, 0xd6, 0xd2,
		0x6d, 0xaa, 0x9b, 0x63, 0x72, 0xe5, 0x08, 0x2a, 0x5d, 0x25, 0xa3, 0xa1, 0x07, 0x17, 0xc6, 0xe4,
		0xaa, 0x17, 0x23, 0x95, 0x03, 0x80, 0x45, 0x34, 0xb4, 0x03, 0x29, 0xdb, 0x0a, 0xef, 0x01, 0xfc,
		0x3f, 0x53, 0xf9, 0x3c, 0x24, 0x43, 0xba, 0x72, 0xbd, 0x76, 0x00, 0x08, 0x63, 0xdc, 0x4d, 0x6f,
		0x57, 0x0a, 0xd1, 0x6b, 0xa4, 0xae, 0x9c, 0x2d, 0x97, 0x3e, 0x54, 0x7e, 0x66, 0x6e, 0x2e, 0x81,
		0x15, 0x7c, 0xe5, 0xe8, 0x23, 0x40, 0x52, 0xb3, 0xe3, 0x7b, 0xc9, 0x1a, 0xdc, 0x6b, 0x08, 0x0a,
		0x09, 0xdd, 0xf2, 0x50, 0x03, 0xb2, 0xa1, 0xa0, 0xae, 0x4e, 0xa8, 0x78, 0xc7, 0x14, 0x2c, 0x6f,
		0x23, 0xd6, 0xd4, 0xd4, 0x3c, 0x66, 0xfe, 0x7e, 0x1e, 0xdb, 0xb0, 0xe9, 0x8e, 0x7c, 0xe6, 0x49,
		0x1a, 0xe8, 0xed, 0xba, 0x67, 0x98, 0x39, 0x19, 0x1d, 0x41, 0x7e, 0xa2, 0x9a, 0x10, 0x9a, 0x39,
		0x1d, 0x66, 0xff, 0x8f, 0x4a, 0xd2, 0x7d, 0xc3, 0x09, 0x53, 0x89, 0x22, 0x48, 0x34, 0x32, 0xf3,
		0xf7, 0x14, 0x45, 0xfd, 0x05, 0x63, 0x4d, 0xdd, 0xff, 0x06, 0x5b, 0x4b, 0x7f, 0x6b, 0xe8, 0x29,
		0x98, 0xb8, 0xd5, 0x6f, 0xe1, 0x8b, 0x86, 0x6d, 0x75, 0xcf, 0x1c, 0x7b, 0xd0, 0x6b, 0x39, 0xd6,
		0xd9, 0x45, 0xa3, 0x63, 0x1d, 0x97, 0xfe, 0x59, 0x69, 0x6d, 0x75, 0x1a, 0x7d, 0xdb, 0x6a, 0x96,
		0x0c, 0xb4, 0x0d, 0x8f, 0x6e, 0x59, 0xfb, 0x76, 0x43, 0x19, 0x37, 0xf6, 0x2f, 0xa0, 0xb4, 0xbc,
		0xa0, 0xe8, 0x19, 0x3c, 0xee, 0x37, 0x4f, 0x5b, 0xc7, 0xe7, 0x1d, 0xeb, 0xac, 0xed, 0xf4, 0xba,
		0x1d, 0xab, 0x39, 0x48, 0xdd, 0xf6, 0x02, 0x76, 0x6f, 0x9b, 0x7b, 0xd8, 0xea, 0x62, 0xcb, 0x1e,
		0x38, 0x27, 0xd6, 0x49, 0xb7, 0x64, 0x1c, 0xbd, 0x87, 0x75, 0x4f, 0xd4, 0xe7, 0xed, 0x35, 0x4f,
		0xdd, 0x97, 0xbc, 0x7e, 0xa3, 0xde, 0xfd, 0x0e, 0x00, 0x00, 0xff, 0xff, 0x8e, 0xb1, 0xac, 0x7d,
		0x10, 0x07, 0x00, 0x00,
	},
}

func init() {
	yarpc.RegisterClientBuilder(
		func(clientConfig transport.ClientConfig, structField reflect.StructField) ResourcePoolServiceYARPCClient {
			return NewResourcePoolServiceYARPCClient(clientConfig, protobuf.ClientBuilderOptions(clientConfig, structField)...)
		},
	)
}
