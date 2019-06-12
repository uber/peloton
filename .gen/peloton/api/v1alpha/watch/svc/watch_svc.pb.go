// Code generated by protoc-gen-go. DO NOT EDIT.
// source: peloton/api/v1alpha/watch/svc/watch_svc.proto

package svc

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	stateless "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	watch "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// WatchRequest is request for method WatchService.Watch. It
// specifies the objects that should be monitored for changes.
type WatchRequest struct {
	// The revision from which to start getting changes. If unspecified,
	// the server will return changes after the current revision. The server
	// may choose to maintain only a limited number of historical revisions;
	// a start revision older than the oldest revision available at the
	// server will result in an error and the watch stream will be closed.
	// Note: Initial implementations will not support historical revisions,
	// so if the client sets a value for this field, it will receive an
	// OUT_OF_RANGE error immediately.
	StartRevision uint64 `protobuf:"varint,1,opt,name=start_revision,json=startRevision,proto3" json:"start_revision,omitempty"`
	// Criteria to select the stateless jobs to watch. If unset,
	// no jobs will be watched.
	StatelessJobFilter *watch.StatelessJobFilter `protobuf:"bytes,2,opt,name=stateless_job_filter,json=statelessJobFilter,proto3" json:"stateless_job_filter,omitempty"`
	// Criteria to select the pods to watch. If unset,
	// no pods will be watched.
	PodFilter            *watch.PodFilter `protobuf:"bytes,3,opt,name=pod_filter,json=podFilter,proto3" json:"pod_filter,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *WatchRequest) Reset()         { *m = WatchRequest{} }
func (m *WatchRequest) String() string { return proto.CompactTextString(m) }
func (*WatchRequest) ProtoMessage()    {}
func (*WatchRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_9320203f9aee53ee, []int{0}
}

func (m *WatchRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WatchRequest.Unmarshal(m, b)
}
func (m *WatchRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WatchRequest.Marshal(b, m, deterministic)
}
func (m *WatchRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WatchRequest.Merge(m, src)
}
func (m *WatchRequest) XXX_Size() int {
	return xxx_messageInfo_WatchRequest.Size(m)
}
func (m *WatchRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_WatchRequest.DiscardUnknown(m)
}

var xxx_messageInfo_WatchRequest proto.InternalMessageInfo

func (m *WatchRequest) GetStartRevision() uint64 {
	if m != nil {
		return m.StartRevision
	}
	return 0
}

func (m *WatchRequest) GetStatelessJobFilter() *watch.StatelessJobFilter {
	if m != nil {
		return m.StatelessJobFilter
	}
	return nil
}

func (m *WatchRequest) GetPodFilter() *watch.PodFilter {
	if m != nil {
		return m.PodFilter
	}
	return nil
}

// WatchResponse is response method for WatchService.Watch. It
// contains the objects that have changed.
// Return errors:
//    OUT_OF_RANGE: Requested start-revision is too old
//    INVALID_ARGUMENT: Requested start-revision is newer than server revision
//    RESOURCE_EXHAUSTED: Number of concurrent watches exceeded
//    CANCELLED: Watch cancelled by user
//    ABORTED: Client not reading events fast enough, causing internal queue
//             to overflow
type WatchResponse struct {
	// Unique identifier for the watch session
	WatchId string `protobuf:"bytes,1,opt,name=watch_id,json=watchId,proto3" json:"watch_id,omitempty"`
	// Server revision when the response results were created
	Revision uint64 `protobuf:"varint,2,opt,name=revision,proto3" json:"revision,omitempty"`
	// Stateless jobs that have changed.
	StatelessJobs []*stateless.JobSummary `protobuf:"bytes,3,rep,name=stateless_jobs,json=statelessJobs,proto3" json:"stateless_jobs,omitempty"`
	// Stateless job IDs that were not found.
	StatelessJobsNotFound []*peloton.JobID `protobuf:"bytes,4,rep,name=stateless_jobs_not_found,json=statelessJobsNotFound,proto3" json:"stateless_jobs_not_found,omitempty"`
	// Pods that have changed.
	Pods []*pod.PodSummary `protobuf:"bytes,5,rep,name=pods,proto3" json:"pods,omitempty"`
	// Names of pods that were not found.
	PodsNotFound         []*peloton.PodName `protobuf:"bytes,6,rep,name=pods_not_found,json=podsNotFound,proto3" json:"pods_not_found,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *WatchResponse) Reset()         { *m = WatchResponse{} }
func (m *WatchResponse) String() string { return proto.CompactTextString(m) }
func (*WatchResponse) ProtoMessage()    {}
func (*WatchResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_9320203f9aee53ee, []int{1}
}

func (m *WatchResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WatchResponse.Unmarshal(m, b)
}
func (m *WatchResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WatchResponse.Marshal(b, m, deterministic)
}
func (m *WatchResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WatchResponse.Merge(m, src)
}
func (m *WatchResponse) XXX_Size() int {
	return xxx_messageInfo_WatchResponse.Size(m)
}
func (m *WatchResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_WatchResponse.DiscardUnknown(m)
}

var xxx_messageInfo_WatchResponse proto.InternalMessageInfo

func (m *WatchResponse) GetWatchId() string {
	if m != nil {
		return m.WatchId
	}
	return ""
}

func (m *WatchResponse) GetRevision() uint64 {
	if m != nil {
		return m.Revision
	}
	return 0
}

func (m *WatchResponse) GetStatelessJobs() []*stateless.JobSummary {
	if m != nil {
		return m.StatelessJobs
	}
	return nil
}

func (m *WatchResponse) GetStatelessJobsNotFound() []*peloton.JobID {
	if m != nil {
		return m.StatelessJobsNotFound
	}
	return nil
}

func (m *WatchResponse) GetPods() []*pod.PodSummary {
	if m != nil {
		return m.Pods
	}
	return nil
}

func (m *WatchResponse) GetPodsNotFound() []*peloton.PodName {
	if m != nil {
		return m.PodsNotFound
	}
	return nil
}

// CancelRequest is request for method WatchService.Cancel
type CancelRequest struct {
	// ID of the watch session to cancel.
	WatchId              string   `protobuf:"bytes,1,opt,name=watch_id,json=watchId,proto3" json:"watch_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CancelRequest) Reset()         { *m = CancelRequest{} }
func (m *CancelRequest) String() string { return proto.CompactTextString(m) }
func (*CancelRequest) ProtoMessage()    {}
func (*CancelRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_9320203f9aee53ee, []int{2}
}

func (m *CancelRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CancelRequest.Unmarshal(m, b)
}
func (m *CancelRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CancelRequest.Marshal(b, m, deterministic)
}
func (m *CancelRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CancelRequest.Merge(m, src)
}
func (m *CancelRequest) XXX_Size() int {
	return xxx_messageInfo_CancelRequest.Size(m)
}
func (m *CancelRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_CancelRequest.DiscardUnknown(m)
}

var xxx_messageInfo_CancelRequest proto.InternalMessageInfo

func (m *CancelRequest) GetWatchId() string {
	if m != nil {
		return m.WatchId
	}
	return ""
}

// CancelRequest is response for method WatchService.Cancel
// Return errors:
//    NOT_FOUND: Watch ID not found
type CancelResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CancelResponse) Reset()         { *m = CancelResponse{} }
func (m *CancelResponse) String() string { return proto.CompactTextString(m) }
func (*CancelResponse) ProtoMessage()    {}
func (*CancelResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_9320203f9aee53ee, []int{3}
}

func (m *CancelResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CancelResponse.Unmarshal(m, b)
}
func (m *CancelResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CancelResponse.Marshal(b, m, deterministic)
}
func (m *CancelResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CancelResponse.Merge(m, src)
}
func (m *CancelResponse) XXX_Size() int {
	return xxx_messageInfo_CancelResponse.Size(m)
}
func (m *CancelResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_CancelResponse.DiscardUnknown(m)
}

var xxx_messageInfo_CancelResponse proto.InternalMessageInfo

func init() {
	proto.RegisterType((*WatchRequest)(nil), "peloton.api.v1alpha.watch.svc.WatchRequest")
	proto.RegisterType((*WatchResponse)(nil), "peloton.api.v1alpha.watch.svc.WatchResponse")
	proto.RegisterType((*CancelRequest)(nil), "peloton.api.v1alpha.watch.svc.CancelRequest")
	proto.RegisterType((*CancelResponse)(nil), "peloton.api.v1alpha.watch.svc.CancelResponse")
}

func init() {
	proto.RegisterFile("peloton/api/v1alpha/watch/svc/watch_svc.proto", fileDescriptor_9320203f9aee53ee)
}

var fileDescriptor_9320203f9aee53ee = []byte{
	// 476 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x94, 0x51, 0x6b, 0xdb, 0x30,
	0x10, 0xc7, 0x71, 0x92, 0x66, 0xed, 0xad, 0x09, 0x43, 0x6c, 0xe0, 0x19, 0x0a, 0x99, 0xd7, 0x42,
	0xd9, 0x16, 0x79, 0xed, 0x1e, 0xf6, 0xb6, 0x87, 0x75, 0x14, 0x9a, 0x87, 0x52, 0x9c, 0xc1, 0x60,
	0x7b, 0x30, 0xb2, 0xa5, 0x52, 0x07, 0xc7, 0xa7, 0x59, 0x8a, 0xc7, 0x3e, 0xeb, 0xbe, 0xcb, 0x18,
	0x96, 0x2d, 0xa7, 0x61, 0x89, 0xdb, 0x87, 0x10, 0xdd, 0xf1, 0xbf, 0x9f, 0xee, 0x7f, 0x92, 0x05,
	0x53, 0x29, 0x32, 0xd4, 0x98, 0x07, 0x4c, 0xa6, 0x41, 0x79, 0xc6, 0x32, 0x79, 0xc7, 0x82, 0x5f,
	0x4c, 0x27, 0x77, 0x81, 0x2a, 0x93, 0x7a, 0x15, 0xa9, 0x32, 0xa1, 0xb2, 0x40, 0x8d, 0xe4, 0xa8,
	0x91, 0x53, 0x26, 0x53, 0xda, 0xc8, 0xa9, 0x11, 0x51, 0x55, 0x26, 0xde, 0xd9, 0x36, 0xda, 0x02,
	0xe3, 0x40, 0x69, 0xa6, 0x45, 0x26, 0x94, 0x5a, 0xaf, 0x6a, 0xa2, 0xf7, 0x6a, 0x5b, 0x89, 0xdd,
	0xa5, 0x4b, 0x82, 0xbc, 0xfa, 0x35, 0x92, 0x93, 0xdd, 0x36, 0xea, 0xee, 0x8c, 0xcc, 0xff, 0xe3,
	0xc0, 0xe1, 0xb7, 0x2a, 0x0e, 0xc5, 0xcf, 0x95, 0x50, 0x9a, 0x9c, 0xc0, 0x58, 0x69, 0x56, 0xe8,
	0xa8, 0x10, 0x65, 0xaa, 0x52, 0xcc, 0x5d, 0x67, 0xe2, 0x9c, 0x0e, 0xc2, 0x91, 0xc9, 0x86, 0x4d,
	0x92, 0x44, 0xf0, 0xbc, 0xed, 0x3b, 0x5a, 0x60, 0x1c, 0xdd, 0xa6, 0x99, 0x16, 0x85, 0xdb, 0x9b,
	0x38, 0xa7, 0x4f, 0xcf, 0xa7, 0x74, 0xf7, 0x54, 0xe6, 0xb6, 0x6c, 0x86, 0xf1, 0xa5, 0x29, 0x0a,
	0x89, 0xfa, 0x2f, 0x47, 0x2e, 0x00, 0x24, 0x72, 0x8b, 0xed, 0x1b, 0xec, 0x71, 0x07, 0xf6, 0x06,
	0x79, 0x43, 0x3b, 0x90, 0x76, 0xe9, 0xff, 0xed, 0xc1, 0xa8, 0x71, 0xa7, 0x24, 0xe6, 0x4a, 0x90,
	0x97, 0xb0, 0x5f, 0x9f, 0x60, 0xca, 0x8d, 0xb1, 0x83, 0xf0, 0x89, 0x89, 0xaf, 0x38, 0xf1, 0x60,
	0xbf, 0xf5, 0xdc, 0x33, 0x9e, 0xdb, 0x98, 0x7c, 0x35, 0x53, 0x59, 0xdb, 0x55, 0x6e, 0x7f, 0xd2,
	0xdf, 0x69, 0x74, 0x81, 0x31, 0x5d, 0x9f, 0xea, 0x0c, 0xe3, 0xf9, 0x6a, 0xb9, 0x64, 0xc5, 0x6f,
	0x33, 0xc4, 0xd6, 0xa8, 0x22, 0x3f, 0xc0, 0xdd, 0xa4, 0x46, 0x39, 0xea, 0xe8, 0x16, 0x57, 0x39,
	0x77, 0x07, 0x86, 0xef, 0x6f, 0xe5, 0xdb, 0xdc, 0x0c, 0xe3, 0xab, 0x2f, 0xe1, 0x8b, 0x0d, 0xe8,
	0x35, 0xea, 0xcb, 0x0a, 0x40, 0x3e, 0xc2, 0x40, 0x22, 0x57, 0xee, 0x9e, 0x01, 0xbd, 0xde, 0x0e,
	0x42, 0x5e, 0x0d, 0xce, 0xb6, 0x67, 0x0a, 0xc8, 0x0c, 0xc6, 0xd5, 0xff, 0xbd, 0x5e, 0x86, 0x06,
	0x71, 0xdc, 0xd9, 0xcb, 0x0d, 0xf2, 0x6b, 0xb6, 0x14, 0xe1, 0x61, 0x55, 0x6b, 0x9b, 0xf0, 0xdf,
	0xc0, 0xe8, 0x82, 0xe5, 0x89, 0xc8, 0xec, 0xf5, 0xda, 0x3d, 0x7f, 0xff, 0x19, 0x8c, 0xad, 0xb6,
	0x3e, 0xac, 0xf3, 0xf6, 0x72, 0xce, 0x45, 0x51, 0xa6, 0x89, 0x20, 0x1c, 0xf6, 0x4c, 0x4c, 0xde,
	0xd2, 0xce, 0xcf, 0x8e, 0xde, 0xbf, 0xd2, 0xde, 0xbb, 0xc7, 0x89, 0xeb, 0x4d, 0xdf, 0x3b, 0x44,
	0xc0, 0xb0, 0x6e, 0x84, 0x3c, 0x54, 0xb9, 0xe1, 0xcd, 0x9b, 0x3e, 0x52, 0x5d, 0x6f, 0xf4, 0xf9,
	0x13, 0x74, 0xbf, 0x1d, 0xdf, 0x8f, 0x3a, 0x5f, 0xa2, 0x78, 0x68, 0xbe, 0xe0, 0x0f, 0xff, 0x02,
	0x00, 0x00, 0xff, 0xff, 0xf4, 0xe4, 0x44, 0x0b, 0xb1, 0x04, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// WatchServiceClient is the client API for WatchService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type WatchServiceClient interface {
	// Create a watch to get notified about changes to Peloton objects.
	// Changed objects are streamed back to the caller till the watch
	// is cancelled.
	Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (WatchService_WatchClient, error)
	// Cancel a watch. The watch stream will get an error indicating
	// watch was cancelled and the stream will be closed.
	Cancel(ctx context.Context, in *CancelRequest, opts ...grpc.CallOption) (*CancelResponse, error)
}

type watchServiceClient struct {
	cc *grpc.ClientConn
}

func NewWatchServiceClient(cc *grpc.ClientConn) WatchServiceClient {
	return &watchServiceClient{cc}
}

func (c *watchServiceClient) Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (WatchService_WatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_WatchService_serviceDesc.Streams[0], "/peloton.api.v1alpha.watch.svc.WatchService/Watch", opts...)
	if err != nil {
		return nil, err
	}
	x := &watchServiceWatchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type WatchService_WatchClient interface {
	Recv() (*WatchResponse, error)
	grpc.ClientStream
}

type watchServiceWatchClient struct {
	grpc.ClientStream
}

func (x *watchServiceWatchClient) Recv() (*WatchResponse, error) {
	m := new(WatchResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *watchServiceClient) Cancel(ctx context.Context, in *CancelRequest, opts ...grpc.CallOption) (*CancelResponse, error) {
	out := new(CancelResponse)
	err := c.cc.Invoke(ctx, "/peloton.api.v1alpha.watch.svc.WatchService/Cancel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WatchServiceServer is the server API for WatchService service.
type WatchServiceServer interface {
	// Create a watch to get notified about changes to Peloton objects.
	// Changed objects are streamed back to the caller till the watch
	// is cancelled.
	Watch(*WatchRequest, WatchService_WatchServer) error
	// Cancel a watch. The watch stream will get an error indicating
	// watch was cancelled and the stream will be closed.
	Cancel(context.Context, *CancelRequest) (*CancelResponse, error)
}

func RegisterWatchServiceServer(s *grpc.Server, srv WatchServiceServer) {
	s.RegisterService(&_WatchService_serviceDesc, srv)
}

func _WatchService_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WatchServiceServer).Watch(m, &watchServiceWatchServer{stream})
}

type WatchService_WatchServer interface {
	Send(*WatchResponse) error
	grpc.ServerStream
}

type watchServiceWatchServer struct {
	grpc.ServerStream
}

func (x *watchServiceWatchServer) Send(m *WatchResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _WatchService_Cancel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WatchServiceServer).Cancel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peloton.api.v1alpha.watch.svc.WatchService/Cancel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WatchServiceServer).Cancel(ctx, req.(*CancelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _WatchService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "peloton.api.v1alpha.watch.svc.WatchService",
	HandlerType: (*WatchServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Cancel",
			Handler:    _WatchService_Cancel_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Watch",
			Handler:       _WatchService_Watch_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "peloton/api/v1alpha/watch/svc/watch_svc.proto",
}
