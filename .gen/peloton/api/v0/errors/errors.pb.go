// Code generated by protoc-gen-go. DO NOT EDIT.
// source: peloton/api/v0/errors/errors.proto

package errors

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
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

type JobNotFound struct {
	Id                   *peloton.JobID `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Message              string         `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *JobNotFound) Reset()         { *m = JobNotFound{} }
func (m *JobNotFound) String() string { return proto.CompactTextString(m) }
func (*JobNotFound) ProtoMessage()    {}
func (*JobNotFound) Descriptor() ([]byte, []int) {
	return fileDescriptor_87d731d109527e8b, []int{0}
}

func (m *JobNotFound) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_JobNotFound.Unmarshal(m, b)
}
func (m *JobNotFound) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_JobNotFound.Marshal(b, m, deterministic)
}
func (m *JobNotFound) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobNotFound.Merge(m, src)
}
func (m *JobNotFound) XXX_Size() int {
	return xxx_messageInfo_JobNotFound.Size(m)
}
func (m *JobNotFound) XXX_DiscardUnknown() {
	xxx_messageInfo_JobNotFound.DiscardUnknown(m)
}

var xxx_messageInfo_JobNotFound proto.InternalMessageInfo

func (m *JobNotFound) GetId() *peloton.JobID {
	if m != nil {
		return m.Id
	}
	return nil
}

func (m *JobNotFound) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

type JobGetRuntimeFail struct {
	Id                   *peloton.JobID `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Message              string         `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *JobGetRuntimeFail) Reset()         { *m = JobGetRuntimeFail{} }
func (m *JobGetRuntimeFail) String() string { return proto.CompactTextString(m) }
func (*JobGetRuntimeFail) ProtoMessage()    {}
func (*JobGetRuntimeFail) Descriptor() ([]byte, []int) {
	return fileDescriptor_87d731d109527e8b, []int{1}
}

func (m *JobGetRuntimeFail) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_JobGetRuntimeFail.Unmarshal(m, b)
}
func (m *JobGetRuntimeFail) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_JobGetRuntimeFail.Marshal(b, m, deterministic)
}
func (m *JobGetRuntimeFail) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobGetRuntimeFail.Merge(m, src)
}
func (m *JobGetRuntimeFail) XXX_Size() int {
	return xxx_messageInfo_JobGetRuntimeFail.Size(m)
}
func (m *JobGetRuntimeFail) XXX_DiscardUnknown() {
	xxx_messageInfo_JobGetRuntimeFail.DiscardUnknown(m)
}

var xxx_messageInfo_JobGetRuntimeFail proto.InternalMessageInfo

func (m *JobGetRuntimeFail) GetId() *peloton.JobID {
	if m != nil {
		return m.Id
	}
	return nil
}

func (m *JobGetRuntimeFail) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

type UnknownError struct {
	Message              string   `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UnknownError) Reset()         { *m = UnknownError{} }
func (m *UnknownError) String() string { return proto.CompactTextString(m) }
func (*UnknownError) ProtoMessage()    {}
func (*UnknownError) Descriptor() ([]byte, []int) {
	return fileDescriptor_87d731d109527e8b, []int{2}
}

func (m *UnknownError) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UnknownError.Unmarshal(m, b)
}
func (m *UnknownError) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UnknownError.Marshal(b, m, deterministic)
}
func (m *UnknownError) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UnknownError.Merge(m, src)
}
func (m *UnknownError) XXX_Size() int {
	return xxx_messageInfo_UnknownError.Size(m)
}
func (m *UnknownError) XXX_DiscardUnknown() {
	xxx_messageInfo_UnknownError.DiscardUnknown(m)
}

var xxx_messageInfo_UnknownError proto.InternalMessageInfo

func (m *UnknownError) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

type InvalidRespool struct {
	RespoolID            *peloton.ResourcePoolID `protobuf:"bytes,1,opt,name=respoolID,proto3" json:"respoolID,omitempty"`
	Message              string                  `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                `json:"-"`
	XXX_unrecognized     []byte                  `json:"-"`
	XXX_sizecache        int32                   `json:"-"`
}

func (m *InvalidRespool) Reset()         { *m = InvalidRespool{} }
func (m *InvalidRespool) String() string { return proto.CompactTextString(m) }
func (*InvalidRespool) ProtoMessage()    {}
func (*InvalidRespool) Descriptor() ([]byte, []int) {
	return fileDescriptor_87d731d109527e8b, []int{3}
}

func (m *InvalidRespool) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InvalidRespool.Unmarshal(m, b)
}
func (m *InvalidRespool) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InvalidRespool.Marshal(b, m, deterministic)
}
func (m *InvalidRespool) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InvalidRespool.Merge(m, src)
}
func (m *InvalidRespool) XXX_Size() int {
	return xxx_messageInfo_InvalidRespool.Size(m)
}
func (m *InvalidRespool) XXX_DiscardUnknown() {
	xxx_messageInfo_InvalidRespool.DiscardUnknown(m)
}

var xxx_messageInfo_InvalidRespool proto.InternalMessageInfo

func (m *InvalidRespool) GetRespoolID() *peloton.ResourcePoolID {
	if m != nil {
		return m.RespoolID
	}
	return nil
}

func (m *InvalidRespool) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

func init() {
	proto.RegisterType((*JobNotFound)(nil), "peloton.api.v0.errors.JobNotFound")
	proto.RegisterType((*JobGetRuntimeFail)(nil), "peloton.api.v0.errors.JobGetRuntimeFail")
	proto.RegisterType((*UnknownError)(nil), "peloton.api.v0.errors.UnknownError")
	proto.RegisterType((*InvalidRespool)(nil), "peloton.api.v0.errors.InvalidRespool")
}

func init() { proto.RegisterFile("peloton/api/v0/errors/errors.proto", fileDescriptor_87d731d109527e8b) }

var fileDescriptor_87d731d109527e8b = []byte{
	// 245 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x91, 0xc1, 0x4b, 0xc3, 0x30,
	0x18, 0xc5, 0x69, 0x0f, 0xca, 0xbe, 0x89, 0x60, 0x61, 0x50, 0x44, 0x61, 0xf4, 0x20, 0xbd, 0x98,
	0x0e, 0xfd, 0x0f, 0xa4, 0x4e, 0xda, 0x83, 0x48, 0x40, 0x0f, 0xe2, 0x25, 0x35, 0x1f, 0x12, 0xcc,
	0xf2, 0x85, 0x24, 0xad, 0xff, 0xbe, 0xd8, 0xb5, 0xa8, 0x63, 0xbd, 0xed, 0x94, 0x3c, 0xf2, 0x7b,
	0x8f, 0x17, 0x1e, 0x64, 0x16, 0x35, 0x05, 0x32, 0x85, 0xb0, 0xaa, 0xe8, 0x56, 0x05, 0x3a, 0x47,
	0xce, 0x0f, 0x07, 0xb3, 0x8e, 0x02, 0x25, 0x8b, 0x81, 0x61, 0xc2, 0x2a, 0xd6, 0xad, 0xd8, 0xf6,
	0xf1, 0xfc, 0x62, 0xc7, 0x3a, 0x52, 0xbd, 0x29, 0x7b, 0x81, 0x79, 0x4d, 0xcd, 0x23, 0x85, 0x35,
	0xb5, 0x46, 0x26, 0xd7, 0x10, 0x2b, 0x99, 0x46, 0xcb, 0x28, 0x9f, 0xdf, 0x5c, 0xb2, 0x9d, 0xc0,
	0x51, 0xd6, 0xd4, 0x54, 0x25, 0x8f, 0x95, 0x4c, 0x52, 0x38, 0xde, 0xa0, 0xf7, 0xe2, 0x03, 0xd3,
	0x78, 0x19, 0xe5, 0x33, 0x3e, 0xca, 0xec, 0x0d, 0xce, 0x6a, 0x6a, 0x1e, 0x30, 0xf0, 0xd6, 0x04,
	0xb5, 0xc1, 0xb5, 0x50, 0xfa, 0x70, 0xe9, 0x39, 0x9c, 0x3c, 0x9b, 0x4f, 0x43, 0x5f, 0xe6, 0xfe,
	0xe7, 0x93, 0x7f, 0xc9, 0xe8, 0x3f, 0x69, 0xe1, 0xb4, 0x32, 0x9d, 0xd0, 0x4a, 0x72, 0xf4, 0x96,
	0x48, 0x27, 0x25, 0xcc, 0xdc, 0xf6, 0x5a, 0x95, 0x43, 0x97, 0xab, 0xa9, 0x2e, 0x1c, 0x3d, 0xb5,
	0xee, 0x1d, 0x9f, 0x7a, 0x9a, 0xff, 0x1a, 0xa7, 0xbb, 0xdd, 0x31, 0xd8, 0x3f, 0xc4, 0xeb, 0x62,
	0xef, 0x86, 0xcd, 0x51, 0x3f, 0xc4, 0xed, 0x77, 0x00, 0x00, 0x00, 0xff, 0xff, 0x44, 0x41, 0x41,
	0x22, 0xe3, 0x01, 0x00, 0x00,
}
