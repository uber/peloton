// Code generated by protoc-gen-go. DO NOT EDIT.
// source: peloton/api/v1alpha/volume/volume.proto

package volume

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
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

// States of a persistent volume
// Deprecated
type VolumeState int32

const (
	// Reserved for future compatibility of new states.
	VolumeState_VOLUME_STATE_INVALID VolumeState = 0
	// The persistent volume is being initialized.
	VolumeState_VOLUME_STATE_INITIALIZED VolumeState = 1
	// The persistent volume is created successfully.
	VolumeState_VOLUME_STATE_CREATED VolumeState = 2
	// The persistent volume is deleted.
	VolumeState_VOLUME_STATE_DELETED VolumeState = 3
)

var VolumeState_name = map[int32]string{
	0: "VOLUME_STATE_INVALID",
	1: "VOLUME_STATE_INITIALIZED",
	2: "VOLUME_STATE_CREATED",
	3: "VOLUME_STATE_DELETED",
}

var VolumeState_value = map[string]int32{
	"VOLUME_STATE_INVALID":     0,
	"VOLUME_STATE_INITIALIZED": 1,
	"VOLUME_STATE_CREATED":     2,
	"VOLUME_STATE_DELETED":     3,
}

func (x VolumeState) String() string {
	return proto.EnumName(VolumeState_name, int32(x))
}

func (VolumeState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{0}
}

// Type of the volume
type VolumeSpec_VolumeType int32

const (
	// Invalid volume type
	VolumeSpec_VOLUME_TYPE_INVALID VolumeSpec_VolumeType = 0
	// Empty directory for the pod.
	VolumeSpec_VOLUME_TYPE_EMPTY_DIR VolumeSpec_VolumeType = 1
	// Host path mapped into the pod.
	VolumeSpec_VOLUME_TYPE_HOST_PATH VolumeSpec_VolumeType = 2
)

var VolumeSpec_VolumeType_name = map[int32]string{
	0: "VOLUME_TYPE_INVALID",
	1: "VOLUME_TYPE_EMPTY_DIR",
	2: "VOLUME_TYPE_HOST_PATH",
}

var VolumeSpec_VolumeType_value = map[string]int32{
	"VOLUME_TYPE_INVALID":   0,
	"VOLUME_TYPE_EMPTY_DIR": 1,
	"VOLUME_TYPE_HOST_PATH": 2,
}

func (x VolumeSpec_VolumeType) String() string {
	return proto.EnumName(VolumeSpec_VolumeType_name, int32(x))
}

func (VolumeSpec_VolumeType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{0, 0}
}

// Volume configuration.
type VolumeSpec struct {
	// Volume's name. Must be unique within a pod.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Type of the volume for the pod.
	Type VolumeSpec_VolumeType `protobuf:"varint,2,opt,name=type,proto3,enum=peloton.api.v1alpha.volume.VolumeSpec_VolumeType" json:"type,omitempty"`
	// EmptyDir represents a temporary directory that shares a pod's lifetime.
	EmptyDir *VolumeSpec_EmptyDirVolumeSource `protobuf:"bytes,3,opt,name=empty_dir,json=emptyDir,proto3" json:"empty_dir,omitempty"`
	// HostPath represents a pre-existing file or directory on the host
	// machine that is directly exposed to the container.
	HostPath             *VolumeSpec_HostPathVolumeSource `protobuf:"bytes,4,opt,name=host_path,json=hostPath,proto3" json:"host_path,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                         `json:"-"`
	XXX_unrecognized     []byte                           `json:"-"`
	XXX_sizecache        int32                            `json:"-"`
}

func (m *VolumeSpec) Reset()         { *m = VolumeSpec{} }
func (m *VolumeSpec) String() string { return proto.CompactTextString(m) }
func (*VolumeSpec) ProtoMessage()    {}
func (*VolumeSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{0}
}

func (m *VolumeSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_VolumeSpec.Unmarshal(m, b)
}
func (m *VolumeSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_VolumeSpec.Marshal(b, m, deterministic)
}
func (m *VolumeSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VolumeSpec.Merge(m, src)
}
func (m *VolumeSpec) XXX_Size() int {
	return xxx_messageInfo_VolumeSpec.Size(m)
}
func (m *VolumeSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_VolumeSpec.DiscardUnknown(m)
}

var xxx_messageInfo_VolumeSpec proto.InternalMessageInfo

func (m *VolumeSpec) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *VolumeSpec) GetType() VolumeSpec_VolumeType {
	if m != nil {
		return m.Type
	}
	return VolumeSpec_VOLUME_TYPE_INVALID
}

func (m *VolumeSpec) GetEmptyDir() *VolumeSpec_EmptyDirVolumeSource {
	if m != nil {
		return m.EmptyDir
	}
	return nil
}

func (m *VolumeSpec) GetHostPath() *VolumeSpec_HostPathVolumeSource {
	if m != nil {
		return m.HostPath
	}
	return nil
}

// Represents an empty directory for a pod.
// This is available only for the Kubelet runtime.
type VolumeSpec_EmptyDirVolumeSource struct {
	// What type of storage medium should back this directory.
	// The default is "" which means to use the node's default medium.
	// Must be an empty string (default) or Memory.
	Medium string `protobuf:"bytes,1,opt,name=medium,proto3" json:"medium,omitempty"`
	// Total amount of local storage required for this EmptyDir volume.
	// The size limit is also applicable for memory medium.
	// The maximum usage on memory medium EmptyDir would be the minimum value
	// between the value specified here and the sum of memory limits of all
	// containers in a pod. The default is nil which means that the limit is undefined.
	SizeInMb             uint32   `protobuf:"varint,2,opt,name=size_in_mb,json=sizeInMb,proto3" json:"size_in_mb,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *VolumeSpec_EmptyDirVolumeSource) Reset()         { *m = VolumeSpec_EmptyDirVolumeSource{} }
func (m *VolumeSpec_EmptyDirVolumeSource) String() string { return proto.CompactTextString(m) }
func (*VolumeSpec_EmptyDirVolumeSource) ProtoMessage()    {}
func (*VolumeSpec_EmptyDirVolumeSource) Descriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{0, 0}
}

func (m *VolumeSpec_EmptyDirVolumeSource) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource.Unmarshal(m, b)
}
func (m *VolumeSpec_EmptyDirVolumeSource) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource.Marshal(b, m, deterministic)
}
func (m *VolumeSpec_EmptyDirVolumeSource) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource.Merge(m, src)
}
func (m *VolumeSpec_EmptyDirVolumeSource) XXX_Size() int {
	return xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource.Size(m)
}
func (m *VolumeSpec_EmptyDirVolumeSource) XXX_DiscardUnknown() {
	xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource.DiscardUnknown(m)
}

var xxx_messageInfo_VolumeSpec_EmptyDirVolumeSource proto.InternalMessageInfo

func (m *VolumeSpec_EmptyDirVolumeSource) GetMedium() string {
	if m != nil {
		return m.Medium
	}
	return ""
}

func (m *VolumeSpec_EmptyDirVolumeSource) GetSizeInMb() uint32 {
	if m != nil {
		return m.SizeInMb
	}
	return 0
}

// Represents a host path mapped into a pod.
type VolumeSpec_HostPathVolumeSource struct {
	// Path of the directory on the host.
	Path                 string   `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *VolumeSpec_HostPathVolumeSource) Reset()         { *m = VolumeSpec_HostPathVolumeSource{} }
func (m *VolumeSpec_HostPathVolumeSource) String() string { return proto.CompactTextString(m) }
func (*VolumeSpec_HostPathVolumeSource) ProtoMessage()    {}
func (*VolumeSpec_HostPathVolumeSource) Descriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{0, 1}
}

func (m *VolumeSpec_HostPathVolumeSource) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_VolumeSpec_HostPathVolumeSource.Unmarshal(m, b)
}
func (m *VolumeSpec_HostPathVolumeSource) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_VolumeSpec_HostPathVolumeSource.Marshal(b, m, deterministic)
}
func (m *VolumeSpec_HostPathVolumeSource) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VolumeSpec_HostPathVolumeSource.Merge(m, src)
}
func (m *VolumeSpec_HostPathVolumeSource) XXX_Size() int {
	return xxx_messageInfo_VolumeSpec_HostPathVolumeSource.Size(m)
}
func (m *VolumeSpec_HostPathVolumeSource) XXX_DiscardUnknown() {
	xxx_messageInfo_VolumeSpec_HostPathVolumeSource.DiscardUnknown(m)
}

var xxx_messageInfo_VolumeSpec_HostPathVolumeSource proto.InternalMessageInfo

func (m *VolumeSpec_HostPathVolumeSource) GetPath() string {
	if m != nil {
		return m.Path
	}
	return ""
}

// Persistent volume information.
// Deprecated
type PersistentVolumeInfo struct {
	// ID of the persistent volume.
	VolumeId *peloton.VolumeID `protobuf:"bytes,1,opt,name=volume_id,json=volumeId,proto3" json:"volume_id,omitempty"`
	// ID of the pod that owns the volume.
	PodName *peloton.PodName `protobuf:"bytes,2,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`
	// Hostname of the persisted volume.
	Hostname string `protobuf:"bytes,3,opt,name=hostname,proto3" json:"hostname,omitempty"`
	// Current state of the volume.
	State VolumeState `protobuf:"varint,4,opt,name=state,proto3,enum=peloton.api.v1alpha.volume.VolumeState" json:"state,omitempty"`
	// Goal state of the volume.
	DesiredState VolumeState `protobuf:"varint,5,opt,name=desired_state,json=desiredState,proto3,enum=peloton.api.v1alpha.volume.VolumeState" json:"desired_state,omitempty"`
	// Volume size in MB.
	SizeMb uint32 `protobuf:"varint,6,opt,name=size_mb,json=sizeMb,proto3" json:"size_mb,omitempty"`
	// Volume mount path inside container.
	ContainerPath string `protobuf:"bytes,7,opt,name=container_path,json=containerPath,proto3" json:"container_path,omitempty"`
	// Volume creation time.
	CreateTime string `protobuf:"bytes,8,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	// Volume info last update time.
	UpdateTime           string   `protobuf:"bytes,9,opt,name=update_time,json=updateTime,proto3" json:"update_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PersistentVolumeInfo) Reset()         { *m = PersistentVolumeInfo{} }
func (m *PersistentVolumeInfo) String() string { return proto.CompactTextString(m) }
func (*PersistentVolumeInfo) ProtoMessage()    {}
func (*PersistentVolumeInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_4d0492ce3f2c3b6d, []int{1}
}

func (m *PersistentVolumeInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PersistentVolumeInfo.Unmarshal(m, b)
}
func (m *PersistentVolumeInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PersistentVolumeInfo.Marshal(b, m, deterministic)
}
func (m *PersistentVolumeInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PersistentVolumeInfo.Merge(m, src)
}
func (m *PersistentVolumeInfo) XXX_Size() int {
	return xxx_messageInfo_PersistentVolumeInfo.Size(m)
}
func (m *PersistentVolumeInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_PersistentVolumeInfo.DiscardUnknown(m)
}

var xxx_messageInfo_PersistentVolumeInfo proto.InternalMessageInfo

func (m *PersistentVolumeInfo) GetVolumeId() *peloton.VolumeID {
	if m != nil {
		return m.VolumeId
	}
	return nil
}

func (m *PersistentVolumeInfo) GetPodName() *peloton.PodName {
	if m != nil {
		return m.PodName
	}
	return nil
}

func (m *PersistentVolumeInfo) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

func (m *PersistentVolumeInfo) GetState() VolumeState {
	if m != nil {
		return m.State
	}
	return VolumeState_VOLUME_STATE_INVALID
}

func (m *PersistentVolumeInfo) GetDesiredState() VolumeState {
	if m != nil {
		return m.DesiredState
	}
	return VolumeState_VOLUME_STATE_INVALID
}

func (m *PersistentVolumeInfo) GetSizeMb() uint32 {
	if m != nil {
		return m.SizeMb
	}
	return 0
}

func (m *PersistentVolumeInfo) GetContainerPath() string {
	if m != nil {
		return m.ContainerPath
	}
	return ""
}

func (m *PersistentVolumeInfo) GetCreateTime() string {
	if m != nil {
		return m.CreateTime
	}
	return ""
}

func (m *PersistentVolumeInfo) GetUpdateTime() string {
	if m != nil {
		return m.UpdateTime
	}
	return ""
}

func init() {
	proto.RegisterEnum("peloton.api.v1alpha.volume.VolumeState", VolumeState_name, VolumeState_value)
	proto.RegisterEnum("peloton.api.v1alpha.volume.VolumeSpec_VolumeType", VolumeSpec_VolumeType_name, VolumeSpec_VolumeType_value)
	proto.RegisterType((*VolumeSpec)(nil), "peloton.api.v1alpha.volume.VolumeSpec")
	proto.RegisterType((*VolumeSpec_EmptyDirVolumeSource)(nil), "peloton.api.v1alpha.volume.VolumeSpec.EmptyDirVolumeSource")
	proto.RegisterType((*VolumeSpec_HostPathVolumeSource)(nil), "peloton.api.v1alpha.volume.VolumeSpec.HostPathVolumeSource")
	proto.RegisterType((*PersistentVolumeInfo)(nil), "peloton.api.v1alpha.volume.PersistentVolumeInfo")
}

func init() {
	proto.RegisterFile("peloton/api/v1alpha/volume/volume.proto", fileDescriptor_4d0492ce3f2c3b6d)
}

var fileDescriptor_4d0492ce3f2c3b6d = []byte{
	// 576 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x54, 0x51, 0x6b, 0xdb, 0x3c,
	0x14, 0xfd, 0x9c, 0xb4, 0x69, 0x72, 0xf3, 0xb5, 0x04, 0x2d, 0x5b, 0x3d, 0x53, 0x58, 0x57, 0x56,
	0x5a, 0xfa, 0xe0, 0xd0, 0xec, 0x65, 0x30, 0xc6, 0x48, 0x67, 0x43, 0x0c, 0x4e, 0x6b, 0x1c, 0x2f,
	0xac, 0xdd, 0x83, 0x70, 0x62, 0x8d, 0x08, 0x62, 0x4b, 0xd8, 0x4a, 0x21, 0xfb, 0x81, 0xfb, 0x41,
	0xfb, 0x05, 0x43, 0xb2, 0x92, 0x65, 0x6d, 0x5a, 0xda, 0x27, 0x4b, 0xe7, 0x9e, 0x73, 0x24, 0x9f,
	0x7b, 0x11, 0x9c, 0x70, 0x32, 0x63, 0x82, 0x65, 0x9d, 0x98, 0xd3, 0xce, 0xed, 0x79, 0x3c, 0xe3,
	0xd3, 0xb8, 0x73, 0xcb, 0x66, 0xf3, 0x94, 0xe8, 0x8f, 0xcd, 0x73, 0x26, 0x18, 0xb2, 0x34, 0xd1,
	0x8e, 0x39, 0xb5, 0x35, 0xd1, 0x2e, 0x19, 0xd6, 0xdb, 0x4d, 0x26, 0x4b, 0xbe, 0x92, 0x1f, 0xfd,
	0xae, 0x02, 0x8c, 0x14, 0x7b, 0xc8, 0xc9, 0x04, 0x21, 0xd8, 0xca, 0xe2, 0x94, 0x98, 0xc6, 0xa1,
	0x71, 0xda, 0x08, 0xd5, 0x1a, 0xb9, 0xb0, 0x25, 0x16, 0x9c, 0x98, 0x95, 0x43, 0xe3, 0x74, 0xaf,
	0x7b, 0x6e, 0x3f, 0x7c, 0xa0, 0xfd, 0xd7, 0x49, 0x2f, 0xa3, 0x05, 0x27, 0xa1, 0x92, 0xa3, 0x6f,
	0xd0, 0x20, 0x29, 0x17, 0x0b, 0x9c, 0xd0, 0xdc, 0xac, 0x1e, 0x1a, 0xa7, 0xcd, 0xee, 0xc7, 0x27,
	0x7a, 0xb9, 0x52, 0xe7, 0xd0, 0x5c, 0x43, 0x6c, 0x9e, 0x4f, 0x48, 0x58, 0x27, 0x1a, 0x95, 0xce,
	0x53, 0x56, 0x08, 0xcc, 0x63, 0x31, 0x35, 0xb7, 0x9e, 0xe5, 0xdc, 0x67, 0x85, 0x08, 0x62, 0x31,
	0xfd, 0xd7, 0x79, 0xaa, 0x51, 0xcb, 0x87, 0xf6, 0xa6, 0xb3, 0xd1, 0x2b, 0xa8, 0xa5, 0x24, 0xa1,
	0xf3, 0x54, 0x07, 0xa5, 0x77, 0xe8, 0x00, 0xa0, 0xa0, 0x3f, 0x09, 0xa6, 0x19, 0x4e, 0xc7, 0x2a,
	0xb0, 0xdd, 0xb0, 0x2e, 0x11, 0x2f, 0x1b, 0x8c, 0xad, 0x33, 0x68, 0x6f, 0x3a, 0x4f, 0x86, 0xae,
	0xae, 0xae, 0x43, 0x97, 0xeb, 0xa3, 0xef, 0xcb, 0xb6, 0xc8, 0x04, 0xd1, 0x3e, 0xbc, 0x18, 0x5d,
	0xf9, 0x5f, 0x07, 0x2e, 0x8e, 0xae, 0x03, 0x17, 0x7b, 0x97, 0xa3, 0x9e, 0xef, 0x39, 0xad, 0xff,
	0xd0, 0x6b, 0x78, 0xb9, 0x5e, 0x70, 0x07, 0x41, 0x74, 0x8d, 0x1d, 0x2f, 0x6c, 0x19, 0x77, 0x4b,
	0xfd, 0xab, 0x61, 0x84, 0x83, 0x5e, 0xd4, 0x6f, 0x55, 0x8e, 0x7e, 0x55, 0xa1, 0x1d, 0x90, 0xbc,
	0xa0, 0x85, 0x20, 0x99, 0x28, 0xcf, 0xf1, 0xb2, 0x1f, 0x0c, 0x5d, 0x40, 0xa3, 0xcc, 0x08, 0xd3,
	0x44, 0x5d, 0xa7, 0xd9, 0x3d, 0xde, 0x98, 0xe4, 0x12, 0xd3, 0x5a, 0x27, 0xac, 0x97, 0x3a, 0x2f,
	0x41, 0x9f, 0xa1, 0xce, 0x59, 0x82, 0xd5, 0x18, 0x55, 0x94, 0xc5, 0xbb, 0x47, 0x2d, 0x02, 0x96,
	0x5c, 0xc6, 0x29, 0x09, 0x77, 0x78, 0xb9, 0x40, 0x16, 0xa8, 0x06, 0x28, 0x83, 0xaa, 0x8a, 0x64,
	0xb5, 0x47, 0x9f, 0x60, 0xbb, 0x10, 0xb1, 0x20, 0xaa, 0xcd, 0x7b, 0xdd, 0x93, 0x27, 0xb4, 0x59,
	0xd2, 0xc3, 0x52, 0x85, 0x7c, 0xd8, 0x4d, 0x48, 0x41, 0x73, 0x92, 0xe0, 0xd2, 0x66, 0xfb, 0x79,
	0x36, 0xff, 0x6b, 0xb5, 0xda, 0xa1, 0x7d, 0xd8, 0x51, 0xdd, 0x4e, 0xc7, 0x66, 0x4d, 0xb5, 0xba,
	0x26, 0xb7, 0x83, 0x31, 0x3a, 0x86, 0xbd, 0x09, 0xcb, 0x44, 0x4c, 0x33, 0x92, 0x97, 0x53, 0xb9,
	0xa3, 0xfe, 0x63, 0x77, 0x85, 0xca, 0x19, 0x40, 0x6f, 0xa0, 0x39, 0xc9, 0x49, 0x2c, 0x08, 0x16,
	0x34, 0x25, 0x66, 0x5d, 0x71, 0xa0, 0x84, 0x22, 0x9a, 0x12, 0x49, 0x98, 0xf3, 0x64, 0x45, 0x68,
	0x94, 0x84, 0x12, 0x92, 0x84, 0xb3, 0x05, 0x34, 0xd7, 0xae, 0x87, 0x4c, 0x68, 0xeb, 0x96, 0x0f,
	0xa3, 0x5e, 0xb4, 0x3e, 0x27, 0x07, 0x60, 0xde, 0xa9, 0x78, 0x91, 0xd7, 0xf3, 0xbd, 0x1b, 0xd7,
	0x69, 0x19, 0xf7, 0x74, 0x5f, 0x42, 0xb7, 0x17, 0xb9, 0x4e, 0xab, 0x72, 0xaf, 0xe2, 0xb8, 0xbe,
	0x2b, 0x2b, 0xd5, 0x8b, 0x0f, 0xf0, 0xc8, 0xcb, 0x73, 0x63, 0x3d, 0xfc, 0x7c, 0x8d, 0x6b, 0xea,
	0xe5, 0x79, 0xff, 0x27, 0x00, 0x00, 0xff, 0xff, 0x75, 0x6b, 0x6f, 0x6c, 0xe3, 0x04, 0x00, 0x00,
}
