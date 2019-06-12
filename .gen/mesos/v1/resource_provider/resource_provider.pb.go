// Code generated by protoc-gen-go. DO NOT EDIT.
// source: mesos/v1/resource_provider/resource_provider.proto

package mesos_v1_resource_provider

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	v1 "github.com/uber/peloton/.gen/mesos/v1"
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

type Event_Type int32

const (
	// This must be the first enum value in this list, to
	// ensure that if 'type' is not set, the default value
	// is UNKNOWN. This enables enum values to be added
	// in a backwards-compatible way. See: MESOS-4997.
	Event_UNKNOWN                      Event_Type = 0
	Event_SUBSCRIBED                   Event_Type = 1
	Event_APPLY_OPERATION              Event_Type = 2
	Event_PUBLISH_RESOURCES            Event_Type = 3
	Event_ACKNOWLEDGE_OPERATION_STATUS Event_Type = 4
	Event_RECONCILE_OPERATIONS         Event_Type = 5
)

var Event_Type_name = map[int32]string{
	0: "UNKNOWN",
	1: "SUBSCRIBED",
	2: "APPLY_OPERATION",
	3: "PUBLISH_RESOURCES",
	4: "ACKNOWLEDGE_OPERATION_STATUS",
	5: "RECONCILE_OPERATIONS",
}

var Event_Type_value = map[string]int32{
	"UNKNOWN":                      0,
	"SUBSCRIBED":                   1,
	"APPLY_OPERATION":              2,
	"PUBLISH_RESOURCES":            3,
	"ACKNOWLEDGE_OPERATION_STATUS": 4,
	"RECONCILE_OPERATIONS":         5,
}

func (x Event_Type) Enum() *Event_Type {
	p := new(Event_Type)
	*p = x
	return p
}

func (x Event_Type) String() string {
	return proto.EnumName(Event_Type_name, int32(x))
}

func (x *Event_Type) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Event_Type_value, data, "Event_Type")
	if err != nil {
		return err
	}
	*x = Event_Type(value)
	return nil
}

func (Event_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 0}
}

type Call_Type int32

const (
	// This must be the first enum value in this list, to
	// ensure that if 'type' is not set, the default value
	// is UNKNOWN. This enables enum values to be added
	// in a backwards-compatible way. See: MESOS-4997.
	Call_UNKNOWN                         Call_Type = 0
	Call_SUBSCRIBE                       Call_Type = 1
	Call_UPDATE_OPERATION_STATUS         Call_Type = 2
	Call_UPDATE_STATE                    Call_Type = 3
	Call_UPDATE_PUBLISH_RESOURCES_STATUS Call_Type = 4
)

var Call_Type_name = map[int32]string{
	0: "UNKNOWN",
	1: "SUBSCRIBE",
	2: "UPDATE_OPERATION_STATUS",
	3: "UPDATE_STATE",
	4: "UPDATE_PUBLISH_RESOURCES_STATUS",
}

var Call_Type_value = map[string]int32{
	"UNKNOWN":                         0,
	"SUBSCRIBE":                       1,
	"UPDATE_OPERATION_STATUS":         2,
	"UPDATE_STATE":                    3,
	"UPDATE_PUBLISH_RESOURCES_STATUS": 4,
}

func (x Call_Type) Enum() *Call_Type {
	p := new(Call_Type)
	*p = x
	return p
}

func (x Call_Type) String() string {
	return proto.EnumName(Call_Type_name, int32(x))
}

func (x *Call_Type) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Call_Type_value, data, "Call_Type")
	if err != nil {
		return err
	}
	*x = Call_Type(value)
	return nil
}

func (Call_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 0}
}

type Call_UpdatePublishResourcesStatus_Status int32

const (
	Call_UpdatePublishResourcesStatus_UNKNOWN Call_UpdatePublishResourcesStatus_Status = 0
	Call_UpdatePublishResourcesStatus_OK      Call_UpdatePublishResourcesStatus_Status = 1
	Call_UpdatePublishResourcesStatus_FAILED  Call_UpdatePublishResourcesStatus_Status = 2
)

var Call_UpdatePublishResourcesStatus_Status_name = map[int32]string{
	0: "UNKNOWN",
	1: "OK",
	2: "FAILED",
}

var Call_UpdatePublishResourcesStatus_Status_value = map[string]int32{
	"UNKNOWN": 0,
	"OK":      1,
	"FAILED":  2,
}

func (x Call_UpdatePublishResourcesStatus_Status) Enum() *Call_UpdatePublishResourcesStatus_Status {
	p := new(Call_UpdatePublishResourcesStatus_Status)
	*p = x
	return p
}

func (x Call_UpdatePublishResourcesStatus_Status) String() string {
	return proto.EnumName(Call_UpdatePublishResourcesStatus_Status_name, int32(x))
}

func (x *Call_UpdatePublishResourcesStatus_Status) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Call_UpdatePublishResourcesStatus_Status_value, data, "Call_UpdatePublishResourcesStatus_Status")
	if err != nil {
		return err
	}
	*x = Call_UpdatePublishResourcesStatus_Status(value)
	return nil
}

func (Call_UpdatePublishResourcesStatus_Status) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 3, 0}
}

// NOTE: For the time being, this API is subject to change and the related
// feature is experimental.
type Event struct {
	Type                       *Event_Type                       `protobuf:"varint,1,opt,name=type,enum=mesos.v1.resource_provider.Event_Type" json:"type,omitempty"`
	Subscribed                 *Event_Subscribed                 `protobuf:"bytes,2,opt,name=subscribed" json:"subscribed,omitempty"`
	ApplyOperation             *Event_ApplyOperation             `protobuf:"bytes,3,opt,name=apply_operation,json=applyOperation" json:"apply_operation,omitempty"`
	PublishResources           *Event_PublishResources           `protobuf:"bytes,4,opt,name=publish_resources,json=publishResources" json:"publish_resources,omitempty"`
	AcknowledgeOperationStatus *Event_AcknowledgeOperationStatus `protobuf:"bytes,5,opt,name=acknowledge_operation_status,json=acknowledgeOperationStatus" json:"acknowledge_operation_status,omitempty"`
	ReconcileOperations        *Event_ReconcileOperations        `protobuf:"bytes,6,opt,name=reconcile_operations,json=reconcileOperations" json:"reconcile_operations,omitempty"`
	XXX_NoUnkeyedLiteral       struct{}                          `json:"-"`
	XXX_unrecognized           []byte                            `json:"-"`
	XXX_sizecache              int32                             `json:"-"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0}
}

func (m *Event) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event.Unmarshal(m, b)
}
func (m *Event) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event.Marshal(b, m, deterministic)
}
func (m *Event) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event.Merge(m, src)
}
func (m *Event) XXX_Size() int {
	return xxx_messageInfo_Event.Size(m)
}
func (m *Event) XXX_DiscardUnknown() {
	xxx_messageInfo_Event.DiscardUnknown(m)
}

var xxx_messageInfo_Event proto.InternalMessageInfo

func (m *Event) GetType() Event_Type {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return Event_UNKNOWN
}

func (m *Event) GetSubscribed() *Event_Subscribed {
	if m != nil {
		return m.Subscribed
	}
	return nil
}

func (m *Event) GetApplyOperation() *Event_ApplyOperation {
	if m != nil {
		return m.ApplyOperation
	}
	return nil
}

func (m *Event) GetPublishResources() *Event_PublishResources {
	if m != nil {
		return m.PublishResources
	}
	return nil
}

func (m *Event) GetAcknowledgeOperationStatus() *Event_AcknowledgeOperationStatus {
	if m != nil {
		return m.AcknowledgeOperationStatus
	}
	return nil
}

func (m *Event) GetReconcileOperations() *Event_ReconcileOperations {
	if m != nil {
		return m.ReconcileOperations
	}
	return nil
}

// First event received by the resource provider when it subscribes
// to the master.
type Event_Subscribed struct {
	ProviderId           *v1.ResourceProviderID `protobuf:"bytes,1,req,name=provider_id,json=providerId" json:"provider_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *Event_Subscribed) Reset()         { *m = Event_Subscribed{} }
func (m *Event_Subscribed) String() string { return proto.CompactTextString(m) }
func (*Event_Subscribed) ProtoMessage()    {}
func (*Event_Subscribed) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 0}
}

func (m *Event_Subscribed) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event_Subscribed.Unmarshal(m, b)
}
func (m *Event_Subscribed) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event_Subscribed.Marshal(b, m, deterministic)
}
func (m *Event_Subscribed) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event_Subscribed.Merge(m, src)
}
func (m *Event_Subscribed) XXX_Size() int {
	return xxx_messageInfo_Event_Subscribed.Size(m)
}
func (m *Event_Subscribed) XXX_DiscardUnknown() {
	xxx_messageInfo_Event_Subscribed.DiscardUnknown(m)
}

var xxx_messageInfo_Event_Subscribed proto.InternalMessageInfo

func (m *Event_Subscribed) GetProviderId() *v1.ResourceProviderID {
	if m != nil {
		return m.ProviderId
	}
	return nil
}

// Received when the master wants to send an operation to the
// resource provider.
type Event_ApplyOperation struct {
	FrameworkId *v1.FrameworkID     `protobuf:"bytes,1,opt,name=framework_id,json=frameworkId" json:"framework_id,omitempty"`
	Info        *v1.Offer_Operation `protobuf:"bytes,2,req,name=info" json:"info,omitempty"`
	// This is the internal UUID for the operation, which is kept
	// independently from the framework-specified operation ID, which
	// is optional.
	OperationUuid *v1.UUID `protobuf:"bytes,3,req,name=operation_uuid,json=operationUuid" json:"operation_uuid,omitempty"`
	// Used to establish the relationship between the operation and
	// the resources that the operation is operating on. Each resource
	// provider will keep a resource version UUID, and change it when
	// it believes that the resources from this resource provider are
	// out of sync from the master's view. The master will keep track
	// of the last known resource version UUID for each resource
	// provider, and attach the resource version UUID in each
	// operation it sends out. The resource provider should reject
	// operations that have a different resource version UUID than
	// that it maintains, because this means the operation is
	// operating on resources that might have already been
	// invalidated.
	ResourceVersionUuid  *v1.UUID `protobuf:"bytes,4,req,name=resource_version_uuid,json=resourceVersionUuid" json:"resource_version_uuid,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Event_ApplyOperation) Reset()         { *m = Event_ApplyOperation{} }
func (m *Event_ApplyOperation) String() string { return proto.CompactTextString(m) }
func (*Event_ApplyOperation) ProtoMessage()    {}
func (*Event_ApplyOperation) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 1}
}

func (m *Event_ApplyOperation) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event_ApplyOperation.Unmarshal(m, b)
}
func (m *Event_ApplyOperation) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event_ApplyOperation.Marshal(b, m, deterministic)
}
func (m *Event_ApplyOperation) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event_ApplyOperation.Merge(m, src)
}
func (m *Event_ApplyOperation) XXX_Size() int {
	return xxx_messageInfo_Event_ApplyOperation.Size(m)
}
func (m *Event_ApplyOperation) XXX_DiscardUnknown() {
	xxx_messageInfo_Event_ApplyOperation.DiscardUnknown(m)
}

var xxx_messageInfo_Event_ApplyOperation proto.InternalMessageInfo

func (m *Event_ApplyOperation) GetFrameworkId() *v1.FrameworkID {
	if m != nil {
		return m.FrameworkId
	}
	return nil
}

func (m *Event_ApplyOperation) GetInfo() *v1.Offer_Operation {
	if m != nil {
		return m.Info
	}
	return nil
}

func (m *Event_ApplyOperation) GetOperationUuid() *v1.UUID {
	if m != nil {
		return m.OperationUuid
	}
	return nil
}

func (m *Event_ApplyOperation) GetResourceVersionUuid() *v1.UUID {
	if m != nil {
		return m.ResourceVersionUuid
	}
	return nil
}

// Received when the master wants to launch a task using resources
// of this resource provider.
type Event_PublishResources struct {
	Uuid                 *v1.UUID       `protobuf:"bytes,1,req,name=uuid" json:"uuid,omitempty"`
	Resources            []*v1.Resource `protobuf:"bytes,2,rep,name=resources" json:"resources,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *Event_PublishResources) Reset()         { *m = Event_PublishResources{} }
func (m *Event_PublishResources) String() string { return proto.CompactTextString(m) }
func (*Event_PublishResources) ProtoMessage()    {}
func (*Event_PublishResources) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 2}
}

func (m *Event_PublishResources) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event_PublishResources.Unmarshal(m, b)
}
func (m *Event_PublishResources) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event_PublishResources.Marshal(b, m, deterministic)
}
func (m *Event_PublishResources) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event_PublishResources.Merge(m, src)
}
func (m *Event_PublishResources) XXX_Size() int {
	return xxx_messageInfo_Event_PublishResources.Size(m)
}
func (m *Event_PublishResources) XXX_DiscardUnknown() {
	xxx_messageInfo_Event_PublishResources.DiscardUnknown(m)
}

var xxx_messageInfo_Event_PublishResources proto.InternalMessageInfo

func (m *Event_PublishResources) GetUuid() *v1.UUID {
	if m != nil {
		return m.Uuid
	}
	return nil
}

func (m *Event_PublishResources) GetResources() []*v1.Resource {
	if m != nil {
		return m.Resources
	}
	return nil
}

// Received when an operation status update is being acknowledged.
// Acknowledgements may be generated either by a framework or by the master.
type Event_AcknowledgeOperationStatus struct {
	// The UUID from the `OperationStatus` being acknowledged.
	StatusUuid *v1.UUID `protobuf:"bytes,1,req,name=status_uuid,json=statusUuid" json:"status_uuid,omitempty"`
	// The UUID from the relevant `Operation`.
	OperationUuid        *v1.UUID `protobuf:"bytes,2,req,name=operation_uuid,json=operationUuid" json:"operation_uuid,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Event_AcknowledgeOperationStatus) Reset()         { *m = Event_AcknowledgeOperationStatus{} }
func (m *Event_AcknowledgeOperationStatus) String() string { return proto.CompactTextString(m) }
func (*Event_AcknowledgeOperationStatus) ProtoMessage()    {}
func (*Event_AcknowledgeOperationStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 3}
}

func (m *Event_AcknowledgeOperationStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event_AcknowledgeOperationStatus.Unmarshal(m, b)
}
func (m *Event_AcknowledgeOperationStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event_AcknowledgeOperationStatus.Marshal(b, m, deterministic)
}
func (m *Event_AcknowledgeOperationStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event_AcknowledgeOperationStatus.Merge(m, src)
}
func (m *Event_AcknowledgeOperationStatus) XXX_Size() int {
	return xxx_messageInfo_Event_AcknowledgeOperationStatus.Size(m)
}
func (m *Event_AcknowledgeOperationStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_Event_AcknowledgeOperationStatus.DiscardUnknown(m)
}

var xxx_messageInfo_Event_AcknowledgeOperationStatus proto.InternalMessageInfo

func (m *Event_AcknowledgeOperationStatus) GetStatusUuid() *v1.UUID {
	if m != nil {
		return m.StatusUuid
	}
	return nil
}

func (m *Event_AcknowledgeOperationStatus) GetOperationUuid() *v1.UUID {
	if m != nil {
		return m.OperationUuid
	}
	return nil
}

// Received when the resource provider manager wants to reconcile its view of
// the resource provider's operation state. The resource provider should
// generate operation status updates for any operation UUIDs in this message
// which are unknown to the resource provider.
type Event_ReconcileOperations struct {
	OperationUuids       []*v1.UUID `protobuf:"bytes,1,rep,name=operation_uuids,json=operationUuids" json:"operation_uuids,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *Event_ReconcileOperations) Reset()         { *m = Event_ReconcileOperations{} }
func (m *Event_ReconcileOperations) String() string { return proto.CompactTextString(m) }
func (*Event_ReconcileOperations) ProtoMessage()    {}
func (*Event_ReconcileOperations) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{0, 4}
}

func (m *Event_ReconcileOperations) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event_ReconcileOperations.Unmarshal(m, b)
}
func (m *Event_ReconcileOperations) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event_ReconcileOperations.Marshal(b, m, deterministic)
}
func (m *Event_ReconcileOperations) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event_ReconcileOperations.Merge(m, src)
}
func (m *Event_ReconcileOperations) XXX_Size() int {
	return xxx_messageInfo_Event_ReconcileOperations.Size(m)
}
func (m *Event_ReconcileOperations) XXX_DiscardUnknown() {
	xxx_messageInfo_Event_ReconcileOperations.DiscardUnknown(m)
}

var xxx_messageInfo_Event_ReconcileOperations proto.InternalMessageInfo

func (m *Event_ReconcileOperations) GetOperationUuids() []*v1.UUID {
	if m != nil {
		return m.OperationUuids
	}
	return nil
}

// NOTE: For the time being, this API is subject to change and the related
// feature is experimental.
type Call struct {
	// Identifies who generated this call.
	// The 'ResourceProviderManager' assigns a resource provider ID when
	// a new resource provider subscribes for the first time. Once
	// assigned, the resource provider must set the 'resource_provider_id'
	// here and within its 'ResourceProviderInfo' (in any further
	// 'SUBSCRIBE' calls). This allows the 'ResourceProviderManager' to
	// identify a resource provider correctly across disconnections,
	// failovers, etc.
	ResourceProviderId           *v1.ResourceProviderID             `protobuf:"bytes,1,opt,name=resource_provider_id,json=resourceProviderId" json:"resource_provider_id,omitempty"`
	Type                         *Call_Type                         `protobuf:"varint,2,opt,name=type,enum=mesos.v1.resource_provider.Call_Type" json:"type,omitempty"`
	Subscribe                    *Call_Subscribe                    `protobuf:"bytes,3,opt,name=subscribe" json:"subscribe,omitempty"`
	UpdateOperationStatus        *Call_UpdateOperationStatus        `protobuf:"bytes,4,opt,name=update_operation_status,json=updateOperationStatus" json:"update_operation_status,omitempty"`
	UpdateState                  *Call_UpdateState                  `protobuf:"bytes,5,opt,name=update_state,json=updateState" json:"update_state,omitempty"`
	UpdatePublishResourcesStatus *Call_UpdatePublishResourcesStatus `protobuf:"bytes,6,opt,name=update_publish_resources_status,json=updatePublishResourcesStatus" json:"update_publish_resources_status,omitempty"`
	XXX_NoUnkeyedLiteral         struct{}                           `json:"-"`
	XXX_unrecognized             []byte                             `json:"-"`
	XXX_sizecache                int32                              `json:"-"`
}

func (m *Call) Reset()         { *m = Call{} }
func (m *Call) String() string { return proto.CompactTextString(m) }
func (*Call) ProtoMessage()    {}
func (*Call) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1}
}

func (m *Call) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Call.Unmarshal(m, b)
}
func (m *Call) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Call.Marshal(b, m, deterministic)
}
func (m *Call) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Call.Merge(m, src)
}
func (m *Call) XXX_Size() int {
	return xxx_messageInfo_Call.Size(m)
}
func (m *Call) XXX_DiscardUnknown() {
	xxx_messageInfo_Call.DiscardUnknown(m)
}

var xxx_messageInfo_Call proto.InternalMessageInfo

func (m *Call) GetResourceProviderId() *v1.ResourceProviderID {
	if m != nil {
		return m.ResourceProviderId
	}
	return nil
}

func (m *Call) GetType() Call_Type {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return Call_UNKNOWN
}

func (m *Call) GetSubscribe() *Call_Subscribe {
	if m != nil {
		return m.Subscribe
	}
	return nil
}

func (m *Call) GetUpdateOperationStatus() *Call_UpdateOperationStatus {
	if m != nil {
		return m.UpdateOperationStatus
	}
	return nil
}

func (m *Call) GetUpdateState() *Call_UpdateState {
	if m != nil {
		return m.UpdateState
	}
	return nil
}

func (m *Call) GetUpdatePublishResourcesStatus() *Call_UpdatePublishResourcesStatus {
	if m != nil {
		return m.UpdatePublishResourcesStatus
	}
	return nil
}

// Request to subscribe with the master.
type Call_Subscribe struct {
	ResourceProviderInfo *v1.ResourceProviderInfo `protobuf:"bytes,1,req,name=resource_provider_info,json=resourceProviderInfo" json:"resource_provider_info,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *Call_Subscribe) Reset()         { *m = Call_Subscribe{} }
func (m *Call_Subscribe) String() string { return proto.CompactTextString(m) }
func (*Call_Subscribe) ProtoMessage()    {}
func (*Call_Subscribe) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 0}
}

func (m *Call_Subscribe) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Call_Subscribe.Unmarshal(m, b)
}
func (m *Call_Subscribe) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Call_Subscribe.Marshal(b, m, deterministic)
}
func (m *Call_Subscribe) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Call_Subscribe.Merge(m, src)
}
func (m *Call_Subscribe) XXX_Size() int {
	return xxx_messageInfo_Call_Subscribe.Size(m)
}
func (m *Call_Subscribe) XXX_DiscardUnknown() {
	xxx_messageInfo_Call_Subscribe.DiscardUnknown(m)
}

var xxx_messageInfo_Call_Subscribe proto.InternalMessageInfo

func (m *Call_Subscribe) GetResourceProviderInfo() *v1.ResourceProviderInfo {
	if m != nil {
		return m.ResourceProviderInfo
	}
	return nil
}

// Notify the master about the status of an operation.
type Call_UpdateOperationStatus struct {
	FrameworkId  *v1.FrameworkID     `protobuf:"bytes,1,opt,name=framework_id,json=frameworkId" json:"framework_id,omitempty"`
	Status       *v1.OperationStatus `protobuf:"bytes,2,req,name=status" json:"status,omitempty"`
	LatestStatus *v1.OperationStatus `protobuf:"bytes,3,opt,name=latest_status,json=latestStatus" json:"latest_status,omitempty"`
	// This is the internal UUID for the operation, which is kept
	// independently from the framework-specified operation ID, which
	// is optional.
	OperationUuid        *v1.UUID `protobuf:"bytes,4,req,name=operation_uuid,json=operationUuid" json:"operation_uuid,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Call_UpdateOperationStatus) Reset()         { *m = Call_UpdateOperationStatus{} }
func (m *Call_UpdateOperationStatus) String() string { return proto.CompactTextString(m) }
func (*Call_UpdateOperationStatus) ProtoMessage()    {}
func (*Call_UpdateOperationStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 1}
}

func (m *Call_UpdateOperationStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Call_UpdateOperationStatus.Unmarshal(m, b)
}
func (m *Call_UpdateOperationStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Call_UpdateOperationStatus.Marshal(b, m, deterministic)
}
func (m *Call_UpdateOperationStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Call_UpdateOperationStatus.Merge(m, src)
}
func (m *Call_UpdateOperationStatus) XXX_Size() int {
	return xxx_messageInfo_Call_UpdateOperationStatus.Size(m)
}
func (m *Call_UpdateOperationStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_Call_UpdateOperationStatus.DiscardUnknown(m)
}

var xxx_messageInfo_Call_UpdateOperationStatus proto.InternalMessageInfo

func (m *Call_UpdateOperationStatus) GetFrameworkId() *v1.FrameworkID {
	if m != nil {
		return m.FrameworkId
	}
	return nil
}

func (m *Call_UpdateOperationStatus) GetStatus() *v1.OperationStatus {
	if m != nil {
		return m.Status
	}
	return nil
}

func (m *Call_UpdateOperationStatus) GetLatestStatus() *v1.OperationStatus {
	if m != nil {
		return m.LatestStatus
	}
	return nil
}

func (m *Call_UpdateOperationStatus) GetOperationUuid() *v1.UUID {
	if m != nil {
		return m.OperationUuid
	}
	return nil
}

// Notify the master about the total resources and pending operations.
type Call_UpdateState struct {
	// This includes pending operations and those that have
	// unacknowledged statuses.
	Operations []*v1.Operation `protobuf:"bytes,1,rep,name=operations" json:"operations,omitempty"`
	// The total resources provided by this resource provider.
	Resources []*v1.Resource `protobuf:"bytes,2,rep,name=resources" json:"resources,omitempty"`
	// Used to establish the relationship between the operation and
	// the resources that the operation is operating on. Each resource
	// provider will keep a resource version UUID, and change it when
	// it believes that the resources from this resource provider are
	// out of sync from the master's view. The master will keep track
	// of the last known resource version UUID for each resource
	// provider, and attach the resource version UUID in each
	// operation it sends out. The resource provider should reject
	// operations that have a different resource version UUID than
	// that it maintains, because this means the operation is
	// operating on resources that might have already been
	// invalidated.
	ResourceVersionUuid  *v1.UUID `protobuf:"bytes,3,req,name=resource_version_uuid,json=resourceVersionUuid" json:"resource_version_uuid,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Call_UpdateState) Reset()         { *m = Call_UpdateState{} }
func (m *Call_UpdateState) String() string { return proto.CompactTextString(m) }
func (*Call_UpdateState) ProtoMessage()    {}
func (*Call_UpdateState) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 2}
}

func (m *Call_UpdateState) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Call_UpdateState.Unmarshal(m, b)
}
func (m *Call_UpdateState) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Call_UpdateState.Marshal(b, m, deterministic)
}
func (m *Call_UpdateState) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Call_UpdateState.Merge(m, src)
}
func (m *Call_UpdateState) XXX_Size() int {
	return xxx_messageInfo_Call_UpdateState.Size(m)
}
func (m *Call_UpdateState) XXX_DiscardUnknown() {
	xxx_messageInfo_Call_UpdateState.DiscardUnknown(m)
}

var xxx_messageInfo_Call_UpdateState proto.InternalMessageInfo

func (m *Call_UpdateState) GetOperations() []*v1.Operation {
	if m != nil {
		return m.Operations
	}
	return nil
}

func (m *Call_UpdateState) GetResources() []*v1.Resource {
	if m != nil {
		return m.Resources
	}
	return nil
}

func (m *Call_UpdateState) GetResourceVersionUuid() *v1.UUID {
	if m != nil {
		return m.ResourceVersionUuid
	}
	return nil
}

type Call_UpdatePublishResourcesStatus struct {
	Uuid                 *v1.UUID                                  `protobuf:"bytes,1,req,name=uuid" json:"uuid,omitempty"`
	Status               *Call_UpdatePublishResourcesStatus_Status `protobuf:"varint,2,req,name=status,enum=mesos.v1.resource_provider.Call_UpdatePublishResourcesStatus_Status" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                                  `json:"-"`
	XXX_unrecognized     []byte                                    `json:"-"`
	XXX_sizecache        int32                                     `json:"-"`
}

func (m *Call_UpdatePublishResourcesStatus) Reset()         { *m = Call_UpdatePublishResourcesStatus{} }
func (m *Call_UpdatePublishResourcesStatus) String() string { return proto.CompactTextString(m) }
func (*Call_UpdatePublishResourcesStatus) ProtoMessage()    {}
func (*Call_UpdatePublishResourcesStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_16a826288d8f7b30, []int{1, 3}
}

func (m *Call_UpdatePublishResourcesStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Call_UpdatePublishResourcesStatus.Unmarshal(m, b)
}
func (m *Call_UpdatePublishResourcesStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Call_UpdatePublishResourcesStatus.Marshal(b, m, deterministic)
}
func (m *Call_UpdatePublishResourcesStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Call_UpdatePublishResourcesStatus.Merge(m, src)
}
func (m *Call_UpdatePublishResourcesStatus) XXX_Size() int {
	return xxx_messageInfo_Call_UpdatePublishResourcesStatus.Size(m)
}
func (m *Call_UpdatePublishResourcesStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_Call_UpdatePublishResourcesStatus.DiscardUnknown(m)
}

var xxx_messageInfo_Call_UpdatePublishResourcesStatus proto.InternalMessageInfo

func (m *Call_UpdatePublishResourcesStatus) GetUuid() *v1.UUID {
	if m != nil {
		return m.Uuid
	}
	return nil
}

func (m *Call_UpdatePublishResourcesStatus) GetStatus() Call_UpdatePublishResourcesStatus_Status {
	if m != nil && m.Status != nil {
		return *m.Status
	}
	return Call_UpdatePublishResourcesStatus_UNKNOWN
}

func init() {
	proto.RegisterEnum("mesos.v1.resource_provider.Event_Type", Event_Type_name, Event_Type_value)
	proto.RegisterEnum("mesos.v1.resource_provider.Call_Type", Call_Type_name, Call_Type_value)
	proto.RegisterEnum("mesos.v1.resource_provider.Call_UpdatePublishResourcesStatus_Status", Call_UpdatePublishResourcesStatus_Status_name, Call_UpdatePublishResourcesStatus_Status_value)
	proto.RegisterType((*Event)(nil), "mesos.v1.resource_provider.Event")
	proto.RegisterType((*Event_Subscribed)(nil), "mesos.v1.resource_provider.Event.Subscribed")
	proto.RegisterType((*Event_ApplyOperation)(nil), "mesos.v1.resource_provider.Event.ApplyOperation")
	proto.RegisterType((*Event_PublishResources)(nil), "mesos.v1.resource_provider.Event.PublishResources")
	proto.RegisterType((*Event_AcknowledgeOperationStatus)(nil), "mesos.v1.resource_provider.Event.AcknowledgeOperationStatus")
	proto.RegisterType((*Event_ReconcileOperations)(nil), "mesos.v1.resource_provider.Event.ReconcileOperations")
	proto.RegisterType((*Call)(nil), "mesos.v1.resource_provider.Call")
	proto.RegisterType((*Call_Subscribe)(nil), "mesos.v1.resource_provider.Call.Subscribe")
	proto.RegisterType((*Call_UpdateOperationStatus)(nil), "mesos.v1.resource_provider.Call.UpdateOperationStatus")
	proto.RegisterType((*Call_UpdateState)(nil), "mesos.v1.resource_provider.Call.UpdateState")
	proto.RegisterType((*Call_UpdatePublishResourcesStatus)(nil), "mesos.v1.resource_provider.Call.UpdatePublishResourcesStatus")
}

func init() {
	proto.RegisterFile("mesos/v1/resource_provider/resource_provider.proto", fileDescriptor_16a826288d8f7b30)
}

var fileDescriptor_16a826288d8f7b30 = []byte{
	// 958 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x56, 0x5d, 0x8f, 0xda, 0x46,
	0x14, 0xad, 0x0d, 0x4b, 0xb5, 0xd7, 0xbb, 0xac, 0x73, 0x77, 0x69, 0x5c, 0x17, 0x35, 0x88, 0x2a,
	0xd5, 0xb6, 0x4a, 0xd9, 0x2c, 0x55, 0xfa, 0xa5, 0xa6, 0x12, 0x1f, 0x4e, 0x63, 0x2d, 0x02, 0x34,
	0xc6, 0xad, 0x22, 0x55, 0xb2, 0xbc, 0x78, 0xc8, 0xa2, 0x10, 0x6c, 0xd9, 0x98, 0x68, 0x5f, 0xfa,
	0x94, 0x3e, 0xf5, 0x0f, 0xf5, 0x37, 0xb4, 0x7f, 0xa7, 0x6f, 0x7d, 0xa9, 0x3c, 0xfe, 0x02, 0xec,
	0x05, 0xa2, 0x3c, 0xfa, 0xce, 0x3d, 0xe7, 0xce, 0xbd, 0x73, 0xe6, 0x8c, 0xa1, 0xf9, 0x9a, 0x7a,
	0xb6, 0x77, 0xb1, 0xbc, 0xbc, 0x70, 0xa9, 0x67, 0xfb, 0xee, 0x98, 0x1a, 0x8e, 0x6b, 0x2f, 0xa7,
	0x16, 0x75, 0xb3, 0x91, 0x86, 0xe3, 0xda, 0x0b, 0x1b, 0x65, 0x86, 0x69, 0x2c, 0x2f, 0x1b, 0x99,
	0x0c, 0xf9, 0x2c, 0xe1, 0x0b, 0x93, 0x18, 0xa2, 0xfe, 0x37, 0xc0, 0x81, 0xb2, 0xa4, 0xf3, 0x05,
	0xfe, 0x00, 0xc5, 0xc5, 0xad, 0x43, 0x25, 0xae, 0xc6, 0x9d, 0x97, 0x9b, 0x9f, 0x37, 0xee, 0xa6,
	0x6a, 0x30, 0x40, 0x63, 0x74, 0xeb, 0x50, 0xc2, 0x30, 0xd8, 0x03, 0xf0, 0xfc, 0x6b, 0x6f, 0xec,
	0x4e, 0xaf, 0xa9, 0x25, 0xf1, 0x35, 0xee, 0x5c, 0x68, 0x3e, 0xda, 0xcd, 0xa0, 0x25, 0x18, 0xb2,
	0x82, 0xc7, 0x17, 0x70, 0x62, 0x3a, 0xce, 0xec, 0xd6, 0xb0, 0x1d, 0xea, 0x9a, 0x8b, 0xa9, 0x3d,
	0x97, 0x0a, 0x8c, 0xf2, 0xf1, 0x6e, 0xca, 0x56, 0x00, 0x1c, 0xc4, 0x38, 0x52, 0x36, 0xd7, 0xbe,
	0xd1, 0x80, 0x7b, 0x8e, 0x7f, 0x3d, 0x9b, 0x7a, 0x37, 0x46, 0xcc, 0xe0, 0x49, 0x45, 0x46, 0xde,
	0xdc, 0x4d, 0x3e, 0x0c, 0xa1, 0x24, 0x46, 0x12, 0xd1, 0xd9, 0x88, 0xe0, 0xef, 0x50, 0x35, 0xc7,
	0xaf, 0xe6, 0xf6, 0x9b, 0x19, 0xb5, 0x5e, 0xd2, 0xb4, 0x03, 0xc3, 0x5b, 0x98, 0x0b, 0xdf, 0x93,
	0x0e, 0x58, 0xad, 0x1f, 0xf7, 0x68, 0x24, 0x65, 0x49, 0xb6, 0xaf, 0x31, 0x0e, 0x22, 0x9b, 0x77,
	0xae, 0xe1, 0x0d, 0x9c, 0xb9, 0x74, 0x6c, 0xcf, 0xc7, 0xd3, 0xd9, 0x4a, 0x75, 0x4f, 0x2a, 0xb1,
	0xba, 0x4f, 0x76, 0xd7, 0x25, 0x31, 0x3a, 0x61, 0xf6, 0xc8, 0xa9, 0x9b, 0x0d, 0xca, 0x57, 0x00,
	0xe9, 0xf9, 0xe1, 0x53, 0x10, 0x62, 0x22, 0x63, 0x6a, 0x49, 0x5c, 0x8d, 0x3f, 0x17, 0x9a, 0xd5,
	0xb4, 0x5c, 0x3c, 0xa1, 0x61, 0x94, 0xa4, 0x76, 0x09, 0xc4, 0x00, 0xd5, 0x92, 0xff, 0xe5, 0xa0,
	0xbc, 0x7e, 0x74, 0xf8, 0x1d, 0x1c, 0x4d, 0x5c, 0xf3, 0x35, 0x7d, 0x63, 0xbb, 0xaf, 0x42, 0xca,
	0xa0, 0x83, 0x4a, 0x4a, 0xf9, 0x2c, 0x5e, 0x55, 0xbb, 0x44, 0x48, 0x52, 0x55, 0x0b, 0xbf, 0x82,
	0xe2, 0x74, 0x3e, 0xb1, 0x25, 0x9e, 0x6d, 0xe2, 0xe3, 0x14, 0x31, 0x98, 0x4c, 0xa8, 0xdb, 0x48,
	0xd5, 0xc1, 0xd2, 0xf0, 0x09, 0x94, 0xd3, 0x63, 0xf2, 0xfd, 0xa9, 0x25, 0x15, 0x18, 0xb0, 0x9c,
	0x02, 0x75, 0x5d, 0xed, 0x92, 0xe3, 0x24, 0x4b, 0xf7, 0xa7, 0x16, 0xb6, 0xa1, 0x92, 0xcc, 0x70,
	0x49, 0x5d, 0x2f, 0x41, 0x17, 0x73, 0xd1, 0xa7, 0x71, 0xf2, 0x2f, 0x61, 0x6e, 0xc0, 0x21, 0xdf,
	0x80, 0xb8, 0xa9, 0x29, 0xac, 0x43, 0x91, 0xd1, 0x70, 0xb9, 0x34, 0x6c, 0x0d, 0x1f, 0xc3, 0x61,
	0x2a, 0x5f, 0xbe, 0x56, 0x38, 0x17, 0x9a, 0x98, 0x9d, 0x35, 0x49, 0x93, 0xe4, 0xb7, 0x1c, 0xc8,
	0x77, 0x4b, 0x0a, 0x2f, 0x40, 0x08, 0x05, 0x6a, 0x6c, 0xa9, 0x0d, 0x61, 0x0a, 0xeb, 0x3e, 0x3b,
	0x34, 0x7e, 0x8f, 0xa1, 0xc9, 0x7d, 0x38, 0xcd, 0x11, 0x18, 0x7e, 0x0b, 0x27, 0xeb, 0x6c, 0x9e,
	0xc4, 0xb1, 0xae, 0x36, 0xe9, 0xca, 0x6b, 0x74, 0x5e, 0xfd, 0x4f, 0x0e, 0x8a, 0x81, 0x0f, 0xa1,
	0x00, 0x1f, 0xea, 0xfd, 0xab, 0xfe, 0xe0, 0xd7, 0xbe, 0xf8, 0x01, 0x96, 0x01, 0x34, 0xbd, 0xad,
	0x75, 0x88, 0xda, 0x56, 0xba, 0x22, 0x87, 0xa7, 0x70, 0xd2, 0x1a, 0x0e, 0x7b, 0x2f, 0x8c, 0xc1,
	0x50, 0x21, 0xad, 0x91, 0x3a, 0xe8, 0x8b, 0x3c, 0x56, 0xe0, 0xde, 0x50, 0x6f, 0xf7, 0x54, 0xed,
	0xb9, 0x41, 0x14, 0x6d, 0xa0, 0x93, 0x8e, 0xa2, 0x89, 0x05, 0xac, 0x41, 0xb5, 0xd5, 0x09, 0x88,
	0x7a, 0x4a, 0xf7, 0x67, 0x25, 0x45, 0x18, 0xda, 0xa8, 0x35, 0xd2, 0x35, 0xb1, 0x88, 0x12, 0x9c,
	0x11, 0xa5, 0x33, 0xe8, 0x77, 0xd4, 0xde, 0xca, 0xba, 0x26, 0x1e, 0xd4, 0xff, 0x10, 0xa0, 0xd8,
	0x31, 0x67, 0x33, 0xec, 0x07, 0xb7, 0x70, 0xe3, 0x7e, 0xa5, 0x1a, 0xde, 0x7e, 0x2d, 0xd0, 0xdd,
	0x8c, 0x59, 0xf8, 0x7d, 0xe4, 0xcd, 0x3c, 0xf3, 0xe6, 0x87, 0xdb, 0x6e, 0x71, 0x50, 0x7f, 0xd5,
	0x9a, 0x9f, 0xc3, 0x61, 0x62, 0xad, 0x91, 0x8d, 0x7e, 0xb9, 0x13, 0x9f, 0x5c, 0x6c, 0x92, 0x82,
	0x71, 0x0e, 0xf7, 0x7d, 0xc7, 0x32, 0x17, 0x39, 0xae, 0x16, 0x3a, 0xe8, 0x37, 0x3b, 0x79, 0x75,
	0x86, 0xdf, 0xf4, 0xb3, 0x8a, 0x9f, 0x17, 0xc6, 0x01, 0x1c, 0x45, 0xf5, 0x82, 0x2a, 0x34, 0xb2,
	0xce, 0x47, 0x7b, 0x16, 0x09, 0x48, 0x28, 0x11, 0xfc, 0xf4, 0x03, 0xdf, 0x72, 0xf0, 0x20, 0x62,
	0xcc, 0x3c, 0x02, 0x71, 0x27, 0xa1, 0x4f, 0x3e, 0xdd, 0xb3, 0xc8, 0xe6, 0xe5, 0x8d, 0x1a, 0xaa,
	0xfa, 0x5b, 0x56, 0x65, 0x13, 0x0e, 0x93, 0xf9, 0xe2, 0x08, 0x3e, 0xca, 0x51, 0x4a, 0xe0, 0x5e,
	0xe1, 0x1d, 0xfc, 0x74, 0x8b, 0x56, 0xe6, 0x13, 0x9b, 0x9c, 0xb9, 0x39, 0x51, 0xf9, 0x3f, 0x0e,
	0x2a, 0xb9, 0xb3, 0x7e, 0x0f, 0x57, 0xbd, 0x84, 0x52, 0x34, 0xa3, 0xac, 0xaf, 0x6e, 0x1c, 0x68,
	0x94, 0x88, 0x3f, 0xc1, 0xf1, 0xcc, 0x5c, 0x50, 0x6f, 0x11, 0x4f, 0x37, 0xd4, 0xdf, 0x16, 0xe4,
	0x51, 0x98, 0x1f, 0x6d, 0x36, 0x6b, 0x32, 0xc5, 0x7d, 0x4c, 0xe6, 0x2f, 0x0e, 0x84, 0x15, 0x11,
	0xe0, 0xd7, 0x00, 0x2b, 0x2f, 0x61, 0x68, 0x2c, 0xa7, 0x39, 0x7b, 0x20, 0x2b, 0x69, 0xef, 0x6e,
	0xb1, 0x77, 0x3f, 0x08, 0x85, 0xfd, 0x1f, 0x84, 0x7f, 0x38, 0xa8, 0x6e, 0x93, 0xd6, 0x5e, 0xaf,
	0xc3, 0x6f, 0x6b, 0x27, 0x55, 0x6e, 0x76, 0xdf, 0x4b, 0xcd, 0x8d, 0xf5, 0x43, 0xad, 0x7f, 0x01,
	0xa5, 0x68, 0x2f, 0x6b, 0x9e, 0x5b, 0x02, 0x7e, 0x70, 0x25, 0x72, 0x08, 0x50, 0x7a, 0xd6, 0x52,
	0x7b, 0x4a, 0x57, 0xe4, 0xeb, 0xcb, 0x3c, 0x73, 0x3e, 0x86, 0xc3, 0xc4, 0x9c, 0x45, 0x0e, 0x3f,
	0x81, 0xfb, 0xfa, 0xb0, 0xdb, 0x1a, 0xe5, 0x58, 0x2d, 0x8f, 0x22, 0x1c, 0x45, 0x8b, 0x41, 0x48,
	0x11, 0x0b, 0xf8, 0x19, 0x3c, 0x88, 0x22, 0x19, 0xf3, 0x4e, 0x1c, 0xba, 0x7d, 0x01, 0x0f, 0x6d,
	0xf7, 0x65, 0xc3, 0x74, 0xcc, 0xf1, 0x0d, 0xdd, 0xd2, 0x7c, 0xbb, 0x34, 0x0c, 0x7e, 0x82, 0xbd,
	0xff, 0x03, 0x00, 0x00, 0xff, 0xff, 0xa7, 0x86, 0x29, 0xc5, 0x6c, 0x0b, 0x00, 0x00,
}
