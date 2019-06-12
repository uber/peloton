// Code generated by protoc-gen-go. DO NOT EDIT.
// source: peloton/private/resmgr/resmgr.proto

package resmgr

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	v1 "github.com/uber/peloton/.gen/mesos/v1"
	job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	task "github.com/uber/peloton/.gen/peloton/api/v0/task"
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

//*
//  TaskType task type definition such as batch, service and infra agent.
type TaskType int32

const (
	// This is unknown type, this is also used in DequeueGangsRequest to
	// indicate that we want tasks of any task type back.
	TaskType_UNKNOWN TaskType = 0
	// Normal batch task
	TaskType_BATCH TaskType = 1
	// STATELESS task which is long running and will be restarted upon failures.
	TaskType_STATELESS TaskType = 2
	// STATEFUL task which is using persistent volume and is long running
	TaskType_STATEFUL TaskType = 3
	// DAEMON task which has one instance running on each host for infra
	// agents like muttley, m3collector etc.
	TaskType_DAEMON TaskType = 4
)

var TaskType_name = map[int32]string{
	0: "UNKNOWN",
	1: "BATCH",
	2: "STATELESS",
	3: "STATEFUL",
	4: "DAEMON",
}

var TaskType_value = map[string]int32{
	"UNKNOWN":   0,
	"BATCH":     1,
	"STATELESS": 2,
	"STATEFUL":  3,
	"DAEMON":    4,
}

func (x TaskType) String() string {
	return proto.EnumName(TaskType_name, int32(x))
}

func (TaskType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{0}
}

//
//  The reason for choosing the task for preemption
type PreemptionReason int32

const (
	// Reserved for compatibility
	PreemptionReason_PREEMPTION_REASON_UNKNOWN PreemptionReason = 0
	// Resource Preemption
	PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES PreemptionReason = 1
	// Host maintenance
	PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE PreemptionReason = 2
)

var PreemptionReason_name = map[int32]string{
	0: "PREEMPTION_REASON_UNKNOWN",
	1: "PREEMPTION_REASON_REVOKE_RESOURCES",
	2: "PREEMPTION_REASON_HOST_MAINTENANCE",
}

var PreemptionReason_value = map[string]int32{
	"PREEMPTION_REASON_UNKNOWN":          0,
	"PREEMPTION_REASON_REVOKE_RESOURCES": 1,
	"PREEMPTION_REASON_HOST_MAINTENANCE": 2,
}

func (x PreemptionReason) String() string {
	return proto.EnumName(PreemptionReason_name, int32(x))
}

func (PreemptionReason) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{1}
}

//*
//  Task describes a task instance at Resource Manager layer. Only
//  includes the minimal set of fields required for Resource Manager
//  and Placement Engine, such as resource config, constraint etc.
type Task struct {
	// Name of the task
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// The unique ID of the task
	Id *peloton.TaskID `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	// The Job ID of the task for use cases like gang scheduling
	JobId *peloton.JobID `protobuf:"bytes,3,opt,name=jobId,proto3" json:"jobId,omitempty"`
	// The mesos task ID of the task
	TaskId *v1.TaskID `protobuf:"bytes,4,opt,name=taskId,proto3" json:"taskId,omitempty"`
	// Resource config of the task
	Resource *task.ResourceConfig `protobuf:"bytes,5,opt,name=resource,proto3" json:"resource,omitempty"`
	//
	// Priority of a task. Higher value takes priority over lower value
	// when making scheduling decisions as well as preemption decisions
	//
	Priority uint32 `protobuf:"varint,6,opt,name=priority,proto3" json:"priority,omitempty"`
	//
	// Whether the task is preemptible. If a task is not preemptible, then
	// it will have to be launched using reserved resources.
	//
	Preemptible bool `protobuf:"varint,7,opt,name=preemptible,proto3" json:"preemptible,omitempty"`
	// List of user-defined labels for the task, these are used to enforce
	// the constraint. These are copied from the TaskConfig.
	Labels *v1.Labels `protobuf:"bytes,8,opt,name=labels,proto3" json:"labels,omitempty"`
	// Constraint on the labels of the host or tasks on the host that this
	// task should run on. This is copied from the TaskConfig.
	Constraint *task.Constraint `protobuf:"bytes,9,opt,name=constraint,proto3" json:"constraint,omitempty"`
	// Type of the Task
	Type TaskType `protobuf:"varint,10,opt,name=type,proto3,enum=peloton.private.resmgr.TaskType" json:"type,omitempty"`
	// Number of dynamic ports
	NumPorts uint32 `protobuf:"varint,11,opt,name=numPorts,proto3" json:"numPorts,omitempty"`
	// Minimum number of running instances.  Value > 1 indicates task is in
	// scheduling gang of that size; task instanceID is in [0..minInstances-1].
	// If value <= 1, task is not in scheduling gang and is scheduled singly.
	MinInstances uint32 `protobuf:"varint,12,opt,name=minInstances,proto3" json:"minInstances,omitempty"`
	// Hostname of the host on which the task is running on.
	Hostname string `protobuf:"bytes,13,opt,name=hostname,proto3" json:"hostname,omitempty"`
	// Whether this is a controler task. A controller is a special batch task
	// which controls other tasks inside a job. E.g. spark driver tasks in a spark
	// job will be a controller task.
	Controller bool `protobuf:"varint,14,opt,name=controller,proto3" json:"controller,omitempty"`
	// This is the timeout for the placement, it needs to be set for different
	// timeout value for each placement iteration. Also it will also be used
	// for communicating between placement engine in case of Host reservation.
	PlacementTimeoutSeconds float64 `protobuf:"fixed64,15,opt,name=placementTimeoutSeconds,proto3" json:"placementTimeoutSeconds,omitempty"`
	// Retry count for how many cycles this task is failed to be Placed. This is
	// needed for calculating next backoff period and decide when we need
	// to do Host reservation.
	PlacementRetryCount float64 `protobuf:"fixed64,16,opt,name=placementRetryCount,proto3" json:"placementRetryCount,omitempty"`
	// Whether the task is revocable. If true, then it will be launched
	// with usage slack resources.
	// Revocable tasks will be killed by QoS controller, if resources
	// are required by tasks to which initial allocation was done.
	Revocable bool `protobuf:"varint,17,opt,name=revocable,proto3" json:"revocable,omitempty"`
	// The name of the host where the instance should be running on upon restart.
	// It is used for best effort in-place update/restart.
	// When this field is set upon enqueuegang, the task would directly move to
	// ready queue.
	DesiredHost string `protobuf:"bytes,18,opt,name=desiredHost,proto3" json:"desiredHost,omitempty"`
	// Number of attempts this task has been retried for placement in a cycle.
	PlacementAttemptCount float64 `protobuf:"fixed64,19,opt,name=placementAttemptCount,proto3" json:"placementAttemptCount,omitempty"`
	// Flag to indicate the task is ready for host reservation
	ReadyForHostReservation bool `protobuf:"varint,20,opt,name=readyForHostReservation,proto3" json:"readyForHostReservation,omitempty"`
	// Preference for placing tasks of the job on hosts.
	PlacementStrategy    job.PlacementStrategy `protobuf:"varint,21,opt,name=placementStrategy,proto3,enum=peloton.api.v0.job.PlacementStrategy" json:"placementStrategy,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *Task) Reset()         { *m = Task{} }
func (m *Task) String() string { return proto.CompactTextString(m) }
func (*Task) ProtoMessage()    {}
func (*Task) Descriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{0}
}

func (m *Task) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Task.Unmarshal(m, b)
}
func (m *Task) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Task.Marshal(b, m, deterministic)
}
func (m *Task) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Task.Merge(m, src)
}
func (m *Task) XXX_Size() int {
	return xxx_messageInfo_Task.Size(m)
}
func (m *Task) XXX_DiscardUnknown() {
	xxx_messageInfo_Task.DiscardUnknown(m)
}

var xxx_messageInfo_Task proto.InternalMessageInfo

func (m *Task) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Task) GetId() *peloton.TaskID {
	if m != nil {
		return m.Id
	}
	return nil
}

func (m *Task) GetJobId() *peloton.JobID {
	if m != nil {
		return m.JobId
	}
	return nil
}

func (m *Task) GetTaskId() *v1.TaskID {
	if m != nil {
		return m.TaskId
	}
	return nil
}

func (m *Task) GetResource() *task.ResourceConfig {
	if m != nil {
		return m.Resource
	}
	return nil
}

func (m *Task) GetPriority() uint32 {
	if m != nil {
		return m.Priority
	}
	return 0
}

func (m *Task) GetPreemptible() bool {
	if m != nil {
		return m.Preemptible
	}
	return false
}

func (m *Task) GetLabels() *v1.Labels {
	if m != nil {
		return m.Labels
	}
	return nil
}

func (m *Task) GetConstraint() *task.Constraint {
	if m != nil {
		return m.Constraint
	}
	return nil
}

func (m *Task) GetType() TaskType {
	if m != nil {
		return m.Type
	}
	return TaskType_UNKNOWN
}

func (m *Task) GetNumPorts() uint32 {
	if m != nil {
		return m.NumPorts
	}
	return 0
}

func (m *Task) GetMinInstances() uint32 {
	if m != nil {
		return m.MinInstances
	}
	return 0
}

func (m *Task) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

func (m *Task) GetController() bool {
	if m != nil {
		return m.Controller
	}
	return false
}

func (m *Task) GetPlacementTimeoutSeconds() float64 {
	if m != nil {
		return m.PlacementTimeoutSeconds
	}
	return 0
}

func (m *Task) GetPlacementRetryCount() float64 {
	if m != nil {
		return m.PlacementRetryCount
	}
	return 0
}

func (m *Task) GetRevocable() bool {
	if m != nil {
		return m.Revocable
	}
	return false
}

func (m *Task) GetDesiredHost() string {
	if m != nil {
		return m.DesiredHost
	}
	return ""
}

func (m *Task) GetPlacementAttemptCount() float64 {
	if m != nil {
		return m.PlacementAttemptCount
	}
	return 0
}

func (m *Task) GetReadyForHostReservation() bool {
	if m != nil {
		return m.ReadyForHostReservation
	}
	return false
}

func (m *Task) GetPlacementStrategy() job.PlacementStrategy {
	if m != nil {
		return m.PlacementStrategy
	}
	return job.PlacementStrategy_PLACEMENT_STRATEGY_INVALID
}

//*
//  Placement describes the mapping of a list of tasks to a host
//  so that Job Manager can launch the tasks on the host.
type Placement struct {
	// The list of tasks to be placed
	// DEPRECATED in favor of taskIDs
	Tasks []*peloton.TaskID `protobuf:"bytes,1,rep,name=tasks,proto3" json:"tasks,omitempty"` // Deprecated: Do not use.
	// The name of the host where the tasks are placed
	Hostname string `protobuf:"bytes,2,opt,name=hostname,proto3" json:"hostname,omitempty"`
	// The Mesos agent ID of the host where the tasks are placed
	AgentId *v1.AgentID `protobuf:"bytes,3,opt,name=agentId,proto3" json:"agentId,omitempty"`
	// The list of Mesos offers of the placed tasks
	// DEPRECATED: see `HostOfferID`
	OfferIds []*v1.OfferID `protobuf:"bytes,4,rep,name=offerIds,proto3" json:"offerIds,omitempty"` // Deprecated: Do not use.
	// The list of allocated ports which should be sufficient for all placed tasks
	Ports []uint32 `protobuf:"varint,5,rep,packed,name=ports,proto3" json:"ports,omitempty"`
	// Type of the tasks in the placement. Note all tasks must belong to same type.
	// By default the type is batch task.
	Type TaskType `protobuf:"varint,6,opt,name=type,proto3,enum=peloton.private.resmgr.TaskType" json:"type,omitempty"`
	// The unique offer id of the offers on the host where the tasks are placed
	HostOfferID *peloton.HostOfferID `protobuf:"bytes,7,opt,name=hostOfferID,proto3" json:"hostOfferID,omitempty"`
	// The list of tasks to be placed
	TaskIDs              []*Placement_Task `protobuf:"bytes,8,rep,name=taskIDs,proto3" json:"taskIDs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Placement) Reset()         { *m = Placement{} }
func (m *Placement) String() string { return proto.CompactTextString(m) }
func (*Placement) ProtoMessage()    {}
func (*Placement) Descriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{1}
}

func (m *Placement) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Placement.Unmarshal(m, b)
}
func (m *Placement) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Placement.Marshal(b, m, deterministic)
}
func (m *Placement) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Placement.Merge(m, src)
}
func (m *Placement) XXX_Size() int {
	return xxx_messageInfo_Placement.Size(m)
}
func (m *Placement) XXX_DiscardUnknown() {
	xxx_messageInfo_Placement.DiscardUnknown(m)
}

var xxx_messageInfo_Placement proto.InternalMessageInfo

// Deprecated: Do not use.
func (m *Placement) GetTasks() []*peloton.TaskID {
	if m != nil {
		return m.Tasks
	}
	return nil
}

func (m *Placement) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

func (m *Placement) GetAgentId() *v1.AgentID {
	if m != nil {
		return m.AgentId
	}
	return nil
}

// Deprecated: Do not use.
func (m *Placement) GetOfferIds() []*v1.OfferID {
	if m != nil {
		return m.OfferIds
	}
	return nil
}

func (m *Placement) GetPorts() []uint32 {
	if m != nil {
		return m.Ports
	}
	return nil
}

func (m *Placement) GetType() TaskType {
	if m != nil {
		return m.Type
	}
	return TaskType_UNKNOWN
}

func (m *Placement) GetHostOfferID() *peloton.HostOfferID {
	if m != nil {
		return m.HostOfferID
	}
	return nil
}

func (m *Placement) GetTaskIDs() []*Placement_Task {
	if m != nil {
		return m.TaskIDs
	}
	return nil
}

// Task to be placed
type Placement_Task struct {
	PelotonTaskID        *peloton.TaskID `protobuf:"bytes,1,opt,name=pelotonTaskID,proto3" json:"pelotonTaskID,omitempty"`
	MesosTaskID          *v1.TaskID      `protobuf:"bytes,2,opt,name=mesosTaskID,proto3" json:"mesosTaskID,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *Placement_Task) Reset()         { *m = Placement_Task{} }
func (m *Placement_Task) String() string { return proto.CompactTextString(m) }
func (*Placement_Task) ProtoMessage()    {}
func (*Placement_Task) Descriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{1, 0}
}

func (m *Placement_Task) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Placement_Task.Unmarshal(m, b)
}
func (m *Placement_Task) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Placement_Task.Marshal(b, m, deterministic)
}
func (m *Placement_Task) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Placement_Task.Merge(m, src)
}
func (m *Placement_Task) XXX_Size() int {
	return xxx_messageInfo_Placement_Task.Size(m)
}
func (m *Placement_Task) XXX_DiscardUnknown() {
	xxx_messageInfo_Placement_Task.DiscardUnknown(m)
}

var xxx_messageInfo_Placement_Task proto.InternalMessageInfo

func (m *Placement_Task) GetPelotonTaskID() *peloton.TaskID {
	if m != nil {
		return m.PelotonTaskID
	}
	return nil
}

func (m *Placement_Task) GetMesosTaskID() *v1.TaskID {
	if m != nil {
		return m.MesosTaskID
	}
	return nil
}

//
//  PreemptionCandidate represents a task which has been chosen to be preempted
type PreemptionCandidate struct {
	// The unique ID of the task
	Id *peloton.TaskID `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The reason for choosing the task for preemption
	Reason               PreemptionReason `protobuf:"varint,2,opt,name=reason,proto3,enum=peloton.private.resmgr.PreemptionReason" json:"reason,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *PreemptionCandidate) Reset()         { *m = PreemptionCandidate{} }
func (m *PreemptionCandidate) String() string { return proto.CompactTextString(m) }
func (*PreemptionCandidate) ProtoMessage()    {}
func (*PreemptionCandidate) Descriptor() ([]byte, []int) {
	return fileDescriptor_e205eaf27728ddc1, []int{2}
}

func (m *PreemptionCandidate) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PreemptionCandidate.Unmarshal(m, b)
}
func (m *PreemptionCandidate) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PreemptionCandidate.Marshal(b, m, deterministic)
}
func (m *PreemptionCandidate) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PreemptionCandidate.Merge(m, src)
}
func (m *PreemptionCandidate) XXX_Size() int {
	return xxx_messageInfo_PreemptionCandidate.Size(m)
}
func (m *PreemptionCandidate) XXX_DiscardUnknown() {
	xxx_messageInfo_PreemptionCandidate.DiscardUnknown(m)
}

var xxx_messageInfo_PreemptionCandidate proto.InternalMessageInfo

func (m *PreemptionCandidate) GetId() *peloton.TaskID {
	if m != nil {
		return m.Id
	}
	return nil
}

func (m *PreemptionCandidate) GetReason() PreemptionReason {
	if m != nil {
		return m.Reason
	}
	return PreemptionReason_PREEMPTION_REASON_UNKNOWN
}

func init() {
	proto.RegisterEnum("peloton.private.resmgr.TaskType", TaskType_name, TaskType_value)
	proto.RegisterEnum("peloton.private.resmgr.PreemptionReason", PreemptionReason_name, PreemptionReason_value)
	proto.RegisterType((*Task)(nil), "peloton.private.resmgr.Task")
	proto.RegisterType((*Placement)(nil), "peloton.private.resmgr.Placement")
	proto.RegisterType((*Placement_Task)(nil), "peloton.private.resmgr.Placement.Task")
	proto.RegisterType((*PreemptionCandidate)(nil), "peloton.private.resmgr.PreemptionCandidate")
}

func init() {
	proto.RegisterFile("peloton/private/resmgr/resmgr.proto", fileDescriptor_e205eaf27728ddc1)
}

var fileDescriptor_e205eaf27728ddc1 = []byte{
	// 898 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x55, 0xdd, 0x6e, 0x1a, 0x47,
	0x14, 0xee, 0x62, 0x7e, 0x0f, 0xc6, 0x5d, 0x8f, 0x9d, 0x74, 0x6a, 0x25, 0xee, 0x8a, 0xa8, 0x11,
	0x4a, 0x25, 0xb0, 0x9d, 0x5c, 0xe4, 0x2e, 0xc1, 0xb0, 0x91, 0xa9, 0xed, 0x05, 0xcd, 0xae, 0x5b,
	0xa9, 0x37, 0xd6, 0xc0, 0x8e, 0xdd, 0x4d, 0x60, 0x67, 0x35, 0x33, 0x46, 0xe2, 0xae, 0xbd, 0xea,
	0x33, 0xf4, 0x45, 0xfa, 0x7c, 0xd5, 0xce, 0xfe, 0x80, 0x09, 0xc8, 0xed, 0x85, 0x0d, 0x67, 0xbe,
	0xef, 0x3b, 0x3f, 0x73, 0xce, 0x1c, 0xe0, 0x55, 0xc4, 0xa6, 0x5c, 0xf1, 0xb0, 0x13, 0x89, 0x60,
	0x4e, 0x15, 0xeb, 0x08, 0x26, 0x67, 0xf7, 0x22, 0xfd, 0x68, 0x47, 0x82, 0x2b, 0x8e, 0x9e, 0xa7,
	0xa4, 0x76, 0x4a, 0x6a, 0x27, 0xe8, 0xd1, 0xe1, 0x8c, 0x49, 0x2e, 0x3b, 0xf3, 0xd3, 0x8e, 0xfe,
	0x92, 0xb0, 0x8f, 0x5e, 0x64, 0x2e, 0x69, 0x14, 0x74, 0xe6, 0x27, 0x9d, 0xcf, 0x7c, 0x1c, 0xff,
	0x6d, 0x41, 0x97, 0xae, 0x63, 0xf4, 0x78, 0x0d, 0x55, 0x54, 0x7e, 0xd1, 0xff, 0x12, 0xbc, 0xf9,
	0x4f, 0x05, 0x8a, 0x1e, 0x95, 0x5f, 0x10, 0x82, 0x62, 0x48, 0x67, 0x0c, 0x1b, 0x96, 0xd1, 0xaa,
	0x11, 0xfd, 0x1d, 0xb5, 0xa1, 0x10, 0xf8, 0xb8, 0x60, 0x19, 0xad, 0xfa, 0xd9, 0x71, 0x3b, 0x73,
	0x4c, 0xa3, 0xa0, 0x3d, 0x3f, 0xc9, 0xcd, 0x58, 0x3d, 0xe8, 0x93, 0x42, 0xe0, 0xa3, 0xb7, 0x50,
	0xfa, 0xcc, 0xc7, 0x03, 0x1f, 0xef, 0x68, 0xc9, 0xcb, 0x6d, 0x92, 0x9f, 0xf9, 0x78, 0xd0, 0x27,
	0x09, 0x17, 0xb5, 0xa0, 0x1c, 0xe7, 0x33, 0xf0, 0x71, 0x51, 0xab, 0xcc, 0x76, 0x52, 0xfb, 0xfc,
	0x34, 0x73, 0x9d, 0xe2, 0xe8, 0x03, 0x54, 0x05, 0x93, 0xfc, 0x41, 0x4c, 0x18, 0x2e, 0x69, 0xee,
	0xab, 0xf5, 0x08, 0xba, 0x32, 0x92, 0x92, 0x7a, 0x3c, 0xbc, 0x0b, 0xee, 0x49, 0x2e, 0x42, 0x47,
	0x50, 0x8d, 0x44, 0xc0, 0x45, 0xa0, 0x16, 0xb8, 0x6c, 0x19, 0xad, 0x06, 0xc9, 0x6d, 0x64, 0x41,
	0x3d, 0x12, 0x8c, 0xcd, 0x22, 0x15, 0x8c, 0xa7, 0x0c, 0x57, 0x2c, 0xa3, 0x55, 0x25, 0xab, 0x47,
	0x71, 0xa2, 0x53, 0x3a, 0x66, 0x53, 0x89, 0xab, 0xeb, 0x89, 0x5e, 0xe9, 0x73, 0x92, 0xe2, 0xe8,
	0x03, 0xc0, 0x84, 0x87, 0x52, 0x09, 0x1a, 0x84, 0x0a, 0xd7, 0x34, 0xfb, 0x87, 0x8d, 0xa9, 0xf6,
	0x72, 0x1a, 0x59, 0x91, 0xa0, 0x77, 0x50, 0x54, 0x8b, 0x88, 0x61, 0xb0, 0x8c, 0xd6, 0xde, 0x99,
	0xd5, 0xde, 0x3c, 0x2e, 0xfa, 0x7e, 0xbc, 0x45, 0xc4, 0x88, 0x66, 0xc7, 0xe5, 0x85, 0x0f, 0xb3,
	0x11, 0x17, 0x4a, 0xe2, 0x7a, 0x52, 0x5e, 0x66, 0xa3, 0x26, 0xec, 0xce, 0x82, 0x70, 0x10, 0x4a,
	0x45, 0xc3, 0x09, 0x93, 0x78, 0x57, 0xe3, 0x8f, 0xce, 0x62, 0xfd, 0xef, 0x5c, 0x2a, 0x3d, 0x06,
	0x0d, 0x3d, 0x06, 0xb9, 0x8d, 0x8e, 0x75, 0x49, 0x4a, 0xf0, 0xe9, 0x94, 0x09, 0xbc, 0xa7, 0x6f,
	0x67, 0xe5, 0x04, 0xbd, 0x87, 0xef, 0xa2, 0x29, 0x9d, 0xb0, 0x19, 0x0b, 0x95, 0x17, 0xcc, 0x18,
	0x7f, 0x50, 0x2e, 0x9b, 0xf0, 0xd0, 0x97, 0xf8, 0x5b, 0xcb, 0x68, 0x19, 0x64, 0x1b, 0x8c, 0x4e,
	0xe0, 0x20, 0x87, 0x08, 0x53, 0x62, 0xd1, 0xe3, 0x0f, 0xa1, 0xc2, 0xa6, 0x56, 0x6d, 0x82, 0xd0,
	0x0b, 0xa8, 0x09, 0x36, 0xe7, 0x13, 0x1a, 0x37, 0x6a, 0x5f, 0xa7, 0xb2, 0x3c, 0x88, 0x1b, 0xe9,
	0x33, 0x19, 0x08, 0xe6, 0x5f, 0x70, 0xa9, 0x30, 0xd2, 0x85, 0xac, 0x1e, 0xa1, 0x77, 0xf0, 0x2c,
	0x77, 0xdb, 0x55, 0x2a, 0x6e, 0x70, 0x12, 0xf3, 0x40, 0xc7, 0xdc, 0x0c, 0xc6, 0x15, 0x0a, 0x46,
	0xfd, 0xc5, 0x27, 0x2e, 0x62, 0x2f, 0x84, 0x49, 0x26, 0xe6, 0x54, 0x05, 0x3c, 0xc4, 0x87, 0x3a,
	0x87, 0x6d, 0x30, 0x72, 0x61, 0x3f, 0x77, 0xe9, 0x2a, 0x41, 0x15, 0xbb, 0x5f, 0xe0, 0x67, 0xba,
	0xb5, 0x3f, 0xae, 0x4f, 0x45, 0xfc, 0xae, 0x47, 0xeb, 0x64, 0xf2, 0xb5, 0xbe, 0xf9, 0x77, 0x11,
	0x6a, 0x39, 0x11, 0xbd, 0x87, 0x52, 0x3c, 0x4f, 0x12, 0x1b, 0xd6, 0xce, 0xd3, 0x8f, 0xf5, 0xbc,
	0x80, 0x0d, 0x92, 0x08, 0x1e, 0x35, 0xbd, 0xb0, 0xd6, 0xf4, 0x9f, 0xa0, 0x42, 0xef, 0x59, 0xa8,
	0xf2, 0x17, 0xbd, 0xbf, 0x1c, 0xf9, 0xae, 0x06, 0xfa, 0x24, 0x63, 0xa0, 0x53, 0xa8, 0xf2, 0xbb,
	0x3b, 0x26, 0x06, 0xbe, 0xc4, 0x45, 0x9d, 0xc5, 0x0a, 0x7b, 0xa8, 0x91, 0x24, 0x70, 0x4e, 0x43,
	0x87, 0x50, 0x8a, 0xf4, 0xb4, 0x96, 0xac, 0x9d, 0x56, 0x83, 0x24, 0x46, 0x3e, 0xfc, 0xe5, 0xff,
	0x35, 0xfc, 0x36, 0xd4, 0xe3, 0xbc, 0xd3, 0x40, 0xfa, 0xfd, 0x6e, 0xd8, 0x0f, 0x99, 0x79, 0xb1,
	0xa4, 0x92, 0x55, 0x1d, 0xfa, 0x08, 0x15, 0xbd, 0x6d, 0xfa, 0xf1, 0x2b, 0x8f, 0x8b, 0x78, 0xbd,
	0x2d, 0x7e, 0x7e, 0xf9, 0x3a, 0x13, 0x92, 0xc9, 0x8e, 0xfe, 0x30, 0xd2, 0x8d, 0xda, 0x87, 0x46,
	0x2a, 0x4d, 0x6e, 0x5d, 0xaf, 0xd6, 0xa7, 0x17, 0xe9, 0x63, 0x11, 0x3a, 0x83, 0xba, 0xbe, 0xc5,
	0xd4, 0x47, 0x61, 0xcb, 0x8e, 0x5c, 0x25, 0x35, 0xff, 0x32, 0xe0, 0x60, 0x94, 0x6e, 0x2e, 0x1e,
	0xf6, 0x68, 0xe8, 0x07, 0x3e, 0x55, 0xd9, 0x3e, 0x37, 0xfe, 0xf3, 0x3e, 0xff, 0x08, 0x65, 0xc1,
	0xa8, 0xe4, 0xa1, 0x0e, 0xbb, 0x77, 0xd6, 0xda, 0x7a, 0x17, 0x79, 0x30, 0xa2, 0xf9, 0x24, 0xd5,
	0xbd, 0xb9, 0x84, 0x6a, 0xd6, 0x27, 0x54, 0x87, 0xca, 0x8d, 0x73, 0xe9, 0x0c, 0x7f, 0x75, 0xcc,
	0x6f, 0x50, 0x0d, 0x4a, 0xe7, 0x5d, 0xaf, 0x77, 0x61, 0x1a, 0xa8, 0x01, 0x35, 0xd7, 0xeb, 0x7a,
	0xf6, 0x95, 0xed, 0xba, 0x66, 0x01, 0xed, 0x42, 0x55, 0x9b, 0x9f, 0x6e, 0xae, 0xcc, 0x1d, 0x04,
	0x50, 0xee, 0x77, 0xed, 0xeb, 0xa1, 0x63, 0x16, 0xdf, 0xfc, 0x69, 0x80, 0xb9, 0x1e, 0x09, 0xbd,
	0x84, 0xef, 0x47, 0xc4, 0xb6, 0xaf, 0x47, 0xde, 0x60, 0xe8, 0xdc, 0x12, 0xbb, 0xeb, 0x0e, 0x9d,
	0xdb, 0x65, 0x9c, 0xd7, 0xd0, 0xfc, 0x1a, 0x26, 0xf6, 0x2f, 0xc3, 0x4b, 0xfb, 0x96, 0xd8, 0xee,
	0xf0, 0x86, 0xf4, 0x6c, 0xd7, 0x34, 0x36, 0xf3, 0x2e, 0x86, 0xae, 0x77, 0x7b, 0xdd, 0x1d, 0x38,
	0x9e, 0xed, 0x74, 0x9d, 0x9e, 0x6d, 0x16, 0xce, 0xf1, 0x6f, 0xcf, 0x37, 0xff, 0xc0, 0x8f, 0xcb,
	0xfa, 0x07, 0xf5, 0xed, 0xbf, 0x01, 0x00, 0x00, 0xff, 0xff, 0xb1, 0x95, 0xe3, 0x33, 0x01, 0x08,
	0x00, 0x00,
}
