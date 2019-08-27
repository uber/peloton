// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	pbhostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"

	"github.com/uber/peloton/pkg/common"
)

const (
	// ResourceEpsilon is the minimum epsilon mesos resource;
	// This is because Mesos internally uses a fixed point precision. See MESOS-4687 for details.
	ResourceEpsilon = 0.0009
)

// UUIDLength represents the length of a 16 byte v4 UUID as a string
var UUIDLength = len(uuid.New())

// Min returns the minimum value of x, y
func Min(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}

// Max returns the maximum value of x, y
func Max(x, y uint32) uint32 {
	if x > y {
		return x
	}
	return y
}

// PtrPrintf returns a pointer to a string format
func PtrPrintf(format string, a ...interface{}) *string {
	str := fmt.Sprintf(format, a...)
	return &str
}

// SubtractSlice returns the result of slice1 - slice2
// if an element is in slice2 but not in slice1, it would be ignored
func SubtractSlice(slice1 []uint32, slice2 []uint32) []uint32 {
	if slice1 == nil {
		return nil
	}

	var result []uint32
	slice2Set := make(map[uint32]bool)

	for _, v := range slice2 {
		slice2Set[v] = true
	}

	for _, v := range slice1 {
		if !slice2Set[v] {
			result = append(result, v)
		}
	}

	return result
}

// IntersectSlice get return the result of slice1 ∩ slice2
// element must be present in both slices
func IntersectSlice(slice1 []uint32, slice2 []uint32) []uint32 {
	if slice1 == nil {
		return nil
	}

	var result []uint32
	slice2Set := make(map[uint32]bool)

	for _, v := range slice2 {
		slice2Set[v] = true
	}

	for _, v := range slice1 {
		if slice2Set[v] {
			result = append(result, v)
		}
	}

	return result
}

// ConvertInstanceIDListToInstanceRange converts list
// of instance ids to list of instance ranges
func ConvertInstanceIDListToInstanceRange(instIDs []uint32) []*pod.InstanceIDRange {
	var instanceIDRange []*pod.InstanceIDRange
	var instanceRange *pod.InstanceIDRange
	var prevInstID uint32

	instIDSortLess := func(i, j int) bool {
		return instIDs[i] < instIDs[j]
	}

	sort.Slice(instIDs, instIDSortLess)

	for _, instID := range instIDs {
		if instanceRange == nil {
			// create a new range
			instanceRange = &pod.InstanceIDRange{
				From: instID,
			}
		} else {
			// range already exists
			if instID != prevInstID+1 {
				// finish the previous range and start a new one
				instanceRange.To = prevInstID
				instanceIDRange = append(instanceIDRange, instanceRange)
				instanceRange = &pod.InstanceIDRange{
					From: instID,
				}
			}
		}
		prevInstID = instID
	}

	// finish the last instance range
	if instanceRange != nil {
		instanceRange.To = prevInstID
		instanceIDRange = append(instanceIDRange, instanceRange)
	}
	return instanceIDRange
}

// GetOfferScalarResourceSummary generates a summary for all the scalar values: role -> offerName-> Value
// first level : role -> map(resource type-> resouce value)
func GetOfferScalarResourceSummary(offer *mesos.Offer) map[string]map[string]float64 {
	var result = make(map[string]map[string]float64)
	for _, resource := range offer.Resources {
		if resource.Scalar != nil {
			var role = "*"
			if resource.Role != nil {
				role = *resource.Role
			}
			if _, ok := result[role]; !ok {
				result[role] = make(map[string]float64)
			}
			result[role][*resource.Name] = result[role][*resource.Name] + *resource.Scalar.Value
		}
	}
	return result
}

// CreateMesosScalarResources is a helper function to convert resource values into Mesos resources.
func CreateMesosScalarResources(values map[string]float64, role string) []*mesos.Resource {
	var rs []*mesos.Resource
	for name, value := range values {
		// Skip any value smaller than Espilon.
		if math.Abs(value) < ResourceEpsilon {
			continue
		}

		rs = append(rs, NewMesosResourceBuilder().WithName(name).WithValue(value).WithRole(role).Build())
	}

	return rs
}

// MesosResourceBuilder is the helper to build a mesos resource
type MesosResourceBuilder struct {
	Resource mesos.Resource
}

// NewMesosResourceBuilder creates a MesosResourceBuilder
func NewMesosResourceBuilder() *MesosResourceBuilder {
	defaultRole := "*"
	defaultType := mesos.Value_SCALAR
	return &MesosResourceBuilder{
		Resource: mesos.Resource{
			Role: &defaultRole,
			Type: &defaultType,
		},
	}
}

// WithName sets name
func (o *MesosResourceBuilder) WithName(name string) *MesosResourceBuilder {
	o.Resource.Name = &name
	return o
}

// WithType sets type
func (o *MesosResourceBuilder) WithType(t mesos.Value_Type) *MesosResourceBuilder {
	o.Resource.Type = &t
	return o
}

// WithRole sets role
func (o *MesosResourceBuilder) WithRole(role string) *MesosResourceBuilder {
	o.Resource.Role = &role
	return o
}

// WithValue sets value
func (o *MesosResourceBuilder) WithValue(value float64) *MesosResourceBuilder {
	scalarVal := mesos.Value_Scalar{
		Value: &value,
	}
	o.Resource.Scalar = &scalarVal
	return o
}

// WithRanges sets ranges
func (o *MesosResourceBuilder) WithRanges(ranges *mesos.Value_Ranges) *MesosResourceBuilder {
	o.Resource.Ranges = ranges
	return o
}

// WithReservation sets reservation info.
func (o *MesosResourceBuilder) WithReservation(
	reservation *mesos.Resource_ReservationInfo) *MesosResourceBuilder {

	o.Resource.Reservation = reservation
	return o
}

// WithDisk sets disk info.
func (o *MesosResourceBuilder) WithDisk(
	diskInfo *mesos.Resource_DiskInfo) *MesosResourceBuilder {

	o.Resource.Disk = diskInfo
	return o
}

// WithRevocable sets resource as revocable resource type
func (o *MesosResourceBuilder) WithRevocable(
	revocable *mesos.Resource_RevocableInfo) *MesosResourceBuilder {
	o.Resource.Revocable = revocable
	return o
}

// TODO: add other building functions when needed

// Build returns the mesos resource
func (o *MesosResourceBuilder) Build() *mesos.Resource {
	res := o.Resource
	return &res
}

// MesosStateToPelotonState translates mesos task state to peloton task state
// TODO: adjust in case there are additional peloton states
func MesosStateToPelotonState(mstate mesos.TaskState) task.TaskState {
	switch mstate {
	case mesos.TaskState_TASK_STAGING:
		return task.TaskState_LAUNCHED
	case mesos.TaskState_TASK_STARTING:
		return task.TaskState_STARTING
	case mesos.TaskState_TASK_RUNNING:
		return task.TaskState_RUNNING
		// NOTE: This should only be sent when the framework has
		// the TASK_KILLING_STATE capability.
	case mesos.TaskState_TASK_KILLING:
		return task.TaskState_RUNNING
	case mesos.TaskState_TASK_FINISHED:
		return task.TaskState_SUCCEEDED
	case mesos.TaskState_TASK_FAILED:
		return task.TaskState_FAILED
	case mesos.TaskState_TASK_KILLED:
		return task.TaskState_KILLED
	case mesos.TaskState_TASK_LOST:
		return task.TaskState_LOST
	case mesos.TaskState_TASK_ERROR:
		return task.TaskState_FAILED
	default:
		log.Errorf("Unknown mesos taskState %v", mstate)
		return task.TaskState_INITIALIZED
	}
}

// IsPelotonStateTerminal returns true if state is terminal
// otherwise false
func IsPelotonStateTerminal(state task.TaskState) bool {
	switch state {
	case task.TaskState_SUCCEEDED, task.TaskState_FAILED,
		task.TaskState_KILLED, task.TaskState_LOST,
		task.TaskState_DELETED:
		return true
	default:
		return false
	}
}

// IsTaskThrottled returns true if a task is currently
// throttled due to repeated failures
func IsTaskThrottled(state task.TaskState, message string) bool {
	if IsPelotonStateTerminal(state) && message == common.TaskThrottleMessage {
		return true
	}
	return false
}

// IsPelotonPodStateTerminal returns true if pod state is
// terminal otherwise false
func IsPelotonPodStateTerminal(state pod.PodState) bool {
	switch state {
	case pod.PodState_POD_STATE_SUCCEEDED, pod.PodState_POD_STATE_FAILED,
		pod.PodState_POD_STATE_KILLED, pod.PodState_POD_STATE_LOST,
		pod.PodState_POD_STATE_DELETED:
		return true
	default:
		return false
	}
}

// IsPelotonJobStateTerminal returns true if job state is terminal
// otherwise false
func IsPelotonJobStateTerminal(state job.JobState) bool {
	switch state {
	case job.JobState_SUCCEEDED, job.JobState_FAILED,
		job.JobState_DELETED, job.JobState_KILLED:
		return true
	default:
		return false
	}
}

// IsTaskHasValidVolume returns true if a task is stateful and has a valid volume
func IsTaskHasValidVolume(taskInfo *task.TaskInfo) bool {
	if taskInfo.GetConfig().GetVolume() != nil &&
		len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0 {
		return true
	}
	return false
}

// CreateMesosTaskID creates mesos task id given jobID, instanceID and runID
func CreateMesosTaskID(jobID *peloton.JobID,
	instanceID uint32,
	runID uint64) *mesos.TaskID {
	mesosID := fmt.Sprintf(
		"%s-%d-%d",
		jobID.GetValue(),
		instanceID,
		runID)
	return &mesos.TaskID{Value: &mesosID}
}

// CreatePelotonTaskID creates a PelotonTaskID given jobID and instanceID
func CreatePelotonTaskID(
	jobID string,
	instanceID uint32,
) string {
	return fmt.Sprintf("%s-%d", jobID, instanceID)
}

// CreatePodIDFromMesosTaskID creates a peloton pod ID from mesos taskID.
func CreatePodIDFromMesosTaskID(t *mesos.TaskID) *v1alphapeloton.PodID {
	return &v1alphapeloton.PodID{
		Value: t.GetValue(),
	}
}

// CreateLeaseIDFromHostOfferID creates a LeaseId from host offer ID.
func CreateLeaseIDFromHostOfferID(id *peloton.HostOfferID) *pbhostmgr.LeaseID {
	return &pbhostmgr.LeaseID{
		Value: id.GetValue(),
	}
}

// ParseRunID parse the runID from mesosTaskID
func ParseRunID(mesosTaskID string) (uint64, error) {
	splitMesosTaskID := strings.Split(mesosTaskID, "-")
	if len(mesosTaskID) == 0 { // prev mesos task id is nil
		return 0,
			yarpcerrors.InvalidArgumentErrorf(
				"mesosTaskID provided is empty",
			)
	} else if len(splitMesosTaskID) == 7 {
		if runID, err := strconv.ParseUint(
			splitMesosTaskID[len(splitMesosTaskID)-1], 10, 64); err == nil {
			return runID, nil
		}
	}

	return 0,
		yarpcerrors.InvalidArgumentErrorf(
			"unable to parse mesos task id: %v",
			mesosTaskID,
		)
}

// ParseTaskID parses the jobID and instanceID from peloton taskID
func ParseTaskID(taskID string) (string, uint32, error) {
	pos := strings.LastIndex(taskID, "-")
	if len(taskID) < UUIDLength || pos == -1 {
		return "", 0,
			yarpcerrors.InvalidArgumentErrorf("invalid pelotonTaskID %v", taskID)
	}
	jobID := taskID[0:pos]
	ins := taskID[pos+1:]

	instanceID, err := strconv.ParseUint(ins, 10, 32)
	if err != nil {
		log.WithFields(log.Fields{
			"task_id": taskID,
			"job_id":  jobID,
		}).WithError(err).Error("failed to parse taskID")
		log.Info(err)
		return "",
			0,
			yarpcerrors.InvalidArgumentErrorf("unable to parse instanceID %v", taskID)
	}
	return jobID, uint32(instanceID), nil
}

// ParseTaskIDFromMesosTaskID parses the taskID from mesosTaskID
func ParseTaskIDFromMesosTaskID(mesosTaskID string) (string, error) {
	// mesos task id would be "(jobID)-(instanceID)-(runID)" form
	if len(mesosTaskID) < UUIDLength+1 {
		return "", yarpcerrors.InvalidArgumentErrorf("invalid mesostaskID %v", mesosTaskID)
	}

	// TODO: deprecate the check once mesos task id migration is complete from
	// uuid-int-uuid -> uuid(job ID)-int(instance ID)-int(monotonically incremental)
	// If uuid has all digits from uuid-int-uuid then it will increment from
	// that value and not default to 1.
	var pelotonTaskID string
	if len(mesosTaskID) > 2*UUIDLength {
		pelotonTaskID = mesosTaskID[:len(mesosTaskID)-(UUIDLength+1)]
	} else {
		pelotonTaskID = mesosTaskID[:strings.LastIndex(mesosTaskID, "-")]
	}

	_, _, err := ParseTaskID(pelotonTaskID)
	if err != nil {
		return "", err
	}
	return pelotonTaskID, nil
}

// ParseJobAndInstanceID return jobID and instanceID from given mesos task id.
func ParseJobAndInstanceID(mesosTaskID string) (string, uint32, error) {
	pelotonTaskID, err := ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		return "", 0, err
	}
	return ParseTaskID(pelotonTaskID)
}

// UnmarshalToType unmarshal a string to a typed interface{}
func UnmarshalToType(jsonString string, resultType reflect.Type) (interface{},
	error) {
	result := reflect.New(resultType)
	err := json.Unmarshal([]byte(jsonString), result.Interface())
	if err != nil {
		log.Errorf("Unmarshal failed with error %v, type %v, jsonString %v",
			err, resultType, jsonString)
		return nil, err
	}
	return result.Interface(), nil
}

// ConvertLabels will convert Peloton labels to Mesos labels.
func ConvertLabels(pelotonLabels []*peloton.Label) *mesos.Labels {
	mesosLabels := &mesos.Labels{
		Labels: make([]*mesos.Label, 0, len(pelotonLabels)),
	}
	for _, label := range pelotonLabels {
		mesosLabel := &mesos.Label{
			Key:   &label.Key,
			Value: &label.Value,
		}
		mesosLabels.Labels = append(mesosLabels.Labels, mesosLabel)
	}
	return mesosLabels
}

// Contains checks whether an item contains in a list
func Contains(list []string, item string) bool {
	for _, name := range list {
		if name == item {
			return true
		}
	}
	return false
}

// ContainsTaskState checks whether a TaskState contains in a list of TaskStates
func ContainsTaskState(list []task.TaskState, item task.TaskState) bool {
	for _, name := range list {
		if name == item {
			return true
		}
	}
	return false
}

// CreateHostInfo takes the agent Info and create the hostsvc.HostInfo
func CreateHostInfo(hostname string,
	agentInfo *mesos.AgentInfo) *hostsvc.HostInfo {
	// if agentInfo is nil , return nil HostInfo
	if agentInfo == nil {
		log.WithField("host", hostname).
			Warn("Agent Info is nil")
		return nil
	}
	// return the HostInfo object
	return &hostsvc.HostInfo{
		Hostname:   hostname,
		AgentId:    agentInfo.Id,
		Attributes: agentInfo.Attributes,
		Resources:  agentInfo.Resources,
	}
}

// CreateSecretVolume builds a mesos volume of type secret
// from the given secret path and secret value string
// This volume will be added to the job's default config
func CreateSecretVolume(secretPath string, secretStr string) *mesos.Volume {
	volumeMode := mesos.Volume_RO
	volumeSourceType := mesos.Volume_Source_SECRET
	secretType := mesos.Secret_VALUE
	return &mesos.Volume{
		Mode:          &volumeMode,
		ContainerPath: &secretPath,
		Source: &mesos.Volume_Source{
			Type: &volumeSourceType,
			Secret: &mesos.Secret{
				Type: &secretType,
				Value: &mesos.Secret_Value{
					Data: []byte(secretStr),
				},
			},
		},
	}
}

// IsSecretVolume returns true if the given volume is of type secret
func IsSecretVolume(volume *mesos.Volume) bool {
	return volume.GetSource().GetType() == mesos.Volume_Source_SECRET
}

// ConfigHasSecretVolumes returns true if config contains secret volumes
func ConfigHasSecretVolumes(config *task.TaskConfig) bool {
	for _, v := range config.GetContainer().GetVolumes() {
		if ok := IsSecretVolume(v); ok {
			return true
		}
	}
	return false
}

// RemoveSecretVolumesFromConfig removes secret volumes from the task config
// in place and returns the secret volumes
// Secret volumes are added internally at the time of creating a job with
// secrets by handleSecrets method. They are not supplied in the config in
// job create/update requests. Consequently, they should not be displayed
// as part of Job Get API response. This is necessary to achieve the broader
// goal of using the secrets proto message in Job Create/Update/Get API to
// describe secrets and not allow users to checkin secrets as part of config
func RemoveSecretVolumesFromConfig(config *task.TaskConfig) []*mesos.Volume {
	if config.GetContainer().GetVolumes() == nil {
		return nil
	}
	secretVolumes := []*mesos.Volume{}
	volumes := []*mesos.Volume{}
	for _, volume := range config.GetContainer().GetVolumes() {
		if ok := IsSecretVolume(volume); ok {
			secretVolumes = append(secretVolumes, volume)
		} else {
			volumes = append(volumes, volume)
		}
	}
	config.GetContainer().Volumes = nil
	if len(volumes) > 0 {
		config.GetContainer().Volumes = volumes
	}
	return secretVolumes
}

// RemoveSecretVolumesFromJobConfig removes secret volumes from the default
// config as well as instance config in place and returns the secret volumes
func RemoveSecretVolumesFromJobConfig(cfg *job.JobConfig) []*mesos.Volume {
	// remove secret volumes if present from default config
	secretVolumes := RemoveSecretVolumesFromConfig(cfg.GetDefaultConfig())

	// remove secret volumes if present from instance config
	for _, config := range cfg.GetInstanceConfig() {
		// instance config contains the same secret volumes as default config,
		// so no need to operate on them
		_ = RemoveSecretVolumesFromConfig(config)
	}
	return secretVolumes
}

// ConvertTimestampToUnixSeconds converts timestamp string in RFC3339 format
// to the unix time in seconds.
func ConvertTimestampToUnixSeconds(timestamp string) (int64, error) {
	ts, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return 0, err
	}

	return ts.Unix(), nil
}

// GetDereferencedJobIDsList dereferences the jobIDs list
func GetDereferencedJobIDsList(jobIDs []*peloton.JobID) []peloton.JobID {
	result := []peloton.JobID{}
	for _, jobID := range jobIDs {
		result = append(result, *jobID)
	}
	return result
}

// FormatTime converts a Unix timestamp to a string format of the
// given layout in UTC. See https://golang.org/pkg/time/ for possible
// time layout in golang. For example, it will return RFC3339 format
// string like 2017-01-02T11:00:00.123456789Z if the layout is
// time.RFC3339Nano
func FormatTime(timestamp float64, layout string) string {
	seconds := int64(timestamp)
	nanoSec := int64((timestamp - float64(seconds)) *
		float64(time.Second/time.Nanosecond))
	return time.Unix(seconds, nanoSec).UTC().Format(layout)
}
