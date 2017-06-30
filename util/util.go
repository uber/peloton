package util

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
)

const (
	// ResourceEspilon is the minimum espilon mesos resource;
	// This is because Mesos internally uses a fixed point precision. See MESOS-4687 for details.
	ResourceEspilon float64 = 0.0009
)

var uuidLength = len(uuid.NewUUID().String())

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
		if math.Abs(value) < ResourceEspilon {
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
		return task.TaskState_LAUNCHING
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
		task.TaskState_KILLED, task.TaskState_LOST:
		return true
	default:
		return false
	}
}

// ParseTaskID parses the jobID and instanceID from peloton taskID
func ParseTaskID(taskID string) (string, int, error) {
	pos := strings.LastIndex(taskID, "-")
	if pos == -1 {
		return taskID, 0, fmt.Errorf("Invalid task ID %v", taskID)
	}
	jobID := taskID[0:pos]
	ins := taskID[pos+1:]

	instanceID, err := strconv.Atoi(ins)
	if err != nil {
		log.Errorf("Failed to parse taskID %v err=%v", taskID, err)
	}
	return jobID, instanceID, err
}

// ParseTaskIDFromMesosTaskID parses the taskID from mesosTaskID
func ParseTaskIDFromMesosTaskID(mesosTaskID string) (string, error) {
	// mesos task id would be "(jobID)-(instanceID)-(GUID)" form
	if len(mesosTaskID) < uuidLength+1 {
		return "", fmt.Errorf("Invalid mesos task ID %v, not ending with uuid", mesosTaskID)
	}
	pelotonTaskID := mesosTaskID[:len(mesosTaskID)-(uuidLength+1)]
	_, _, err := ParseTaskID(pelotonTaskID)
	if err != nil {
		log.WithError(err).
			WithField("mesos_task_id", mesosTaskID).
			Error("Invalid mesos task ID, cannot parse jobID / instance")
		return "", err
	}
	return pelotonTaskID, nil
}

// ParseJobAndInstanceID return jobID and instanceID from given mesos task id.
func ParseJobAndInstanceID(mesosTaskID string) (string, int, error) {
	pelotonTaskID, err := ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		return "", 0, err
	}
	return ParseTaskID(pelotonTaskID)
}

// UnmarshalToType unmarshal a string to a typed interface{}
func UnmarshalToType(jsonString string, resultType reflect.Type) (interface{}, error) {
	result := reflect.New(resultType)
	err := json.Unmarshal([]byte(jsonString), result.Interface())
	if err != nil {
		log.Errorf("Unmarshal failed with error %v, type %v, jsonString %v", err, resultType, jsonString)
		return nil, nil
	}
	return result.Interface(), nil
}

// UnmarshalStringArray unmarshal a string array to a typed array, in interface{}
func UnmarshalStringArray(jsonStrings []string, resultType reflect.Type) ([]interface{}, error) {
	var results []interface{}
	for _, jsonString := range jsonStrings {
		result, err := UnmarshalToType(jsonString, resultType)
		if err != nil {
			return nil, nil
		}
		results = append(results, result)
	}
	return results, nil
}

// ConvertToResMgrGangs converts the taskinfo for the tasks comprising
// the config job to resmgr tasks and organizes them into gangs, each
// of which is a set of 1+ tasks to be admitted and placed as a group.
func ConvertToResMgrGangs(
	tasks []*task.TaskInfo,
	config *job.JobConfig) []*resmgrsvc.Gang {
	var gangs []*resmgrsvc.Gang

	// Gangs of multiple tasks are placed at the front of the returned list for
	// preferential treatment, since they are expected to be both more important
	// and harder to place than gangs comprising a single task.
	var multiTaskGangs []*resmgrsvc.Gang

	for _, t := range tasks {
		resmgrtask := ConvertTaskToResMgrTask(t, config)
		// Currently a job has at most 1 gang comprising multiple tasks;
		// those tasks have their MinInstances field set > 1.
		if resmgrtask.MinInstances > 1 {
			if len(multiTaskGangs) == 0 {
				var multiTaskGang resmgrsvc.Gang
				multiTaskGangs = append(multiTaskGangs, &multiTaskGang)
			}
			multiTaskGangs[0].Tasks = append(multiTaskGangs[0].Tasks, resmgrtask)
		} else {
			// Gang comprising one task
			var gang resmgrsvc.Gang
			gang.Tasks = append(gang.Tasks, resmgrtask)
			gangs = append(gangs, &gang)
		}
	}
	if len(multiTaskGangs) > 0 {
		gangs = append(multiTaskGangs, gangs...)
	}
	return gangs
}

// ConvertTaskToResMgrTask converts taskinfo to resmgr task.
func ConvertTaskToResMgrTask(
	taskInfo *task.TaskInfo,
	jobConfig *job.JobConfig) *resmgr.Task {
	instanceID := taskInfo.GetInstanceId()
	taskID := &peloton.TaskID{
		Value: fmt.Sprintf(
			"%s-%d",
			taskInfo.GetJobId().GetValue(),
			instanceID),
	}

	// If minInstances > 1, instances w/instanceID between 0..minInstances-1 should be gang-scheduled;
	// only pass MinInstances value > 1 for those tasks.
	minInstances := jobConfig.GetSla().GetMinimumRunningInstances()
	if (minInstances <= 1) || (instanceID >= minInstances) {
		minInstances = 1
	}

	numPorts := 0
	for _, portConfig := range taskInfo.GetConfig().GetPorts() {
		if portConfig.GetValue() == 0 {
			// Dynamic port.
			numPorts++
		}
	}

	// By default task type is batch.
	taskType := resmgr.TaskType_BATCH
	if taskInfo.GetConfig().GetVolume() != nil {
		taskType = resmgr.TaskType_STATEFUL
	}

	return &resmgr.Task{
		Id:           taskID,
		JobId:        taskInfo.GetJobId(),
		TaskId:       taskInfo.GetRuntime().GetMesosTaskId(),
		Name:         taskInfo.GetConfig().GetName(),
		Preemptible:  jobConfig.GetSla().GetPreemptible(),
		Priority:     jobConfig.GetSla().GetPriority(),
		MinInstances: minInstances,
		Resource:     taskInfo.GetConfig().GetResource(),
		Constraint:   taskInfo.GetConfig().GetConstraint(),
		NumPorts:     uint32(numPorts),
		Type:         taskType,
	}
}

// RegenerateMesosTaskID generates a new mesos task ID and update task info.
func RegenerateMesosTaskID(taskInfo *task.TaskInfo) {
	newMesosTaskID := fmt.Sprintf(
		"%s-%d-%s",
		taskInfo.GetJobId().GetValue(),
		taskInfo.GetInstanceId(),
		uuid.NewUUID().String())
	taskInfo.GetRuntime().GetMesosTaskId().Value = &newMesosTaskID
	taskInfo.GetRuntime().State = task.TaskState_INITIALIZED
}
