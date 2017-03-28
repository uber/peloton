package util

import (
	"errors"
	"fmt"
	"math"
	mesos "mesos/v1"
	"peloton/api/task"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

const (
	// ResourceEspilon is the minimum espilon mesos resource;
	// This is because Mesos internally uses a fixed point precision. See MESOS-4687 for details.
	ResourceEspilon float64 = 0.0009
)

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

// ConvertToMesosTaskInfo converts a task.TaskInfo into mesos TaskInfo
func ConvertToMesosTaskInfo(taskInfo *task.TaskInfo) (*mesos.TaskInfo, error) {
	taskConfig := taskInfo.GetConfig()
	taskID := taskInfo.GetRuntime().GetTaskId()
	return GetMesosTaskInfo(taskID, taskConfig)
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

// GetMesosTaskInfo converts TaskID and TaskConfig into mesos TaskInfo.
func GetMesosTaskInfo(
	taskID *mesos.TaskID,
	taskConfig *task.TaskConfig) (*mesos.TaskInfo, error) {
	if taskConfig == nil {
		return nil, errors.New("TaskConfig cannot be nil")
	}

	if taskID == nil {
		return nil, errors.New("taskID cannot be nil")
	}

	taskResources := taskConfig.Resource

	if taskResources == nil {
		return nil, errors.New("TaskConfig.Resource cannot be nil")
	}

	rs := CreateMesosScalarResources(map[string]float64{
		"cpus": taskResources.CpuLimit,
		"mem":  taskResources.MemLimitMb,
		"disk": taskResources.DiskLimitMb,
		"gpus": taskResources.GpuLimit,
	}, "*")

	tid, err := ParseTaskIDFromMesosTaskID(taskID.GetValue())
	if err != nil {
		return nil, err
	}

	jobID, _, err := ParseTaskID(tid)
	if err != nil {
		return nil, err
	}

	mesosTask := &mesos.TaskInfo{
		Name:      &jobID,
		TaskId:    taskID,
		Resources: rs,
		Command:   taskConfig.GetCommand(),
		Container: taskConfig.GetContainer(),
	}
	return mesosTask, nil
}

// CanTakeTask takes an offer and a taskInfo and check if the offer can be used to run the task.
// If yes, the resources for the task would be substracted from the offer
func CanTakeTask(offerScalaSummary *map[string]map[string]float64, nextTask *task.TaskInfo) bool {
	nextMesosTask, err := ConvertToMesosTaskInfo(nextTask)
	if err != nil {
		log.Error(err)
		return false
	}

	log.Debugf("resources -- %v", nextMesosTask.Resources)
	log.Debugf("summary -- %v", offerScalaSummary)
	for _, resource := range nextMesosTask.Resources {
		// Check if all scalar resource requirements are met
		// TODO: check range and items
		if *resource.Type == mesos.Value_SCALAR {
			role := "*"
			if resource.Role != nil {
				role = *resource.Role
			}
			if _, ok := (*offerScalaSummary)[role]; ok {
				if (*offerScalaSummary)[role][*resource.Name] < *resource.Scalar.Value {
					return false
				}
			} else {
				return false
			}
		}
	}
	for _, resource := range nextMesosTask.Resources {
		role := *resource.Role
		if role == "" {
			role = "*"
		}
		(*offerScalaSummary)[role][*resource.Name] = (*offerScalaSummary)[role][*resource.Name] - *resource.Scalar.Value
	}
	return true
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

// ParseTaskID parses the jobID and instanceID from taskID
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
	// mesos task id would be "(jobID)-(instanceID)-(GUID)" form, while the GUID part is optional
	parts := strings.Split(mesosTaskID, "-")

	if len(parts) < 2 {
		return "", fmt.Errorf("Invalid peloton mesos task ID %v", mesosTaskID)
	}

	jobID := parts[0]
	iID, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", fmt.Errorf("Invalid peloton mesos task ID %v, err %v", mesosTaskID, err)
	}
	return fmt.Sprintf("%s-%d", jobID, iID), nil
}
