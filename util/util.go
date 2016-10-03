package util

import (
	"code.uber.internal/go-common.git/x/log"
	mesos_v1 "mesos/v1"
	"peloton/task"
)

// TaskLauncher is the interface to launch a set of tasks using an offer
type TaskLauncher interface {
	LaunchTasks(offer *mesos_v1.Offer, pelotonTasks []*task.TaskInfo)
}

// GetOfferScalarResourceSummary generates a summary for all the scalar values: role -> offerName-> Value
// first level : role -> map(resource type-> resouce value)
func GetOfferScalarResourceSummary(offer *mesos_v1.Offer) map[string]map[string]float64 {
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
func ConvertToMesosTaskInfo(taskInfo *task.TaskInfo) *mesos_v1.TaskInfo {
	var rs []*mesos_v1.Resource
	taskResources := taskInfo.GetJobConfig().Resource
	rs = append(rs, NewMesosResourceBuilder().WithName("cpus").WithValue(taskResources.CpusLimit).Build())
	rs = append(rs, NewMesosResourceBuilder().WithName("mem").WithValue(taskResources.MemLimitMb).Build())
	rs = append(rs, NewMesosResourceBuilder().WithName("disk").WithValue(taskResources.DiskLimitMb).Build())
	// TODO: translate job.ResourceConfig fdlimit

	taskId := taskInfo.GetRuntime().GetTaskId().GetValue()
	mesosTask := &mesos_v1.TaskInfo{
		Name: &taskInfo.JobId.Value,
		TaskId: &mesos_v1.TaskID{
			Value: &taskId,
		},
		Resources: rs,
		Command:   taskInfo.GetJobConfig().GetCommand(),
	}
	return mesosTask
}

// CanTakeTask takes an offer and a taskInfo and check if the offer can be used to run the task.
// If yes, the resources for the task would be substracted from the offer
func CanTakeTask(offerScalaSummary *map[string]map[string]float64, nextTask *task.TaskInfo) bool {
	nextMesosTask := ConvertToMesosTaskInfo(nextTask)
	log.Debugf("resources -- %v", nextMesosTask.Resources)
	log.Debugf("summary -- %v", offerScalaSummary)
	for _, resource := range nextMesosTask.Resources {
		// Check if all scalar resource requirements are met
		// TODO: check range and items
		if *resource.Type == mesos_v1.Value_SCALAR {
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
	Resource mesos_v1.Resource
}

//NewMesosResourceBuilder creates a MesosResourceBuilder
func NewMesosResourceBuilder() *MesosResourceBuilder {
	defaultRole := "*"
	defaultType := mesos_v1.Value_SCALAR
	return &MesosResourceBuilder{
		Resource: mesos_v1.Resource{
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

//WithType sets type
func (o *MesosResourceBuilder) WithType(t mesos_v1.Value_Type) *MesosResourceBuilder {
	o.Resource.Type = &t
	return o
}

//WithRole sets role
func (o *MesosResourceBuilder) WithRole(role string) *MesosResourceBuilder {
	o.Resource.Role = &role
	return o
}

//WithValue sets value
func (o *MesosResourceBuilder) WithValue(value float64) *MesosResourceBuilder {
	scalarVal := mesos_v1.Value_Scalar{
		Value: &value,
	}
	o.Resource.Scalar = &scalarVal
	return o
}

// TODO: add other building functions when needed

//Build returns the mesos resource
func (o *MesosResourceBuilder) Build() *mesos_v1.Resource {
	res := o.Resource
	return &res
}

// TODO: adjust in case there are additional peloton states
func MesosStateToPelotonState(mstate mesos_v1.TaskState) task.RuntimeInfo_TaskState {
	switch mstate {
	case mesos_v1.TaskState_TASK_STAGING:
		return task.RuntimeInfo_LAUNCHED
	case mesos_v1.TaskState_TASK_STARTING:
		return task.RuntimeInfo_LAUNCHED
	case mesos_v1.TaskState_TASK_RUNNING:
		return task.RuntimeInfo_RUNNING
	// NOTE: This should only be sent when the framework has
	// the TASK_KILLING_STATE capability.
	case mesos_v1.TaskState_TASK_KILLING:
		return task.RuntimeInfo_RUNNING
	case mesos_v1.TaskState_TASK_FINISHED:
		return task.RuntimeInfo_SUCCEEDED
	case mesos_v1.TaskState_TASK_FAILED:
		return task.RuntimeInfo_FAILED
	case mesos_v1.TaskState_TASK_KILLED:
		return task.RuntimeInfo_KILLED
	case mesos_v1.TaskState_TASK_LOST:
		return task.RuntimeInfo_LOST
	case mesos_v1.TaskState_TASK_ERROR:
		return task.RuntimeInfo_FAILED
	default:
		log.Errorf("Unknown mesos taskState %v", mstate)
		return task.RuntimeInfo_INITIALIZED
	}
}
