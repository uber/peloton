package storage

import (
	"fmt"
	mesos_v1 "mesos/v1"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/respool"
	"peloton/api/task"
	"peloton/api/volume"
)

// TaskNotFoundError indicates that task is not found
type TaskNotFoundError struct {
	TaskID string
}

func (e *TaskNotFoundError) Error() string {
	return fmt.Sprintf("%v is not found", e.TaskID)
}

// JobStore is the interface to store job states
type JobStore interface {
	CreateJob(id *peloton.JobID, Config *job.JobConfig, createBy string) error
	GetJobConfig(id *peloton.JobID) (*job.JobConfig, error)
	Query(Labels *mesos_v1.Labels) (map[string]*job.JobConfig, error)
	DeleteJob(id *peloton.JobID) error
	GetJobsByOwner(owner string) (map[string]*job.JobConfig, error)
	GetAllJobs() (map[string]*job.JobConfig, error)
	GetJobRuntime(id *peloton.JobID) (*job.RuntimeInfo, error)
	GetJobsByState(state job.JobState) ([]peloton.JobID, error)
	UpdateJobRuntime(id *peloton.JobID, runtime *job.RuntimeInfo) error
	GetJobsByRespoolID(respoolID *respool.ResourcePoolID) (map[string]*job.JobConfig, error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	// TODO: remove CreateTask as it should be deprecated for CreateTasks
	CreateTask(id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, createdBy string) error
	CreateTasks(id *peloton.JobID, taskInfos []*task.TaskInfo, createdBy string) error
	GetTasksForJob(id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobByRange(id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	UpdateTask(taskInfo *task.TaskInfo) error
	GetTaskByID(taskID string) (*task.TaskInfo, error)
	QueryTasks(id *peloton.JobID, offset uint32, limit uint32) ([]*task.TaskInfo, uint32, error)
}

// FrameworkInfoStore is the interface to store mesosStreamID for peloton frameworks
type FrameworkInfoStore interface {
	SetMesosStreamID(frameworkName string, mesosStreamID string) error
	SetMesosFrameworkID(frameworkName string, frameworkID string) error
	GetMesosStreamID(frameworkName string) (string, error)
	GetFrameworkID(frameworkName string) (string, error)
}

// ResourcePoolStore is the interface to store all the resource pool information
type ResourcePoolStore interface {
	CreateResourcePool(id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig, createdBy string) error
	GetResourcePool(id *respool.ResourcePoolID) (*respool.ResourcePoolInfo, error)
	DeleteResourcePool(id *respool.ResourcePoolID) error
	UpdateResourcePool(id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig) error
	// TODO change to return ResourcePoolInfo
	GetResourcePoolsByOwner(owner string) (map[string]*respool.ResourcePoolConfig, error)
	GetAllResourcePools() (map[string]*respool.ResourcePoolConfig, error)
}

// PersistentVolumeStore is the interface to store all the persistent volume info
type PersistentVolumeStore interface {
	CreatePersistentVolume(volumeInfo *volume.PersistentVolumeInfo) error
	UpdatePersistentVolume(volumeID string, state volume.VolumeState) error
	GetPersistentVolume(volumeID string) (*volume.PersistentVolumeInfo, error)
	DeletePersistentVolume(volumeID string) error
}
