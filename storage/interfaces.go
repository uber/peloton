package storage

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
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
	CreateJob(ctx context.Context, id *peloton.JobID, Config *job.JobConfig, createBy string) error
	GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error)
	Query(ctx context.Context, labels []*peloton.Label, keywords []string) (map[string]*job.JobConfig, error)
	UpdateJobConfig(ctx context.Context, id *peloton.JobID, Config *job.JobConfig) error
	DeleteJob(ctx context.Context, id *peloton.JobID) error
	GetJobsByOwner(ctx context.Context, owner string) (map[string]*job.JobConfig, error)
	GetAllJobs(ctx context.Context) (map[string]*job.JobConfig, error)
	GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error)
	GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error)
	UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error
	GetJobsByRespoolID(ctx context.Context, respoolID *respool.ResourcePoolID) (map[string]*job.JobConfig, error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	// TODO: remove CreateTask as it should be deprecated for CreateTasks
	CreateTask(ctx context.Context, id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, createdBy string) error
	CreateTasks(ctx context.Context, id *peloton.JobID, taskInfos []*task.TaskInfo, createdBy string) error
	GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	UpdateTask(ctx context.Context, taskInfo *task.TaskInfo) error
	GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error)
	QueryTasks(ctx context.Context, id *peloton.JobID, offset uint32, limit uint32) ([]*task.TaskInfo, uint32, error)
}

// FrameworkInfoStore is the interface to store mesosStreamID for peloton frameworks
type FrameworkInfoStore interface {
	SetMesosStreamID(ctx context.Context, frameworkName string, mesosStreamID string) error
	SetMesosFrameworkID(ctx context.Context, frameworkName string, frameworkID string) error
	GetMesosStreamID(ctx context.Context, frameworkName string) (string, error)
	GetFrameworkID(ctx context.Context, frameworkName string) (string, error)
}

// ResourcePoolStore is the interface to store all the resource pool information
type ResourcePoolStore interface {
	CreateResourcePool(ctx context.Context, id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig, createdBy string) error
	GetResourcePool(ctx context.Context, id *respool.ResourcePoolID) (*respool.ResourcePoolInfo, error)
	DeleteResourcePool(ctx context.Context, id *respool.ResourcePoolID) error
	UpdateResourcePool(ctx context.Context, id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig) error
	// TODO change to return ResourcePoolInfo
	GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error)
	GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error)
}

// PersistentVolumeStore is the interface to store all the persistent volume info
type PersistentVolumeStore interface {
	CreatePersistentVolume(ctx context.Context, volumeInfo *volume.PersistentVolumeInfo) error
	UpdatePersistentVolume(ctx context.Context, volumeID string, state volume.VolumeState) error
	GetPersistentVolume(ctx context.Context, volumeID string) (*volume.PersistentVolumeInfo, error)
	DeletePersistentVolume(ctx context.Context, volumeID string) error
}
