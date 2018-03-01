package storage

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
)

// TaskNotFoundError indicates that task is not found
type TaskNotFoundError struct {
	TaskID string
}

func (e *TaskNotFoundError) Error() string {
	return fmt.Sprintf("%v is not found", e.TaskID)
}

// VolumeNotFoundError indicates that persistent volume is not found
type VolumeNotFoundError struct {
	VolumeID *peloton.VolumeID
}

func (e *VolumeNotFoundError) Error() string {
	return fmt.Sprintf("volume %v is not found", e.VolumeID.GetValue())
}

// JobStore is the interface to store job states
type JobStore interface {
	CreateJob(ctx context.Context, id *peloton.JobID, Config *job.JobConfig, createBy string) error
	GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error)
	QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec, summaryOnly bool) ([]*job.JobInfo, []*job.JobSummary, uint32, error)
	UpdateJobConfig(ctx context.Context, id *peloton.JobID, Config *job.JobConfig) error
	DeleteJob(ctx context.Context, id *peloton.JobID) error
	GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error)
	GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error)
	GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error)
	UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error
}

// TaskStore is the interface to store task states
type TaskStore interface {
	// TODO: remove CreateTaskRuntime as it should be deprecated for CreateTaskRuntimes
	CreateTaskRuntime(ctx context.Context, id *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo, createdBy string) error
	// CreateTaskRuntimes creates the runtimes by running multiple parallel go routines, each of which do batching as well
	CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo, createdBy string) error
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error)
	// UpdateTaskRuntimes updates the runtimes by running multiple parallel go routines, each of which do batching as well
	UpdateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo) error

	CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error
	GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTaskIDsForJobAndState(ctx context.Context, id *peloton.JobID, state string) ([]uint32, error)
	GetTaskRuntimesForJobByRange(ctx context.Context, id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.RuntimeInfo, error)
	GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	GetTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version int64) (*task.TaskConfig, error)
	GetTaskConfigs(ctx context.Context, id *peloton.JobID, instanceIDs []uint32, version int64) (map[uint32]*task.TaskConfig, error)
	GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error)
	QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error)
	GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error)
	GetTaskEvents(ctx context.Context, id *peloton.JobID, instanceID uint32) ([]*task.TaskEvent, error)
}

// UpdateStore is the interface to store updates and updates progress.
type UpdateStore interface {
	// CreateUpdate by creating a new update in the storage. It's an error
	// if the update already exists.
	CreateUpdate(ctx context.Context, id *update.UpdateID, jobID *peloton.JobID, jobConfig *job.JobConfig, updateConfig *update.UpdateConfig) error

	// GetUpdateProgress returns the progress of the update.
	//
	// The overall state of the update can be explained by `instances` and
	// `progress`:
	// - An update can start the next task, if `len(processing) < batch_size`.
	// - An update has completed `progress - len(processing)` tasks.
	// - An update is done if
	//   `progress == InstanceCount && len(processing) == 0`.
	// - A task has been updated if
	//  `InstanceID < progress && !processing.Contains(InstanceID)`.
	// TODO: Create UpdateProgress struct for carrying this state.
	GetUpdateProgress(ctx context.Context, id *update.UpdateID) (processing []uint32, progress uint32, err error)
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
	CreateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, Config *respool.ResourcePoolConfig, createdBy string) error
	GetResourcePool(ctx context.Context, id *peloton.ResourcePoolID) (*respool.ResourcePoolInfo, error)
	DeleteResourcePool(ctx context.Context, id *peloton.ResourcePoolID) error
	UpdateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, Config *respool.ResourcePoolConfig) error
	// TODO change to return ResourcePoolInfo
	GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error)
	GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error)
}

// PersistentVolumeStore is the interface to store all the persistent volume info
type PersistentVolumeStore interface {
	CreatePersistentVolume(ctx context.Context, volumeInfo *volume.PersistentVolumeInfo) error
	UpdatePersistentVolume(ctx context.Context, volumeInfo *volume.PersistentVolumeInfo) error
	GetPersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) (*volume.PersistentVolumeInfo, error)
	DeletePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) error
}
