package storage

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
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
	QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec) ([]*job.JobInfo, uint32, error)
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
	CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes []*task.RuntimeInfo, createdBy string) error
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error)
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error

	CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error
	GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	GetTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version int64) (*task.TaskConfig, error)
	GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error)
	QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error)
	GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error)
}

// UpgradeStore is the interface to store upgrades and upgrade progress.
type UpgradeStore interface {
	// CreateUpgrade by creating a new workflow in the storage. It's an error
	// if the worklfow already exists.
	CreateUpgrade(ctx context.Context, id *upgrade.WorkflowID, spec *upgrade.UpgradeSpec) error

	// AddTaskToProcessing adds instanceID to the set of processing instances, and
	// incrementing the progress by one. It's an error if instanceID is not the
	// next instance in line to be upgraded.
	AddTaskToProcessing(ctx context.Context, id *upgrade.WorkflowID, instanceID uint32) error

	// RemoveTaskFromProcessing removes the instanceID from the set of processing
	// instances. This function is a no-op if the instanceID is not in the list
	// of processing tasks.
	RemoveTaskFromProcessing(ctx context.Context, id *upgrade.WorkflowID, instanceID uint32) error

	// GetWorkflowProgress returns the progress of the workflow.
	//
	// The overall state of the upgrade can be explained by `instances` and
	// `progress`:
	// - An upgrade can start the next task, if `len(processing) < batch_size`.
	// - An upgrade has completed `progress - len(processing)` tasks.
	// - An upgrade is done if
	//   `progress == InstanceCount && len(processing) == 0`.
	// - A task has been upgraded if
	//  `InstanceID < progress && !processing.Contains(InstanceID)`.
	// TODO: Create UpgradeProgress struct for carrying this state.
	GetWorkflowProgress(ctx context.Context, id *upgrade.WorkflowID) (processing []uint32, progress uint32, err error)
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
