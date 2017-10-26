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
	CreateJobConfig(ctx context.Context, id *peloton.JobID, config *job.JobConfig) error
	GetJobConfig(ctx context.Context, id *peloton.JobID, version uint64) (*job.JobConfig, error)

	CreateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo, config *job.JobConfig) error
	GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error)
	UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo, config *job.JobConfig) error

	GetJob(ctx context.Context, id *peloton.JobID) (*job.JobInfo, error)
	DeleteJob(ctx context.Context, id *peloton.JobID) error
	GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error)
	GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error)
	QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec) ([]*job.JobInfo, uint32, error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error
	GetTaskConfig(ctx context.Context, jobID *peloton.JobID, instanceID uint32, configVersion uint64) (*task.TaskConfig, error)

	// TODO: remove CreateTask as it should be deprecated for CreateTasks
	CreateTaskRuntime(ctx context.Context, id *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo, createdBy string) error
	CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes []*task.RuntimeInfo, createdBy string) error
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error)
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error

	GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	GetTaskByID(ctx context.Context, taskID *peloton.TaskID) (*task.TaskInfo, error)
	QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error)
	GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error)
}

// UpgradeStore is the interface to store upgrade options and status.
type UpgradeStore interface {
	// CreateUpgrade by creating a new workflow in the storage. It's an error
	// if the worklfow already exists.
	CreateUpgrade(ctx context.Context, id *peloton.UpgradeID, status *upgrade.Status, options *upgrade.Options, fromConfigVersion, toConfigVersion uint64) error

	GetUpgradeStatus(ctx context.Context, id *peloton.UpgradeID) (*upgrade.Status, error)
	UpdateUpgradeStatus(ctx context.Context, id *peloton.UpgradeID, status *upgrade.Status) error

	GetUpgradeOptions(ctx context.Context, id *peloton.UpgradeID) (options *upgrade.Options, fromConfigVersion, toConfigVersion uint64, err error)

	// TODO: Do by state?
	GetUpgrades(ctx context.Context) ([]*upgrade.Status, error)
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
	UpdatePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID, state volume.VolumeState) error
	GetPersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) (*volume.PersistentVolumeInfo, error)
	DeletePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) error
}
