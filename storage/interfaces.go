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

// Store is is a generic store interface which is
// a collection of different store interfaces
type Store interface {
	JobStore
	TaskStore
	UpdateStore
	FrameworkInfoStore
	ResourcePoolStore
	PersistentVolumeStore
	SecretStore
}

// JobStore is the interface to store job states
type JobStore interface {
	// CreateJobConfig creates the job configuration
	CreateJobConfig(ctx context.Context, id *peloton.JobID, config *job.JobConfig, version uint64, createBy string) error
	// CreateJobRuntimeWithConfig creates the job runtime
	CreateJobRuntimeWithConfig(ctx context.Context, id *peloton.JobID, initialRuntime *job.RuntimeInfo, config *job.JobConfig) error
	// GetJobConfig fetches the job configuration for a given job
	GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error)
	// QueryJobs queries for all jobs which match the query in the QuerySpec
	QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec, summaryOnly bool) ([]*job.JobInfo, []*job.JobSummary, uint32, error)
	// UpdateJobConfig updates the job configuration of an existing job
	UpdateJobConfig(ctx context.Context, id *peloton.JobID, Config *job.JobConfig) error
	// DeleteJob deletes the job configuration, runtime
	// and all tasks in DB of a given job
	DeleteJob(ctx context.Context, id *peloton.JobID) error
	// GetAllJobs gets the runtime of all jobs in DB
	GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error)
	// GetJobRuntime gets the job runtime of a given job
	GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error)
	// GetJobsByStates gets all jobs in a given state
	GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error)
	// UpdateJobRuntime updates the runtime of a given job
	UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error
}

// TaskStore is the interface to store task states
type TaskStore interface {
	// CreateTaskRuntime creates the runtime of a given task
	CreateTaskRuntime(ctx context.Context, id *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo, createdBy string) error
	// CreateTaskRuntimes creates the runtimes by running multiple parallel go routines, each of which do batching as well
	CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo, createdBy string) error
	// GetTaskRuntime gets the runtime of a given task
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error)
	// UpdateTaskRuntime updates the runtime of a given task
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error
	// UpdateTaskRuntimes updates the runtimes by running multiple parallel go routines, each of which do batching as well
	UpdateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo) error

	// CreateTaskConfigs creates the configuration of all tasks
	CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error

	// GetTasksForJob gets the task info for all tasks in a job
	GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error)
	// GetTasksForJobAndStates gets the task info for all
	// tasks in a given job and in a given state
	GetTasksForJobAndStates(ctx context.Context, id *peloton.JobID, states []task.TaskState) (map[uint32]*task.TaskInfo, error)
	// GetTaskIDsForJobAndState gets the task identifiers for all
	// tasks in a given job and in a given state
	GetTaskIDsForJobAndState(ctx context.Context, id *peloton.JobID, state string) ([]uint32, error)
	// GetTaskRuntimesForJobByRange gets the task runtime for all
	// tasks in a job with instanceID in the given range
	GetTaskRuntimesForJobByRange(ctx context.Context, id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.RuntimeInfo, error)
	// GetTasksForJobByRange gets the task info for all
	// tasks in a job with instanceID in the given range
	GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	// GetTaskForJob gets the task info for a given task
	GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	// GetTaskConfig gets the task config of a given task
	GetTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version uint64) (*task.TaskConfig, error)
	// GetTaskConfigs gets the task config for all tasks in a job
	// for all the instanceIDs provided in the input
	GetTaskConfigs(ctx context.Context, id *peloton.JobID, instanceIDs []uint32, version uint64) (map[uint32]*task.TaskConfig, error)
	// GetTaskByID gets the task info for a given task
	GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error)
	// QueryTasks queries for all tasks in a job matching the QuerySpec
	QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error)
	// GetTaskStateSummaryForJob gets the map state to instanceIDs
	// (in that state) in a given job
	GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error)
	// GetTaskEvents gets all the events for runtime changes for a given task
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

// SecretStore is the interface to store job secrets.
type SecretStore interface {
	// Create a secret described by peloton secret proto message in the database
	// Returns error in case the storage backend is unable to store the
	// secret in the database
	CreateSecret(ctx context.Context, secret *peloton.Secret, id *peloton.JobID) error
	// Get a secret described by peloton secret id from the database
	// Returns secret formatted as peloton secret proto message
	// Returns error in case the storage backend is unable to retrieve the
	// secret from the database
	GetSecret(ctx context.Context, id *peloton.SecretID) (*peloton.Secret, error)
}
