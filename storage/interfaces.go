package storage

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"
)

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
	CreateJobConfig(ctx context.Context, id *peloton.JobID, config *job.JobConfig, configAddOn *models.ConfigAddOn, version uint64, createBy string) error
	// CreateJobRuntimeWithConfig creates the job runtime
	CreateJobRuntimeWithConfig(ctx context.Context, id *peloton.JobID, initialRuntime *job.RuntimeInfo, config *job.JobConfig) error
	// GetJobConfig fetches the job configuration for a given job
	GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, *models.ConfigAddOn, error)
	// GetJobConfigWithVersion fetches the job configuration for a given job of a given version
	GetJobConfigWithVersion(ctx context.Context, id *peloton.JobID, version uint64) (*job.JobConfig, *models.ConfigAddOn, error)
	// QueryJobs queries for all jobs which match the query in the QuerySpec
	QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec, summaryOnly bool) ([]*job.JobInfo, []*job.JobSummary, uint32, error)
	// UpdateJobConfig updates the job configuration of an existing job
	UpdateJobConfig(ctx context.Context, id *peloton.JobID, Config *job.JobConfig, configAddOn *models.ConfigAddOn) error
	// DeleteJob deletes the job configuration, runtime
	// and all tasks in DB of a given job
	DeleteJob(ctx context.Context, id *peloton.JobID) error
	// GetJobRuntime gets the job runtime of a given job
	GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error)
	// GetJobsByStates gets all jobs in a given state
	GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error)
	// UpdateJobRuntime updates the runtime of a given job
	UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error
	// GetMaxJobConfigVersion returns the maximum version of configs of a given job
	GetMaxJobConfigVersion(ctx context.Context, id *peloton.JobID) (uint64, error)
	GetJobSummaryFromIndex(ctx context.Context, id *peloton.JobID) (*job.JobSummary, error)

	// AddActiveJob adds job to active jobs table
	AddActiveJob(ctx context.Context, id *peloton.JobID) error
	// GetActiveJobs fetches all active jobs
	GetActiveJobs(ctx context.Context) ([]*peloton.JobID, error)
	// DeleteActiveJob deletes job from active jobs table
	DeleteActiveJob(ctx context.Context, id *peloton.JobID) error

	// GetJobIDFromJobName resolves the job ID from job name.
	GetJobIDFromJobName(ctx context.Context, jobName string) ([]*peloton.JobID, error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	// CreateTaskRuntime creates the runtime of a given task
	CreateTaskRuntime(
		ctx context.Context,
		id *peloton.JobID,
		instanceID uint32,
		runtime *task.RuntimeInfo,
		createdBy string,
		jobType job.JobType) error
	// GetTaskRuntime gets the runtime of a given task
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error)
	// UpdateTaskRuntime updates the runtime of a given task
	UpdateTaskRuntime(
		ctx context.Context,
		jobID *peloton.JobID,
		instanceID uint32,
		runtime *task.RuntimeInfo,
		jobType job.JobType) error

	// CreateTaskConfig creates the task configuration
	CreateTaskConfig(
		ctx context.Context,
		id *peloton.JobID,
		instanceID int64,
		taskConfig *task.TaskConfig,
		configAddOn *models.ConfigAddOn,
		version uint64,
	) error

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
	GetTaskForJob(ctx context.Context, jobID string, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	// GetTaskConfig gets the task config of a given task
	GetTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version uint64) (*task.TaskConfig, *models.ConfigAddOn, error)
	// GetTaskConfigs gets the task config for all tasks in a job
	// for all the instanceIDs provided in the input
	GetTaskConfigs(ctx context.Context, id *peloton.JobID, instanceIDs []uint32, version uint64) (map[uint32]*task.TaskConfig, *models.ConfigAddOn, error)
	// GetTaskByID gets the task info for a given task
	GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error)
	// QueryTasks queries for all tasks in a job matching the QuerySpec
	QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error)
	// GetTaskStateSummaryForJob gets the map state to instanceIDs
	// (in that state) in a given job
	GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error)
	// GetPodEvents returns pod events (state transition for a job instance).
	// limit parameter manages number of pod events to return
	// and optional runID parameter to fetch pod events only for that run if
	// not provided or unparseable then return for all runs
	GetPodEvents(ctx context.Context, jobID string, instanceID uint32, podID ...string) ([]*pod.PodEvent, error)
	// DeleteTaskRuntime deletes the task runtime for a given job instance
	DeleteTaskRuntime(ctx context.Context, id *peloton.JobID, instanceID uint32) error
	// DeletePodEvents deletes the pod events for provided JobID, InstanceID and RunID in the range [fromRunID-toRunID)
	DeletePodEvents(ctx context.Context, jobID string, instanceID uint32, fromRunID uint64, toRunID uint64) error
}

// UpdateStore is the interface to store updates and updates progress.
type UpdateStore interface {
	// CreateUpdate by creating a new update in the storage. It's an error
	// if the update already exists.
	CreateUpdate(
		ctx context.Context,
		updateInfo *models.UpdateModel,
	) error

	// DeleteUpdate deletes the update from the update_info table and deletes all
	// job and task configurations created for the update.
	DeleteUpdate(
		ctx context.Context,
		updateID *peloton.UpdateID,
		jobID *peloton.JobID,
		jobConfigVersion uint64,
	) error

	// GetUpdate fetches the job update stored in the DB
	GetUpdate(ctx context.Context, id *peloton.UpdateID) (
		*models.UpdateModel,
		error,
	)

	// WriteUpdateProgress writes the progress of the job update to the DB
	WriteUpdateProgress(
		ctx context.Context,
		updateInfo *models.UpdateModel,
	) error

	// ModifyUpdate modify the progress of an update,
	// instances to update/remove/add and the job config version
	ModifyUpdate(
		ctx context.Context,
		updateInfo *models.UpdateModel,
	) error

	// GetUpdateProgess fetches the job update progress, which includes the
	// instances already updated, instances being updated and the current
	// state of the update.
	GetUpdateProgress(ctx context.Context, id *peloton.UpdateID) (
		*models.UpdateModel,
		error,
	)

	// GetUpdatesForJob returns the list of job updates created for a given job
	GetUpdatesForJob(ctx context.Context, jobID *peloton.JobID) (
		[]*peloton.UpdateID, error)
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

	// Update a secret described by peloton secret proto message in the database
	// Returns error in case the storage backend is unable to store the
	// secret in the database
	UpdateSecret(ctx context.Context, secret *peloton.Secret) error
}
