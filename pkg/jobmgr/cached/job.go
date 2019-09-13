// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cached

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	stringsutil "github.com/uber/peloton/pkg/common/util/strings"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstateutil "github.com/uber/peloton/pkg/jobmgr/util/goalstate"

	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

var (
	_defaultTimeout     = 10 * time.Second
	_updateDeleteJobErr = yarpcerrors.InvalidArgumentErrorf("job is going to be deleted")
)

// Job in the cache.
// TODO there a lot of methods in this interface. To determine if
// this can be broken up into smaller pieces.
type Job interface {
	WorkflowOps

	// Identifier of the job.
	ID() *peloton.JobID

	// CreateTaskConfigs creates task configurations in the DB
	CreateTaskConfigs(
		ctx context.Context,
		jobID *peloton.JobID,
		jobConfig *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		spec *stateless.JobSpec,
	) error

	// CreateTaskRuntimes creates the task runtimes in cache and DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	CreateTaskRuntimes(ctx context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, owner string) error

	// PatchTasks patches runtime diff to the existing task cache. runtimeDiffs
	// is a kv map with key as the instance_id of the task to be updated.
	// Value of runtimeDiffs is RuntimeDiff, of which key is the field name
	// to be update, and value is the new value of the field. PatchTasks
	// would save the change in both cache and DB. If persisting to DB fails,
	// cache would be invalidated as well. The `force` flag affects only stateless
	// jobs. By default (with force flag unset), stateless jobs are patched in
	// a SLA aware manner i.e. only the tasks in the runtimeDiff which do not
	// violate the job SLA will be patched. If `force` flag is set, the diff
	// will be patched even if it violates job SLA. PatchTasks returns 2 lists
	// 1. list of instance_ids which were successfully patched and
	// 2. a list of instance_ids that should be retried.
	PatchTasks(
		ctx context.Context,
		runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
		force bool,
	) (instancesSucceeded []uint32, instancesToBeRetried []uint32, err error)

	// ReplaceTasks replaces task runtime and config in cache.
	// If forceReplace is false, it would check Revision version
	// and decide whether to replace the runtime and config.
	// If forceReplace is true, the func would always replace the runtime and config.
	ReplaceTasks(taskInfos map[uint32]*pbtask.TaskInfo, forceReplace bool) error

	// AddTask adds a new task to the job, and if already present,
	// just returns it. In addition if the task is not present, then
	// the runtime is recovered from DB as well. And
	// if the recovery does not succeed, the task is not
	// added to the cache either.
	AddTask(ctx context.Context, id uint32) (Task, error)

	// GetTask from the task id.
	GetTask(id uint32) Task

	// RemoveTask clear task out of cache.
	RemoveTask(id uint32)

	// GetAllTasks returns all tasks for the job
	GetAllTasks() map[uint32]Task

	// Create will be used to create the job configuration and runtime in DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	Create(
		ctx context.Context,
		config *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		spec *stateless.JobSpec,
	) error

	// RollingCreate is used to create the job configuration and runtime in DB.
	// It would create a workflow to manage the job creation, therefore the creation
	// process can be paused/resumed/aborted.
	RollingCreate(
		ctx context.Context,
		config *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		spec *stateless.JobSpec,
		updateConfig *pbupdate.UpdateConfig,
		opaqueData *peloton.OpaqueData,
	) error

	// Update updates job with the new runtime and config. If the request is to update
	// both DB and cache, it first attempts to persist the request in storage,
	// If that fails, it just returns back the error for now.
	// If successful, the cache is updated as well.
	// TODO: no config update should go through this API, divide this API into
	// config and runtime part
	Update(
		ctx context.Context,
		jobInfo *pbjob.JobInfo,
		configAddOn *models.ConfigAddOn,
		spec *stateless.JobSpec,
		req UpdateRequest,
	) error

	// CompareAndSetRuntime replaces the existing job runtime in cache and DB with
	// the job runtime supplied. CompareAndSetRuntime would use
	// RuntimeInfo.Revision.Version for concurrency control, and it would
	// update RuntimeInfo.Revision.Version automatically upon success. Caller
	// should not manually modify the value of RuntimeInfo.Revision.Version.
	// It returns the resultant jobRuntime with version updated.
	CompareAndSetRuntime(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error)

	// CompareAndSetConfig compares the version of config supplied and the
	// version of config in cache. If the version matches, it would update
	// the config in cache and DB with the config supplied (Notice: it does
	// NOT mean job would use the new job config, job would still use the
	// config which its runtime.ConfigurationVersion points to).
	// CompareAndSetConfig would update JobConfig.ChangeLog.Version
	// automatically upon success. Caller should not manually modify
	// the value of JobConfig.ChangeLog.Version.
	// It returns the resultant jobConfig with version updated.
	// JobSpec is also passed along so that it can be written as is to the DB
	CompareAndSetConfig(
		ctx context.Context,
		config *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		spec *stateless.JobSpec,
	) (jobmgrcommon.JobConfig, error)

	// CompareAndSetTask replaces the existing task runtime in DB and cache.
	// It uses RuntimeInfo.Revision.Version for concurrency control, and it would
	// update RuntimeInfo.Revision.Version automatically upon success.
	// Caller should not manually modify the value of RuntimeInfo.Revision.Version.
	// The `force` flag affects only stateless jobs. By default (with force flag
	// not set), for stateless job, if the task is becoming unavailable due to
	// host maintenance and update, then runtime is set only if it does not
	// violate the job SLA. If `force` flag is set, the task runtime will
	// be set even if it violates job SLA.
	CompareAndSetTask(
		ctx context.Context,
		id uint32,
		runtime *pbtask.RuntimeInfo,
		force bool,
	) (*pbtask.RuntimeInfo, error)

	// IsPartiallyCreated returns if job has not been fully created yet
	IsPartiallyCreated(config jobmgrcommon.JobConfig) bool

	// ValidateEntityVersion validates the entity version of the job is the
	// same as provided in the input, and if not, then returns an error.
	ValidateEntityVersion(ctx context.Context, version *v1alphapeloton.EntityVersion) error

	// GetRuntime returns the runtime of the job
	GetRuntime(ctx context.Context) (*pbjob.RuntimeInfo, error)

	// GetConfig returns the current config of the job
	GetConfig(ctx context.Context) (jobmgrcommon.JobConfig, error)

	// GetCachedConfig returns the job config if
	// present in the cache. Returns nil otherwise.
	GetCachedConfig() jobmgrcommon.JobConfig

	// GetJobType returns the job type in the job config stored in the cache
	// The type can be nil when we read it. It should be only used for
	// non-critical purpose (e.g calculate delay).
	// Logically this should be part of JobConfig
	// TODO(zhixin): remove GetJobType from the interface after
	// EnqueueJobWithDefaultDelay does not take cached job
	GetJobType() pbjob.JobType

	// SetTaskUpdateTime updates the task update times in the job cache
	SetTaskUpdateTime(t *float64)

	// GetFirstTaskUpdateTime gets the first task update time
	GetFirstTaskUpdateTime() float64

	// GetLastTaskUpdateTime gets the last task update time
	GetLastTaskUpdateTime() float64

	// UpdateResourceUsage adds the task resource usage from a terminal task
	// to the resource usage map for this job
	UpdateResourceUsage(taskResourceUsage map[string]float64)

	// GetResourceUsage gets the resource usage map for this job
	GetResourceUsage() map[string]float64

	// RecalculateResourceUsage recalculates the resource usage of a job
	// by adding together resource usage of all terminal tasks of this job.
	RecalculateResourceUsage(ctx context.Context)

	// CurrentState of the job.
	CurrentState() JobStateVector

	// GoalState of the job.
	GoalState() JobStateVector

	// Delete deletes the job from DB and clears the cache
	Delete(ctx context.Context) error

	// GetTaskStateCount returns the state/goal state count of all
	// tasks in a job, the total number of throttled tasks in
	// stateless jobs and the spread counts of a job
	GetTaskStateCount() (
		map[TaskStateSummary]int,
		int,
		JobSpreadCounts)

	// GetWorkflowStateCount returns the state count of all workflows in the cache
	GetWorkflowStateCount() map[pbupdate.State]int

	// RepopulateInstanceAvailabilityInfo repopulates the SLA information in the job cache
	RepopulateInstanceAvailabilityInfo(ctx context.Context) error

	// GetInstanceAvailabilityType return the instance availability type per instance
	// for the specified instances. If `instanceFilter` is empty then the instance
	// availability type for all instances of the job is returned
	GetInstanceAvailabilityType(
		ctx context.Context,
		instances ...uint32,
	) map[uint32]jobmgrcommon.InstanceAvailability_Type
}

// JobSpreadCounts contains task and host counts for jobs that use
// "spread" placement strategy. Counts are set to zero for
// invalid/inapplicable cases.
type JobSpreadCounts struct {
	// Number of tasks in a job that have been placed and
	// the number of unique hosts for those placements
	taskCount, hostCount int
}

// WorkflowOps defines operations on workflow
type WorkflowOps interface {
	// CreateWorkflow creates a workflow associated with
	// the calling object
	CreateWorkflow(
		ctx context.Context,
		workflowType models.WorkflowType,
		updateConfig *pbupdate.UpdateConfig,
		entityVersion *v1alphapeloton.EntityVersion,
		option ...Option,
	) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error)

	// PauseWorkflow pauses the current workflow, if any
	PauseWorkflow(
		ctx context.Context,
		entityVersion *v1alphapeloton.EntityVersion,
		option ...Option,
	) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error)

	// ResumeWorkflow resumes the current workflow, if any
	ResumeWorkflow(
		ctx context.Context,
		entityVersion *v1alphapeloton.EntityVersion,
		option ...Option,
	) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error)

	// AbortWorkflow aborts the current workflow, if any
	AbortWorkflow(
		ctx context.Context,
		entityVersion *v1alphapeloton.EntityVersion,
		option ...Option,
	) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error)

	// RollbackWorkflow rollbacks the current workflow, if any
	RollbackWorkflow(ctx context.Context) error

	// AddWorkflow add a workflow to the calling object
	AddWorkflow(updateID *peloton.UpdateID) Update

	// GetWorkflow gets the workflow to the calling object
	// it should only be used in place like handler, where
	// a read operation should not mutate cache
	GetWorkflow(updateID *peloton.UpdateID) Update

	// ClearWorkflow removes a workflow from the calling object
	ClearWorkflow(updateID *peloton.UpdateID)

	// GetAllWorkflows returns all workflows for the job
	GetAllWorkflows() map[string]Update

	// WriteWorkflowProgress updates the workflow status
	// based on update id
	WriteWorkflowProgress(
		ctx context.Context,
		updateID *peloton.UpdateID,
		state pbupdate.State,
		instancesDone []uint32,
		instanceFailed []uint32,
		instancesCurrent []uint32,
	) error
}

// JobConfigCache is a union of JobConfig
// and helper methods only available for cached config
type JobConfigCache interface {
	jobmgrcommon.JobConfig
	HasControllerTask() bool
}

// JobStateVector defines the state of a job.
// This encapsulates both the actual state and the goal state.
type JobStateVector struct {
	State        pbjob.JobState
	StateVersion uint64
}

// newJob creates a new cache job object
func newJob(id *peloton.JobID, jobFactory *jobFactory) *job {
	return &job{
		id: id,
		// jobFactory is stored in the job instead of using the singleton object
		// because job needs access to the different stores in the job factory
		// which are private variables and not available to other packages.
		jobFactory:    jobFactory,
		tasks:         map[uint32]*task{},
		resourceUsage: createEmptyResourceUsageMap(),
		workflows:     map[string]*update{},
	}
}

// cachedConfig structure holds the config fields need to be cached
type cachedConfig struct {
	instanceCount     uint32                  // Instance count in the job configuration
	sla               *pbjob.SlaConfig        // SLA configuration in the job configuration
	jobType           pbjob.JobType           // Job type (batch or service) in the job configuration
	changeLog         *peloton.ChangeLog      // ChangeLog in the job configuration
	respoolID         *peloton.ResourcePoolID // Resource Pool ID in the job configuration
	hasControllerTask bool                    // if the job contains any task which is controller task
	labels            []*peloton.Label        // Label of the job
	name              string                  // Name of the job
	placementStrategy pbjob.PlacementStrategy // Placement strategy
	owner             string                  // Owner of the job in the job configuration
	owningTeam        string                  // Owning team of the job in the job configuration
}

// job structure holds the information about a given active job
// in the cache. It should only hold information which either
// (i) a job manager component needs often and is expensive to
// fetch from the DB, or (ii) storing a view of underlying tasks
// which help with job lifecycle management.
type job struct {
	sync.RWMutex // Mutex to acquire before accessing any job information in cache

	id      *peloton.JobID     // The job identifier
	config  *cachedConfig      // The job config need to be cached
	runtime *pbjob.RuntimeInfo // Runtime information of the job

	// jobType is updated when a valid JobConfig is used to update
	// member 'config'. However unlike config, it does not get unset on
	// failures.
	jobType pbjob.JobType

	jobFactory *jobFactory // Pointer to the parent job factory object

	tasks map[uint32]*task // map of all job tasks

	// time at which the first mesos task update was received (indicates when a job starts running)
	firstTaskUpdateTime float64
	// time at which the last mesos task update was received (helps determine when job completes)
	lastTaskUpdateTime float64

	// The resource usage for this job. The map key is each resource kind
	// in string format and the map value is the number of unit-seconds
	// of that resource used by the job. Example: if a job has one task that
	// uses 1 CPU and finishes in 10 seconds, this map will contain <"cpu":10>
	resourceUsage map[string]float64

	workflows map[string]*update // map of all job workflows

	// instance availability information
	instanceAvailabilityInfo *instanceAvailabilityInfo

	// prevUpdateID and prevWorkflow keeps update cache from previous
	// workflow in memory before clearing from workflows map.
	prevUpdateID string
	prevWorkflow *update
}

// instanceAvailabilityInfo holds the instance availability information of the job
type instanceAvailabilityInfo struct {
	// Instances that are preempted/explicitly killed.
	// These are not included in instance availability calculations
	killedInstances map[uint32]bool
	// Instances that are unavailable
	unavailableInstances map[uint32]bool
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) AddTask(
	ctx context.Context,
	id uint32) (instance Task, err error) {
	if t := j.GetTask(id); t != nil {
		return t, nil
	}

	defer func() {
		j.updateInstanceAvailabilityInfoForInstances(ctx, []uint32{id}, err != nil)
	}()

	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTask(j.ID(), id, j.jobFactory, j.jobType)

		// first fetch the runtime of the task
		_, err := t.GetRuntime(ctx)
		if err != nil {
			// if task runtime is not found and instance id is larger than
			// instance count, then throw a different error
			if !yarpcerrors.IsNotFound(err) {
				return nil, err
			}

			// validate that the task being added is within
			// the instance count of the job.
			if err := j.populateCurrentJobConfig(ctx); err != nil {
				return nil, err
			}

			if j.config.GetInstanceCount() <= id {
				return nil, InstanceIDExceedsInstanceCountError
			}
			return nil, err
		}
		// store the task with the job
		j.tasks[id] = t
	}
	return t, nil
}

// CreateTaskConfigs creates task configurations in the DB
func (j *job) CreateTaskConfigs(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) error {
	if jobConfig.GetDefaultConfig() != nil {
		// Create default task config in DB
		if err := j.jobFactory.taskConfigV2Ops.Create(
			ctx,
			jobID,
			common.DefaultTaskConfigID,
			jobConfig.GetDefaultConfig(),
			configAddOn,
			spec.GetDefaultSpec(),
			jobConfig.GetChangeLog().GetVersion(),
		); err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      j.ID().GetValue(),
					"instance_id": common.DefaultTaskConfigID,
				}).Info("failed to write default task config")
			return yarpcerrors.InternalErrorf(err.Error())
		}
	}

	createSingleTaskConfig := func(id uint32) error {
		var cfg *pbtask.TaskConfig
		var instanceSpec, podSpec *pbpod.PodSpec
		var ok bool
		if cfg, ok = jobConfig.GetInstanceConfig()[id]; !ok {
			return yarpcerrors.NotFoundErrorf(
				"failed to get instance config for instance %v", id,
			)
		}
		taskConfig := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)

		if spec != nil {
			// The assumption here is that if the spec is present, it has
			// already been converted to v0 JobConfig. So the id can be
			// used to retrieve InstanceSpec in the same way as InstanceConfig
			if instanceSpec, ok = spec.GetInstanceSpec()[id]; !ok {
				return yarpcerrors.NotFoundErrorf(
					"failed to get pod spec for instance %v", id,
				)
			}
			podSpec = taskconfig.MergePodSpec(
				spec.GetDefaultSpec(),
				instanceSpec,
			)
		}

		return j.jobFactory.taskConfigV2Ops.Create(
			ctx,
			jobID,
			int64(id),
			taskConfig,
			configAddOn,
			podSpec,
			jobConfig.GetChangeLog().GetVersion(),
		)
	}

	var instanceIDList []uint32
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		if _, ok := jobConfig.GetInstanceConfig()[i]; ok {
			instanceIDList = append(instanceIDList, i)
		}
	}

	return util.RunInParallel(
		j.ID().GetValue(),
		instanceIDList,
		createSingleTaskConfig)
}

func (j *job) CreateTaskRuntimes(
	ctx context.Context,
	runtimes map[uint32]*pbtask.RuntimeInfo,
	owner string) error {
	createSingleTask := func(id uint32) error {
		runtime := runtimes[id]
		now := time.Now().UTC()
		runtime.Revision = &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		}

		if j.GetTask(id) != nil {
			return yarpcerrors.AlreadyExistsErrorf("task %d already exists", id)
		}

		t := j.addTaskToJobMap(id)
		return t.createTask(ctx, runtime, owner)
	}
	return util.RunInParallel(
		j.ID().GetValue(),
		getIdsFromRuntimeMap(runtimes),
		createSingleTask)
}

func (j *job) PatchTasks(
	ctx context.Context,
	runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
	force bool,
) (instancesSucceeded []uint32, instancesToBeRetried []uint32, err error) {
	defer func() {
		j.updateInstanceAvailabilityInfoForInstances(ctx, instancesSucceeded, err != nil)
	}()

	runtimesToPatch := runtimeDiffs
	if j.jobType == pbjob.JobType_SERVICE && !force {
		// Stateless jobs are patched in SLA aware manner unless force flag is set
		runtimesToPatch, instancesToBeRetried, err = j.filterRuntimeDiffsBySLA(ctx, runtimeDiffs)
		if err != nil {
			return nil, nil, err
		}
	}

	instancesSucceeded = getIdsFromDiffs(runtimesToPatch)

	patchSingleTask := func(id uint32) error {
		t, err := j.AddTask(ctx, id)
		if err != nil {
			return err
		}
		return t.(*task).patchTask(ctx, runtimeDiffs[id])
	}

	err = util.RunInParallel(
		j.ID().GetValue(),
		instancesSucceeded,
		patchSingleTask)

	return instancesSucceeded, instancesToBeRetried, err
}

func (j *job) ReplaceTasks(
	taskInfos map[uint32]*pbtask.TaskInfo,
	forceReplace bool) (err error) {

	var instancesReplaced []uint32

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), _defaultTimeout)
		defer cancel()

		j.updateInstanceAvailabilityInfoForInstances(ctx, instancesReplaced, err != nil)
	}()

	replaceSingleTask := func(id uint32) error {
		t := j.addTaskToJobMap(id)
		return t.replaceTask(
			taskInfos[id].GetRuntime(),
			taskInfos[id].GetConfig(),
			forceReplace,
		)
	}

	instancesReplaced = getIdsFromTaskInfoMap(taskInfos)
	err = util.RunInParallel(
		j.ID().GetValue(),
		instancesReplaced,
		replaceSingleTask)

	return err
}

func (j *job) GetTask(id uint32) Task {
	j.RLock()
	defer j.RUnlock()

	if t, ok := j.tasks[id]; ok {
		return t
	}

	return nil
}

func (j *job) RemoveTask(id uint32) {
	j.Lock()
	defer j.Unlock()

	if t, ok := j.tasks[id]; ok {
		t.deleteTask()
	}

	delete(j.tasks, id)
}

func (j *job) GetAllTasks() map[uint32]Task {
	j.RLock()
	defer j.RUnlock()
	taskMap := make(map[uint32]Task)
	for k, v := range j.tasks {
		taskMap[k] = v
	}
	return taskMap
}

func (j *job) Create(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) error {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	if config == nil {
		return yarpcerrors.InvalidArgumentErrorf("missing config in jobInfo")
	}

	config = populateConfigChangeLog(config)

	// Add jobID to active jobs table before creating job runtime. This should
	// happen every time a job is first created.
	if err := j.jobFactory.activeJobsOps.Create(
		ctx, j.ID()); err != nil {
		j.invalidateCache()
		return err
	}

	// create job runtime and set state to UNINITIALIZED
	if err := j.createJobRuntime(ctx, config, nil); err != nil {
		j.invalidateCache()
		return err
	}

	// create job name to job id mapping.
	// if the creation fails here, since job config is not created yet,
	// the job will be cleaned up in goalstate engine JobRecover action.
	if config.GetType() == pbjob.JobType_SERVICE {
		if err := j.jobFactory.jobNameToIDOps.Create(
			ctx,
			config.GetName(),
			j.ID(),
		); err != nil {
			j.invalidateCache()
			return err
		}
	}

	// create job config
	if err := j.createJobConfig(ctx, config, configAddOn, spec); err != nil {
		j.invalidateCache()
		return err
	}
	jobTypeCopy = j.jobType

	// both config and runtime are created, move the state to INITIALIZED
	j.runtime.State = pbjob.JobState_INITIALIZED
	if err := j.jobFactory.jobRuntimeOps.Upsert(
		ctx,
		j.id,
		j.runtime); err != nil {
		j.invalidateCache()
		return err
	}

	if err := j.jobFactory.jobIndexOps.Create(
		ctx, j.ID(),
		config,
		j.runtime,
	); err != nil {
		j.invalidateCache()
		return err
	}

	// create JobSummary and WorkflowStatus while we have the lock
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

	return nil
}

func (j *job) RollingCreate(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
	updateConfig *pbupdate.UpdateConfig,
	opaqueData *peloton.OpaqueData,
) error {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	if config == nil {
		return yarpcerrors.InvalidArgumentErrorf("missing config in jobInfo")
	}

	if updateConfig.GetRollbackOnFailure() == true {
		return yarpcerrors.InvalidArgumentErrorf("job creation cannot rollback on failure")
	}

	// Add jobID to active jobs table before creating job runtime. This should
	// happen every time a job is first created.
	if err := j.jobFactory.activeJobsOps.Create(
		ctx, j.ID()); err != nil {
		j.invalidateCache()
		return err
	}

	config = populateConfigChangeLog(config)

	// dummy config is used as the starting config for update workflow
	dummyConfig := proto.Clone(config).(*pbjob.JobConfig)
	dummyConfig.InstanceCount = 0
	dummyConfig.ChangeLog.Version = jobmgrcommon.DummyConfigVersion
	dummyConfig.DefaultConfig = nil
	dummyConfig.InstanceConfig = nil

	instancesAdded := make([]uint32, config.InstanceCount)
	for i := uint32(0); i < config.InstanceCount; i++ {
		instancesAdded[i] = i
	}

	// create workflow which is going to initialize the job
	updateID := &peloton.UpdateID{Value: uuid.New()}

	// create job runtime and set state to UNINITIALIZED with updateID,
	// so on error recovery, update config such as batch size can be
	// recovered
	if err := j.createJobRuntime(ctx, config, updateID); err != nil {
		j.invalidateCache()
		return err
	}

	// create job name to job id mapping.
	// if the creation fails here, since job config is not created yet,
	// the job will be cleaned up in goalstate engine JobRecover action.
	if config.GetType() == pbjob.JobType_SERVICE {
		if err := j.jobFactory.jobNameToIDOps.Create(
			ctx,
			config.GetName(),
			j.ID(),
		); err != nil {
			j.invalidateCache()
			return err
		}
	}

	newWorkflow := newUpdate(updateID, j.jobFactory)
	if err := newWorkflow.Create(
		ctx,
		j.id,
		config,
		dummyConfig,
		configAddOn,
		instancesAdded,
		nil,
		nil,
		models.WorkflowType_UPDATE,
		updateConfig,
		opaqueData,
	); err != nil {
		j.invalidateCache()
		return err
	}

	// create the dummy config in db, it is possible that the dummy config already
	// exists in db when doing error retry. So ignore already exist error here
	if err := j.createJobConfig(ctx, dummyConfig, configAddOn, nil); err != nil &&
		!yarpcerrors.IsAlreadyExists(errors.Cause(err)) {
		j.invalidateCache()
		return err
	}

	// create the real config as the target config for update workflow.
	// Once the config is persisted successfully in db, the job is considered
	// as created successfully, and should be able to recover from
	// rest of the error. Calling RollingCreate after this call succeeds again,
	// would result in AlreadyExist error
	if err := j.createJobConfig(ctx, config, configAddOn, spec); err != nil {
		j.invalidateCache()
		return err
	}
	jobTypeCopy = j.jobType

	// both config and runtime are created, move the state to PENDING
	j.runtime.State = pbjob.JobState_PENDING
	if err := j.jobFactory.jobRuntimeOps.Upsert(
		ctx,
		j.id,
		j.runtime); err != nil {
		j.invalidateCache()
		return err
	}

	if err := j.jobFactory.jobIndexOps.Create(
		ctx,
		j.id,
		config,
		j.runtime,
	); err != nil {
		j.invalidateCache()
		return err
	}

	j.workflows[updateID.GetValue()] = newWorkflow

	// create JobSummary and WorkflowStatus while we have the lock
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, updateID)

	return nil
}

func (j *job) CompareAndSetRuntime(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error) {
	if jobRuntime == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("unexpected nil jobRuntime")
	}

	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	// first make sure we have job runtime in cache
	if err := j.populateRuntime(ctx); err != nil {
		return nil, err
	}

	if j.runtime.GetGoalState() == pbjob.JobState_DELETED &&
		jobRuntime.GetGoalState() != j.runtime.GetGoalState() {
		return nil, _updateDeleteJobErr

	}

	if j.runtime.GetRevision().GetVersion() !=
		jobRuntime.GetRevision().GetVersion() {
		j.invalidateCache()
		return nil, jobmgrcommon.UnexpectedVersionError
	}

	// version matches, update the input changeLog
	newRuntime := *jobRuntime
	newRuntime.Revision = &peloton.ChangeLog{
		Version:   jobRuntime.GetRevision().GetVersion() + 1,
		CreatedAt: jobRuntime.GetRevision().GetCreatedAt(),
		UpdatedAt: uint64(time.Now().UnixNano()),
	}

	if err := j.jobFactory.jobRuntimeOps.Upsert(
		ctx,
		j.id,
		&newRuntime,
	); err != nil {
		j.invalidateCache()
		return nil, err
	}
	if err := j.jobFactory.jobIndexOps.Update(
		ctx,
		j.id,
		nil,
		&newRuntime,
	); err != nil {
		j.invalidateCache()
		return nil, err
	}

	j.runtime = &newRuntime
	runtimeCopy := proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

	return runtimeCopy, nil
}

func (j *job) CompareAndSetConfig(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) (jobmgrcommon.JobConfig, error) {
	j.Lock()
	defer j.Unlock()

	return j.compareAndSetConfig(ctx, config, configAddOn, spec)
}

// CompareAndSetTask replaces the existing task runtime in DB and cache.
// It uses RuntimeInfo.Revision.Version for concurrency control, and it would
// update RuntimeInfo.Revision.Version automatically upon success.
// Caller should not manually modify the value of RuntimeInfo.Revision.Version.
// The `force` flag affects only stateless jobs. By default (with force flag
// not set), for stateless job, if the task is becoming unavailable due to
// host maintenance and update, then runtime is set only if it does not
// violate the job SLA. If `force` flag is set, the task runtime will
// be set even if it violates job SLA.
func (j *job) CompareAndSetTask(
	ctx context.Context,
	id uint32,
	runtime *pbtask.RuntimeInfo,
	force bool,
) (runtimeCopy *pbtask.RuntimeInfo, err error) {
	defer func() {
		j.updateInstanceAvailabilityInfoForInstances(ctx, []uint32{id}, err != nil)
	}()

	t, err := j.AddTask(ctx, id)
	if err != nil {
		return nil, err
	}

	if j.jobType == pbjob.JobType_SERVICE && !force {
		j.Lock()
		defer j.Unlock()

		instanceAvailabilityInfo, err := j.getInstanceAvailabilityInfo(ctx)
		if err != nil {
			return nil, err
		}

		if err = j.populateCurrentJobConfig(ctx); err != nil {
			return nil, errors.Wrap(err, "failed to populate job config")
		}

		if runtime.GetTerminationStatus() != nil {
			switch runtime.GetTerminationStatus().GetReason() {
			// If restart/kill is due to host-maintenance,
			// skip doing so if SLA is violated
			case pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE:
				// if SLA is defined (and MaximumUnavailableInstances
				// is non zero), check for SLA violation
				if j.config.GetSLA().GetMaximumUnavailableInstances() != 0 {
					if !instanceAvailabilityInfo.unavailableInstances[id] &&
						uint32(len(instanceAvailabilityInfo.unavailableInstances)) >=
							j.config.GetSLA().GetMaximumUnavailableInstances() {
						// return the existing runtime so that the caller can look at the
						// revision and determine the new runtime did not get set
						return t.GetRuntime(ctx)
					}
				}
			}
		}
	}

	runtimeCopy, err = t.(*task).compareAndSetTask(ctx, runtime, j.jobType)
	return runtimeCopy, err
}

// CurrentState of the job.
func (j *job) CurrentState() JobStateVector {
	j.RLock()
	defer j.RUnlock()

	return JobStateVector{
		State:        j.runtime.GetState(),
		StateVersion: j.runtime.GetStateVersion(),
	}
}

// GoalState of the job.
func (j *job) GoalState() JobStateVector {
	j.RLock()
	defer j.RUnlock()

	return JobStateVector{
		State:        j.runtime.GetGoalState(),
		StateVersion: j.runtime.GetDesiredStateVersion(),
	}
}

// The runtime being passed should only set the fields which the caller intends to change,
// the remaining fields should be left unfilled.
// The config would be updated to the config passed in (except changeLog)
func (j *job) Update(
	ctx context.Context,
	jobInfo *pbjob.JobInfo,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
	req UpdateRequest) error {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	var (
		updatedConfig *pbjob.JobConfig
		err           error
	)

	if jobInfo.GetConfig() != nil {
		if configAddOn == nil {
			return fmt.Errorf(
				"ConfigAddOn cannot be nil when 'JobInfo.JobConfig' is not nil")
		}
		if req == UpdateCacheOnly {
			// overwrite the cache after validating that
			// version is either the same or increasing
			if j.config == nil || j.config.changeLog.GetVersion() <=
				jobInfo.GetConfig().GetChangeLog().GetVersion() {
				j.populateJobConfigCache(jobInfo.GetConfig())
			}
		} else {
			updatedConfig, err = j.getUpdatedJobConfigCache(
				ctx, jobInfo.GetConfig(), req)
			if err != nil {
				// invalidate cache if error not from validation failure
				if !yarpcerrors.IsInvalidArgument(err) {
					j.invalidateCache()
				}
				return err
			}
			if updatedConfig != nil {
				j.populateJobConfigCache(updatedConfig)
			}
		}
	}

	var updatedRuntime *pbjob.RuntimeInfo
	if jobInfo.GetRuntime() != nil {
		updatedRuntime, err = j.getUpdatedJobRuntimeCache(ctx, jobInfo.GetRuntime(), req)
		if err != nil {
			if err != _updateDeleteJobErr {
				j.invalidateCache()
			}
			return err
		}
		if updatedRuntime != nil {
			j.runtime = updatedRuntime
		}
	}

	if req == UpdateCacheAndDB {
		// Must update config first then runtime. Update config would create a
		// new config entry and update runtime would ask job to use the latest
		// config. If we update the runtime first successfully, and update
		// config with failure, job would try to access a non-existent config.
		if updatedConfig != nil {
			// Create a new versioned, entry for job_config, so this is not
			// an Update
			if err := j.jobFactory.jobConfigOps.Create(
				ctx,
				j.ID(),
				updatedConfig,
				configAddOn,
				spec,
				updatedConfig.GetChangeLog().GetVersion(),
			); err != nil {
				j.invalidateCache()
				return err
			}
		}

		if updatedRuntime != nil {
			err := j.jobFactory.jobRuntimeOps.Upsert(ctx, j.ID(), updatedRuntime)
			if err != nil {
				j.invalidateCache()
				return err
			}

			jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())
		}

		if updatedConfig != nil || updatedRuntime != nil {
			if err := j.jobFactory.jobIndexOps.Update(
				ctx,
				j.ID(),
				updatedConfig,
				updatedRuntime,
			); err != nil {
				j.invalidateCache()
				jobSummaryCopy = nil
				updateModelCopy = nil
				return err
			}
		}
	}
	jobTypeCopy = j.jobType
	return nil
}

func (j *job) SetTaskUpdateTime(t *float64) {
	j.Lock()
	defer j.Unlock()

	if j.firstTaskUpdateTime == 0 {
		j.firstTaskUpdateTime = *t
	}

	j.lastTaskUpdateTime = *t
}

func (j *job) IsPartiallyCreated(config jobmgrcommon.JobConfig) bool {
	j.RLock()
	defer j.RUnlock()

	// While the instance count is being reduced in an update,
	// the number of instance in the cache will exceed the instance
	// count in the configuration.
	if config.GetInstanceCount() <= uint32(len(j.tasks)) {
		return false
	}
	return true
}

func (j *job) GetRuntime(ctx context.Context) (*pbjob.RuntimeInfo, error) {
	j.Lock()
	defer j.Unlock()

	if err := j.populateRuntime(ctx); err != nil {
		return nil, err
	}

	runtime := proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	return runtime, nil
}

func (j *job) GetConfig(ctx context.Context) (jobmgrcommon.JobConfig, error) {
	j.Lock()
	defer j.Unlock()

	if err := j.populateCurrentJobConfig(ctx); err != nil {
		return nil, err
	}
	return j.config, nil
}

func (j *job) GetJobType() pbjob.JobType {
	j.RLock()
	defer j.RUnlock()

	if j.config != nil {
		return j.config.jobType
	}

	// service jobs are optimized for lower latency (e.g. job runtime
	// updater is run more frequently for service jobs than batch jobs,
	// service jobs may have higher priority).
	// For a short duration, when cache does not have the config, running
	// batch jobs as service jobs is ok, but running service jobs as batch
	// jobs will create problems. Therefore, default to SERVICE type.
	return pbjob.JobType_SERVICE
}

func (j *job) GetFirstTaskUpdateTime() float64 {
	j.RLock()
	defer j.RUnlock()

	return j.firstTaskUpdateTime
}

func (j *job) GetLastTaskUpdateTime() float64 {
	j.RLock()
	defer j.RUnlock()

	return j.lastTaskUpdateTime
}

func (j *job) GetCachedConfig() jobmgrcommon.JobConfig {
	j.RLock()
	defer j.RUnlock()
	if j == nil || j.config == nil {
		return nil
	}

	return j.config
}

// RepopulateInstanceAvailabilityInfo repopulates the instance availability information in the job cache
func (j *job) RepopulateInstanceAvailabilityInfo(ctx context.Context) error {
	if j.jobType != pbjob.JobType_SERVICE {
		return nil
	}

	j.Lock()
	defer j.Unlock()

	return j.populateInstanceAvailabilityInfo(ctx)
}

// GetInstanceAvailabilityType return the instance availability type per instance
// for the specified instances. If no instances are specified then the instance
// availability type for all instances of the job is returned
func (j *job) GetInstanceAvailabilityType(
	ctx context.Context,
	instances ...uint32,
) map[uint32]jobmgrcommon.InstanceAvailability_Type {
	j.RLock()
	defer j.RUnlock()

	instanceAvailability := make(map[uint32]jobmgrcommon.InstanceAvailability_Type)
	instancesToFilter := instances
	if len(instancesToFilter) == 0 {
		for i := range j.tasks {
			instancesToFilter = append(instancesToFilter, i)
		}
	}

	for _, i := range instancesToFilter {
		instanceAvailability[i] = jobmgrcommon.InstanceAvailability_INVALID
		if _, ok := j.tasks[i]; !ok {
			continue
		}

		runtime, err := j.tasks[i].GetRuntime(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"job_id":      j.id.GetValue(),
				"instance_id": i,
			}).Error("failed to get task runtime")
			continue
		}

		currentState := &TaskStateVector{
			State:         runtime.GetState(),
			MesosTaskID:   runtime.GetMesosTaskId(),
			ConfigVersion: runtime.GetConfigVersion(),
		}

		goalState := &TaskStateVector{
			State:         runtime.GetGoalState(),
			MesosTaskID:   runtime.GetDesiredMesosTaskId(),
			ConfigVersion: runtime.GetDesiredConfigVersion(),
		}

		instanceAvailability[i] = getInstanceAvailability(
			currentState,
			goalState,
			runtime.GetHealthy(),
			runtime.GetTerminationStatus(),
		)
	}

	return instanceAvailability
}

// populateCurrentJobConfig populates the config pointed by runtime config version
// into cache
func (j *job) populateCurrentJobConfig(ctx context.Context) error {
	if err := j.populateRuntime(ctx); err != nil {
		return err
	}

	// repopulate the config when config is not present or
	// the version mismatches withe job runtime configuration version
	if j.config == nil ||
		j.config.GetChangeLog().GetVersion() !=
			j.runtime.GetConfigurationVersion() {
		config, _, err := j.jobFactory.jobConfigOps.Get(
			ctx,
			j.ID(),
			j.runtime.GetConfigurationVersion(),
		)
		if err != nil {
			return err
		}
		j.populateJobConfigCache(config)
	}
	return nil
}

// addTaskToJobMap is a private API to add a task to job map
func (j *job) addTaskToJobMap(id uint32) *task {
	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTask(j.ID(), id, j.jobFactory, j.jobType)
	}

	j.tasks[id] = t
	return t
}

// createJobConfig creates job config in db and cache
func (j *job) createJobConfig(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) error {
	if err := j.jobFactory.jobConfigOps.Create(
		ctx,
		j.ID(),
		config,
		configAddOn,
		spec,
		config.ChangeLog.Version,
	); err != nil {
		return err
	}
	j.populateJobConfigCache(config)
	return nil
}

// createJobRuntime creates job runtime in db and cache,
// job state is set to UNINITIALIZED, because job config is persisted after
// calling createJobRuntime and job creation is not complete
func (j *job) createJobRuntime(ctx context.Context, config *pbjob.JobConfig, updateID *peloton.UpdateID) error {
	goalState := goalstateutil.GetDefaultJobGoalState(config.Type)
	now := time.Now().UTC()
	initialJobRuntime := &pbjob.RuntimeInfo{
		State:        pbjob.JobState_UNINITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: config.GetChangeLog().GetVersion(),
		ResourceUsage:        createEmptyResourceUsageMap(),
		WorkflowVersion:      1,
		StateVersion:         1,
		DesiredStateVersion:  1,
		UpdateID:             updateID,
	}
	// Init the task stats to reflect that all tasks are in initialized state
	initialJobRuntime.TaskStats[pbtask.TaskState_INITIALIZED.String()] = config.InstanceCount

	if err := j.jobFactory.jobRuntimeOps.Upsert(
		ctx,
		j.ID(),
		initialJobRuntime,
	); err != nil {
		return err
	}
	j.runtime = initialJobRuntime
	return nil
}

func (j *job) compareAndSetConfig(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) (jobmgrcommon.JobConfig, error) {
	// first make sure current config is in cache
	if err := j.populateCurrentJobConfig(ctx); err != nil {
		return nil, err
	}

	// then validate and merge config
	updatedConfig, err := j.validateAndMergeConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	// updatedConfig now contains updated version number and timestamp.
	// If the job spec is provided, the spec should reflect the same version
	// and time.
	if spec != nil {
		spec.Revision = &v1alphapeloton.Revision{
			Version:   updatedConfig.GetChangeLog().GetVersion(),
			CreatedAt: updatedConfig.GetChangeLog().GetCreatedAt(),
			UpdatedAt: updatedConfig.GetChangeLog().GetUpdatedAt(),
		}
	}

	// write the config into DB
	if err := j.jobFactory.jobConfigOps.Create(
		ctx,
		j.ID(),
		updatedConfig,
		configAddOn,
		spec,
		updatedConfig.GetChangeLog().GetVersion(),
	); err != nil {
		j.invalidateCache()
		return nil, err
	}
	if err := j.jobFactory.jobIndexOps.Update(
		ctx,
		j.ID(),
		updatedConfig,
		nil,
	); err != nil {
		j.invalidateCache()
		return nil, err
	}

	// finally update the cache
	j.populateJobConfigCache(updatedConfig)

	return j.config, nil
}

// updateInstanceAvailabilityInfoForInstances updates the
// instanceAvailabilityInfo for the instances
func (j *job) updateInstanceAvailabilityInfoForInstances(
	ctx context.Context,
	instances []uint32,
	invalidateInstanceAvailabilityInfo bool,
) {
	if j.jobType != pbjob.JobType_SERVICE {
		return
	}

	j.Lock()
	defer j.Unlock()

	if invalidateInstanceAvailabilityInfo {
		j.invalidateInstanceAvailabilityInfo()
		return
	}

	for _, id := range instances {
		t := j.tasks[id]
		if t == nil {
			j.invalidateInstanceAvailabilityInfo()
			return
		}

		runtime, err := t.GetRuntime(ctx)
		if err != nil || runtime == nil {
			j.invalidateInstanceAvailabilityInfo()
			return
		}

		currentState := &TaskStateVector{
			State:         runtime.GetState(),
			ConfigVersion: runtime.GetConfigVersion(),
			MesosTaskID:   runtime.GetMesosTaskId(),
		}

		goalState := &TaskStateVector{
			State:         runtime.GetGoalState(),
			ConfigVersion: runtime.GetDesiredConfigVersion(),
			MesosTaskID:   runtime.GetDesiredMesosTaskId(),
		}

		j.updateInstanceAvailabilityInfo(
			ctx,
			id,
			currentState,
			goalState,
			runtime.GetHealthy(),
			t.TerminationStatus(),
		)
	}
}

// filterRuntimeDiffsBySLA runs through the given runtimeDiffs and returns the following
// 1. runtimeDiffs which do not violate job SLA
// 2. list of instances whose state is unknown to cache. These should be
//    retried after reloading their runtimes into cache
func (j *job) filterRuntimeDiffsBySLA(
	ctx context.Context,
	runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
) (map[uint32]jobmgrcommon.RuntimeDiff, []uint32, error) {
	j.Lock()
	defer j.Unlock()

	instanceAvailabilityInfo, err := j.getInstanceAvailabilityInfo(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err = j.populateCurrentJobConfig(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "failed to populate job config")
	}

	log.WithFields(log.Fields{
		"killed_instances":      instanceAvailabilityInfo.killedInstances,
		"unavailable_instances": instanceAvailabilityInfo.unavailableInstances,
		"runtime_diffs":         runtimeDiffs,
	}).Debug("instance availability before patch")

	runtimesToPatch := make(map[uint32]jobmgrcommon.RuntimeDiff)
	var instancesToBeRetried []uint32
	for i, runtimeDiff := range runtimeDiffs {
		t := j.tasks[i]
		taskCurrentState := t.CurrentState()
		taskGoalState := t.GoalState()
		if goalState, ok := runtimeDiff[jobmgrcommon.GoalStateField]; ok &&
			goalState.(pbtask.TaskState) == pbtask.TaskState_DELETED {

			delete(instanceAvailabilityInfo.killedInstances, i)
			delete(instanceAvailabilityInfo.unavailableInstances, i)

			runtimesToPatch[i] = runtimeDiff
			continue
		}

		if goalState, ok := runtimeDiff[jobmgrcommon.GoalStateField]; ok &&
			goalState.(pbtask.TaskState) == pbtask.TaskState_KILLED {

			delete(instanceAvailabilityInfo.unavailableInstances, i)
			instanceAvailabilityInfo.killedInstances[i] = true

			runtimesToPatch[i] = runtimeDiff
			continue
		}

		if termStatus := runtimeDiff[jobmgrcommon.TerminationStatusField]; termStatus != nil {
			reason := termStatus.(*pbtask.TerminationStatus).GetReason()

			switch reason {
			// If restart/kill is due to host-maintenance,
			// skip doing so if SLA is violated
			case pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
				pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART:
				if j.config.GetSLA().GetMaximumUnavailableInstances() != 0 {
					// if SLA is defined (and MaximumUnavailableInstances
					// is non zero), check for SLA violation
					if !instanceAvailabilityInfo.unavailableInstances[i] &&
						uint32(len(instanceAvailabilityInfo.unavailableInstances)) >=
							j.config.GetSLA().GetMaximumUnavailableInstances() {
						continue
					}
				}

				delete(instanceAvailabilityInfo.killedInstances, i)
				instanceAvailabilityInfo.unavailableInstances[i] = true

			// If restart/kill is due to job update or if the instance has failed,
			// mark the instance unavailable
			case pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
				pbtask.TerminationStatus_TERMINATION_STATUS_REASON_FAILED:
				delete(instanceAvailabilityInfo.killedInstances, i)
				instanceAvailabilityInfo.unavailableInstances[i] = true

			default:
				delete(instanceAvailabilityInfo.unavailableInstances, i)
				instanceAvailabilityInfo.killedInstances[i] = true
			}

			runtimesToPatch[i] = runtimeDiff
			continue
		}

		if taskCurrentState.State == pbtask.TaskState_UNKNOWN ||
			taskGoalState.State == pbtask.TaskState_UNKNOWN {
			instancesToBeRetried = append(instancesToBeRetried, i)
			continue
		}

		if desiredMesosTaskID := runtimeDiff[jobmgrcommon.DesiredMesosTaskIDField]; desiredMesosTaskID != nil &&
			desiredMesosTaskID.(*mesos.TaskID).GetValue() != taskGoalState.MesosTaskID.GetValue() {

			// If the desired mesos-task-id is being modified and
			// the termination status is not set, mark the instance KILLED.
			// Ideally we shouldn't hit this path since we always set the
			// termination status when changing the desired mesos task
			delete(instanceAvailabilityInfo.unavailableInstances, i)
			instanceAvailabilityInfo.killedInstances[i] = true

		} else if desiredConfigVersion, ok := runtimeDiff[jobmgrcommon.DesiredConfigVersionField]; ok &&
			desiredConfigVersion.(uint64) != taskGoalState.ConfigVersion {
			// if the desired config version is being changed, we need to mark
			// instance as KILLED. The only exception is when both config
			// version and desired config version are being set to the same
			// value (for unchanged instances in update) since it doesn't
			// affect instance availability
			if configVersion, ok := runtimeDiff[jobmgrcommon.ConfigVersionField]; !ok ||
				configVersion.(uint64) != desiredConfigVersion.(uint64) {
				delete(instanceAvailabilityInfo.unavailableInstances, i)
				instanceAvailabilityInfo.killedInstances[i] = true
			}

		} else {
			goalState, ok := runtimeDiff[jobmgrcommon.GoalStateField]
			if ok && goalState.(pbtask.TaskState) == pbtask.TaskState_RUNNING ||
				taskGoalState.State == pbtask.TaskState_RUNNING {
				// if the task goal state is being set to RUNNING or if the task
				// is already RUNNING, mark it unavailable. If the task is
				// incorrectly marked unavailable (say when updating HealthState),
				// it'll be updated once the task has been patched (when
				// instanceAvailabilityInfo is updated)
				delete(instanceAvailabilityInfo.killedInstances, i)
				instanceAvailabilityInfo.unavailableInstances[i] = true
			}
		}

		runtimesToPatch[i] = runtimeDiff
	}

	log.WithFields(log.Fields{
		"killed_instances":          instanceAvailabilityInfo.killedInstances,
		"unavailable_instances":     instanceAvailabilityInfo.unavailableInstances,
		"max_unavailable_instances": j.config.GetSLA().GetMaximumUnavailableInstances(),
	}).Debug("instance availability after change")

	return runtimesToPatch, instancesToBeRetried, nil
}

// getUpdatedJobConfigCache validates the config input and
// returns updated config. return value is nil, if validation
// fails
func (j *job) getUpdatedJobConfigCache(
	ctx context.Context,
	config *pbjob.JobConfig,
	req UpdateRequest) (*pbjob.JobConfig, error) {
	if req == UpdateCacheAndDB {
		if j.config == nil {
			runtime, err := j.jobFactory.jobRuntimeOps.Get(
				ctx,
				j.ID(),
			)
			if err != nil {
				return nil, err
			}
			config, _, err := j.jobFactory.jobConfigOps.Get(
				ctx,
				j.ID(),
				runtime.GetConfigurationVersion(),
			)
			if err != nil {
				return nil, err
			}
			j.populateJobConfigCache(config)
		}
	}

	updatedConfig := config
	var err error

	if j.config != nil {
		updatedConfig, err = j.validateAndMergeConfig(ctx, config)
		if err != nil {
			return nil, err
		}
	}

	return updatedConfig, nil
}

// validateAndMergeConfig validates whether the input config should be merged,
// and returns the merged config if merge is valid.
func (j *job) validateAndMergeConfig(
	ctx context.Context,
	config *pbjob.JobConfig,
) (*pbjob.JobConfig, error) {
	if err := j.validateConfig(config); err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"current_revision": j.config.GetChangeLog().GetVersion(),
				"new_revision":     config.GetChangeLog().GetVersion(),
				"job_id":           j.id.Value}).
			Info("failed job config validation")
		return nil, err
	}

	newConfig := *config

	// GetMaxJobConfigVersion should still go through legacy jobStore interface
	// since ORM does not intend to support this. Once we remove CAS writes,
	// this sort of concurrency control is not required
	maxVersion, err := j.jobFactory.jobStore.GetMaxJobConfigVersion(ctx, j.id.GetValue())
	if err != nil {
		return nil, err
	}

	currentChangeLog := *j.config.changeLog
	newConfig.ChangeLog = &currentChangeLog
	newConfig.ChangeLog.Version = maxVersion + 1
	newConfig.ChangeLog.UpdatedAt = uint64(time.Now().UnixNano())
	return &newConfig, nil
}

// validateConfig validates whether the input config is valid
// to update the exisiting config cache
func (j *job) validateConfig(newConfig *pbjob.JobConfig) error {
	currentConfig := j.config

	if newConfig == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"no job configuration provided")
	}

	// changeLog is not nil, the version in the new config should
	// match the current config version
	if newConfig.GetChangeLog() != nil {
		// Make sure that not overwriting with old or same version
		if newConfig.GetChangeLog().GetVersion() != currentConfig.GetChangeLog().GetVersion() {
			return yarpcerrors.InvalidArgumentErrorf(
				"invalid job configuration version")
		}
	}
	return nil
}

// populateJobConfigCache update the cache in job cache
func (j *job) populateJobConfigCache(config *pbjob.JobConfig) {
	if config == nil {
		return
	}

	if j.config == nil {
		j.config = &cachedConfig{}
	}

	j.config.instanceCount = config.GetInstanceCount()

	if config.GetSLA() != nil {
		j.config.sla = config.GetSLA()
	}

	if config.GetChangeLog() != nil {
		j.config.changeLog = config.GetChangeLog()
	}

	if config.GetRespoolID() != nil {
		j.config.respoolID = config.GetRespoolID()
	}

	if config.GetLabels() != nil {
		var copy []*peloton.Label
		for _, l := range config.GetLabels() {
			copy = append(copy, &peloton.Label{Key: l.GetKey(), Value: l.GetValue()})
		}
		j.config.labels = copy
	}

	j.config.name = config.GetName()

	j.config.hasControllerTask = hasControllerTask(config)

	j.config.jobType = config.GetType()
	j.jobType = j.config.jobType
	j.config.placementStrategy = config.GetPlacementStrategy()
	j.config.owner = config.GetOwner()
	j.config.owningTeam = config.GetOwningTeam()
}

// getUpdatedJobRuntimeCache validates the runtime input and
// returns updated config. return value is nil, if validation
// fails
func (j *job) getUpdatedJobRuntimeCache(
	ctx context.Context,
	runtime *pbjob.RuntimeInfo,
	req UpdateRequest) (*pbjob.RuntimeInfo, error) {
	newRuntime := runtime

	if req == UpdateCacheAndDB {
		if err := j.populateRuntime(ctx); err != nil {
			return nil, err
		}
	}

	if j.runtime != nil {
		newRuntime = j.validateAndMergeRuntime(runtime, req)
		if j.runtime.GetGoalState() == pbjob.JobState_DELETED &&
			newRuntime != nil &&
			j.runtime.GetGoalState() != newRuntime.GetGoalState() {
			return nil, _updateDeleteJobErr
		}
	}

	return newRuntime, nil
}

// validateAndMergeRuntime validates whether a runtime can be merged with
// existing runtime cache. It returns the merged runtime if merge is valid.
func (j *job) validateAndMergeRuntime(
	runtime *pbjob.RuntimeInfo,
	req UpdateRequest) *pbjob.RuntimeInfo {
	if !j.validateStateUpdate(runtime) {
		log.WithField("current_revision", j.runtime.GetRevision().GetVersion()).
			WithField("new_revision", runtime.GetRevision().GetVersion()).
			WithField("new_state", runtime.GetState().String()).
			WithField("old_state", j.runtime.GetState().String()).
			WithField("new_goal_state", runtime.GetGoalState().String()).
			WithField("old_goal_state", j.runtime.GetGoalState().String()).
			WithField("job_id", j.id.Value).
			Info("failed job state validation")
		return nil
	}

	newRuntime := j.mergeRuntime(runtime)
	// No change in the runtime, ignore the update
	if reflect.DeepEqual(j.runtime, newRuntime) {
		return nil
	}

	return newRuntime
}

// validateStateUpdate returns whether the runtime update can be
// applied to the existing job runtime cache.
func (j *job) validateStateUpdate(newRuntime *pbjob.RuntimeInfo) bool {
	currentRuntime := j.runtime

	if newRuntime == nil {
		return false
	}

	// changeLog is not nil, newRuntime is from db
	if newRuntime.GetRevision() != nil {
		// Make sure that not overwriting with old or same version
		if newRuntime.GetRevision().GetVersion() <=
			currentRuntime.GetRevision().GetVersion() {
			return false
		}
	}
	return true
}

// mergeRuntime merges the current runtime and the new runtime and returns the merged
// runtime back. The runtime provided as input only contains the fields which
// the caller intends to change and the remaining are kept invalid/nil.
func (j *job) mergeRuntime(newRuntime *pbjob.RuntimeInfo) *pbjob.RuntimeInfo {
	currentRuntime := j.runtime
	runtime := *currentRuntime

	if newRuntime.GetState() != pbjob.JobState_UNKNOWN {
		runtime.State = newRuntime.GetState()
	}

	if stringsutil.ValidateString(newRuntime.GetCreationTime()) {
		runtime.CreationTime = newRuntime.GetCreationTime()
	}

	if stringsutil.ValidateString(newRuntime.GetStartTime()) {
		runtime.StartTime = newRuntime.GetStartTime()
	}

	if stringsutil.ValidateString(newRuntime.GetCompletionTime()) {
		runtime.CompletionTime = newRuntime.GetCompletionTime()
	}

	if len(newRuntime.GetTaskStats()) > 0 {
		runtime.TaskStats = newRuntime.GetTaskStats()
	}

	if newRuntime.GetTaskStatsByConfigurationVersion() != nil {
		runtime.TaskStatsByConfigurationVersion = newRuntime.GetTaskStatsByConfigurationVersion()
	}

	if len(newRuntime.GetResourceUsage()) > 0 {
		runtime.ResourceUsage = newRuntime.GetResourceUsage()
	}

	if newRuntime.GetConfigVersion() > 0 {
		runtime.ConfigVersion = newRuntime.GetConfigVersion()
	}

	if newRuntime.GetConfigurationVersion() > 0 {
		runtime.ConfigurationVersion = newRuntime.GetConfigurationVersion()
	}

	if newRuntime.GetGoalState() != pbjob.JobState_UNKNOWN {
		runtime.GoalState = newRuntime.GetGoalState()
	}

	if newRuntime.GetUpdateID() != nil {
		runtime.UpdateID = newRuntime.GetUpdateID()
	}

	if newRuntime.GetWorkflowVersion() > 0 {
		runtime.WorkflowVersion = newRuntime.GetWorkflowVersion()
	}

	if newRuntime.GetDesiredStateVersion() > 0 {
		runtime.DesiredStateVersion = newRuntime.GetDesiredStateVersion()
	}

	if newRuntime.GetStateVersion() > 0 {
		runtime.StateVersion = newRuntime.GetStateVersion()
	}

	if runtime.Revision == nil {
		// should never enter here
		log.WithField("job_id", j.id.GetValue()).
			Error("runtime changeLog is nil in update jobs")
		runtime.Revision = &peloton.ChangeLog{
			Version:   1,
			CreatedAt: uint64(time.Now().UnixNano()),
		}
	}

	// bump up the runtime version
	runtime.Revision = &peloton.ChangeLog{
		Version:   runtime.GetRevision().GetVersion() + 1,
		CreatedAt: runtime.GetRevision().GetCreatedAt(),
		UpdatedAt: uint64(time.Now().UnixNano()),
	}

	return &runtime
}

// invalidateCache clean job runtime and config cache
func (j *job) invalidateCache() {
	j.runtime = nil
	j.config = nil
}

// getInstanceAvailabilityInfo returns the instance availability info of the job.
// If the instance availability info is not present in cache, it is populated
// and returned. The caller should have the job lock before calling this function.
func (j *job) getInstanceAvailabilityInfo(ctx context.Context) (*instanceAvailabilityInfo, error) {
	if j.instanceAvailabilityInfo == nil {
		if err := j.populateInstanceAvailabilityInfo(ctx); err != nil {
			j.invalidateInstanceAvailabilityInfo()
			return nil, err
		}
	}
	return j.instanceAvailabilityInfo, nil
}

// populateInstanceAvailabilityInfo populates the instance availability info of
// the job. The caller should have the job lock before calling this function.
func (j *job) populateInstanceAvailabilityInfo(ctx context.Context) error {
	info := &instanceAvailabilityInfo{
		killedInstances:      make(map[uint32]bool),
		unavailableInstances: make(map[uint32]bool),
	}

	for i, t := range j.tasks {
		taskRuntime, err := t.GetRuntime(ctx)
		if err != nil {
			return err
		}

		currentStateVector := &TaskStateVector{
			State:         taskRuntime.GetState(),
			ConfigVersion: taskRuntime.GetConfigVersion(),
			MesosTaskID:   taskRuntime.GetMesosTaskId(),
		}

		goalStateVector := &TaskStateVector{
			State:         taskRuntime.GetGoalState(),
			ConfigVersion: taskRuntime.GetDesiredConfigVersion(),
			MesosTaskID:   taskRuntime.GetDesiredMesosTaskId(),
		}

		availability := getInstanceAvailability(
			currentStateVector,
			goalStateVector,
			taskRuntime.GetHealthy(),
			taskRuntime.GetTerminationStatus(),
		)

		switch availability {
		case jobmgrcommon.InstanceAvailability_UNAVAILABLE:
			info.unavailableInstances[i] = true
		case jobmgrcommon.InstanceAvailability_KILLED:
			info.killedInstances[i] = true
		}
	}

	j.instanceAvailabilityInfo = info
	return nil
}

// updateInstanceAvailabilityInfo updates the instance availability information
// of the job. The caller must acquire the job lock before calling this function.
func (j *job) updateInstanceAvailabilityInfo(
	ctx context.Context,
	id uint32,
	currentState *TaskStateVector,
	goalState *TaskStateVector,
	healthState pbtask.HealthState,
	terminationStatus *pbtask.TerminationStatus,
) {
	if j.jobType != pbjob.JobType_SERVICE {
		return
	}

	instanceAvailabilityInfo, err := j.getInstanceAvailabilityInfo(ctx)
	if err != nil {
		j.invalidateInstanceAvailabilityInfo()
		return
	}

	availability := getInstanceAvailability(
		currentState,
		goalState,
		healthState,
		terminationStatus,
	)

	switch availability {
	case jobmgrcommon.InstanceAvailability_UNAVAILABLE:
		delete(instanceAvailabilityInfo.killedInstances, id)
		instanceAvailabilityInfo.unavailableInstances[id] = true
	case jobmgrcommon.InstanceAvailability_KILLED:
		delete(instanceAvailabilityInfo.unavailableInstances, id)
		instanceAvailabilityInfo.killedInstances[id] = true
	case jobmgrcommon.InstanceAvailability_AVAILABLE, jobmgrcommon.InstanceAvailability_DELETED:
		delete(instanceAvailabilityInfo.unavailableInstances, id)
		delete(instanceAvailabilityInfo.killedInstances, id)
	}
}

// invalidateInstanceAvailabilityInfo clears the job instance availability information
func (j *job) invalidateInstanceAvailabilityInfo() {
	j.instanceAvailabilityInfo = nil
}

func populateConfigChangeLog(config *pbjob.JobConfig) *pbjob.JobConfig {
	newConfig := *config
	now := time.Now().UTC()
	newConfig.ChangeLog = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   1,
	}
	return &newConfig
}

func getInstanceAvailability(
	currentState *TaskStateVector,
	goalState *TaskStateVector,
	healthState pbtask.HealthState,
	terminationStatus *pbtask.TerminationStatus,
) jobmgrcommon.InstanceAvailability_Type {
	if goalState.State == pbtask.TaskState_DELETED {
		return jobmgrcommon.InstanceAvailability_DELETED
	}

	if goalState.State == pbtask.TaskState_KILLED {
		return jobmgrcommon.InstanceAvailability_KILLED
	}

	// If termination status is set then the instance has been terminated. See
	// the termination reason to determine whether to mark it UNAVAILABLE or KILLED.
	if terminationStatus != nil {
		switch terminationStatus.GetReason() {
		case pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
			pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
			pbtask.TerminationStatus_TERMINATION_STATUS_REASON_FAILED:
			return jobmgrcommon.InstanceAvailability_UNAVAILABLE
		default:
			return jobmgrcommon.InstanceAvailability_KILLED
		}
	}

	// If the current mesos-task-id/config-version do not match with desired
	// mesos-task-id/config-version and the termination status is not set,
	// mark the instance KILLED. This is because we always set termination status
	// with the appropriate reason whenever it is SLA aware killed (host-maintenance/update)
	if currentState.MesosTaskID.GetValue() != goalState.MesosTaskID.GetValue() ||
		currentState.ConfigVersion != goalState.ConfigVersion {
		return jobmgrcommon.InstanceAvailability_KILLED
	}

	if currentState.State == pbtask.TaskState_RUNNING &&
		goalState.State == pbtask.TaskState_RUNNING {
		switch healthState {
		case pbtask.HealthState_HEALTHY, pbtask.HealthState_DISABLED:
			return jobmgrcommon.InstanceAvailability_AVAILABLE
		}
	}

	return jobmgrcommon.InstanceAvailability_UNAVAILABLE
}

// generateJobSummaryFromCache returns:
// - JobSummary by combining RuntimeInfo and fields from cached job config
// - UpdateModel by converting update cache
//
// generateJobSummaryFromCache should only be called while holding the lock.
func (j *job) generateJobSummaryFromCache(
	runtime *pbjob.RuntimeInfo,
	updateID *peloton.UpdateID,
) (*pbjob.JobSummary, *models.UpdateModel) {
	var s *pbjob.JobSummary
	var um *models.UpdateModel

	if runtime != nil && j.config != nil {
		runtimeCopy := proto.Clone(runtime).(*pbjob.RuntimeInfo)

		s = &pbjob.JobSummary{
			Id:         j.ID(),
			Runtime:    runtimeCopy,
			Name:       j.config.GetName(),
			Type:       j.config.jobType,
			Owner:      j.config.GetOwner(),
			OwningTeam: j.config.GetOwningTeam(),
			// We are doing a swap of labels slice when populating
			// from job config, simple get is sufficient.
			Labels:        j.config.GetLabels(),
			InstanceCount: j.config.GetInstanceCount(),
			RespoolID:     j.config.GetRespoolID(),
			SLA:           j.config.GetSLA(),
		}
	}

	if updateID != nil {
		var updateCache *update
		if u, ok := j.workflows[updateID.GetValue()]; ok {
			updateCache = u
		} else if updateID.GetValue() == j.prevUpdateID {
			updateCache = j.prevWorkflow
		}

		if updateCache != nil {
			um = &models.UpdateModel{
				Type:            updateCache.GetWorkflowType(),
				State:           updateCache.GetState().State,
				PrevState:       updateCache.GetPrevState(),
				InstancesDone:   uint32(len(updateCache.GetInstancesDone())),
				InstancesTotal:  uint32(len(updateCache.GetInstancesUpdated()) + len(updateCache.GetInstancesAdded()) + len(updateCache.GetInstancesRemoved())),
				InstancesFailed: uint32(len(updateCache.GetInstancesFailed())),
				// GetInstancesCurrent() returns a copy from update cache
				InstancesCurrent:     updateCache.GetInstancesCurrent(),
				JobConfigVersion:     updateCache.GetJobVersion(),
				PrevJobConfigVersion: updateCache.GetJobPrevVersion(),
			}
		}
	}

	return s, um
}

// Option to create a workflow
type Option interface {
	apply(*workflowOpts)
}

type workflowOpts struct {
	jobConfig       *pbjob.JobConfig
	prevJobConfig   *pbjob.JobConfig
	configAddOn     *models.ConfigAddOn
	jobSpec         *stateless.JobSpec
	instanceAdded   []uint32
	instanceUpdated []uint32
	instanceRemoved []uint32
	opaqueData      *peloton.OpaqueData
}

// WithConfig defines the original config and target config for the workflow.
// Workflow could use the configs to calculate the instances it would need to
// work on as well as verify if the update is a noop. It also includes the
// target job spec which would be stored to the DB as part of workflow creation.
func WithConfig(
	jobConfig *pbjob.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobSpec *stateless.JobSpec,
) Option {
	return &configOpt{
		jobConfig:     jobConfig,
		prevJobConfig: prevJobConfig,
		configAddOn:   configAddOn,
		jobSpec:       jobSpec,
	}
}

// WithInstanceToProcess defines the instances
// the workflow would work on. When it is provided,
// workflow would not calculate instances to process
// based on config.
func WithInstanceToProcess(
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32,
) Option {
	return &instanceToProcessOpt{
		instancesAdded:   instancesAdded,
		instancesUpdated: instancesUpdated,
		instancesRemoved: instancesRemoved,
	}
}

// WithOpaqueData defines the opaque data provided by the
// user to be stored with the update
func WithOpaqueData(opaqueData *peloton.OpaqueData) Option {
	return &opaqueDataOpt{
		opaqueData: opaqueData,
	}
}

type configOpt struct {
	jobConfig     *pbjob.JobConfig
	prevJobConfig *pbjob.JobConfig
	configAddOn   *models.ConfigAddOn
	jobSpec       *stateless.JobSpec
}

func (o *configOpt) apply(opts *workflowOpts) {
	opts.jobConfig = o.jobConfig
	opts.prevJobConfig = o.prevJobConfig
	opts.configAddOn = o.configAddOn
	opts.jobSpec = o.jobSpec
}

type instanceToProcessOpt struct {
	instancesAdded   []uint32
	instancesUpdated []uint32
	instancesRemoved []uint32
}

func (o *instanceToProcessOpt) apply(opts *workflowOpts) {
	opts.instanceAdded = o.instancesAdded
	opts.instanceUpdated = o.instancesUpdated
	opts.instanceRemoved = o.instancesRemoved
}

type opaqueDataOpt struct {
	opaqueData *peloton.OpaqueData
}

func (o *opaqueDataOpt) apply(opts *workflowOpts) {
	opts.opaqueData = o.opaqueData
}

func (j *job) CreateWorkflow(
	ctx context.Context,
	workflowType models.WorkflowType,
	updateConfig *pbupdate.UpdateConfig,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	if err := j.ValidateEntityVersion(ctx, entityVersion); err != nil {
		return nil, nil, err
	}

	if util.IsPelotonJobStateTerminal(j.runtime.GetGoalState()) &&
		!util.IsPelotonJobStateTerminal(j.runtime.GetState()) &&
		updateConfig.GetStartTasks() {
		return nil,
			nil,
			yarpcerrors.AbortedErrorf("job is being terminated, cannot update with start_pods set now")
	}

	var currentUpdate *models.UpdateModel
	var err error
	if currentUpdate, err = j.getCurrentUpdate(ctx); err != nil {
		return nil, nil, err
	}

	opts := &workflowOpts{}
	for _, option := range options {
		option.apply(opts)
	}

	if j.isWorkflowNoop(
		ctx,
		opts.prevJobConfig,
		opts.jobConfig,
		updateConfig,
		workflowType,
		currentUpdate,
	) {
		if opts.opaqueData.GetData() != currentUpdate.GetOpaqueData().GetData() {
			// update workflow version first, so the change to opaque data would cause
			// an entity version change.
			// This is needed as user behavior may depend on opaque data, peloton needs to
			// make sure user takes the correct action based on update-to-date opaque data.
			j.runtime.WorkflowVersion++
			newRuntime := j.mergeRuntime(&pbjob.RuntimeInfo{WorkflowVersion: j.runtime.GetWorkflowVersion()})
			if err := j.jobFactory.jobRuntimeOps.Upsert(ctx, j.id, newRuntime); err != nil {
				j.invalidateCache()
				return currentUpdate.GetUpdateID(),
					nil,
					errors.Wrap(err, "fail to update job runtime when create workflow")
			}

			// TODO: move this under update cache object
			currentUpdate.OpaqueData = &peloton.OpaqueData{Data: opts.opaqueData.GetData()}
			currentUpdate.UpdateTime = time.Now().Format(time.RFC3339Nano)
			if err := j.
				jobFactory.
				updateStore.
				ModifyUpdate(ctx, currentUpdate); err != nil {
				return nil, nil, errors.Wrap(err, "fail to modify update opaque data")
			}
		}

		// nothing changed, directly return
		return currentUpdate.GetUpdateID(),
			versionutil.GetJobEntityVersion(
				j.runtime.GetConfigurationVersion(),
				j.runtime.GetDesiredStateVersion(),
				j.runtime.GetWorkflowVersion(),
			),
			nil
	}

	newConfig, err := j.compareAndSetConfig(
		ctx,
		opts.jobConfig,
		opts.configAddOn,
		opts.jobSpec,
	)
	if err != nil {
		return nil, nil, err
	}

	updateID := &peloton.UpdateID{Value: uuid.New()}
	newWorkflow := newUpdate(updateID, j.jobFactory)

	if err := newWorkflow.Create(
		ctx,
		j.id,
		newConfig,
		opts.prevJobConfig,
		opts.configAddOn,
		opts.instanceAdded,
		opts.instanceUpdated,
		opts.instanceRemoved,
		workflowType,
		updateConfig,
		opts.opaqueData,
	); err != nil {
		// Directly return without invalidating job config cache.
		// When reading job config later, it would check if
		// runtime.GetConfigurationVersion has the same version with cached config.
		// If not, it would invalidate config cache and repopulate the cache with
		// the correct version.
		return nil, nil, err
	}

	err = j.updateJobRuntime(
		ctx,
		newConfig.GetChangeLog().GetVersion(),
		j.runtime.GetWorkflowVersion()+1,
		newWorkflow,
	)

	if err != nil {
		return updateID, nil, err
	}

	// only add new workflow to job if runtime update succeeds.
	// If err is not nil, it is unclear whether update id in job
	// runtime is updated successfully. If the update id does get
	// persisted in job runtime, workflow.Recover and AddWorkflow
	// can ensure that job tracks the workflow when the workflow
	// is processed.
	j.workflows[updateID.GetValue()] = newWorkflow

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, updateID)

	// entity version is changed due to change in config version
	newEntityVersion := versionutil.GetJobEntityVersion(
		j.runtime.GetConfigurationVersion(),
		j.runtime.GetDesiredStateVersion(),
		j.runtime.GetWorkflowVersion(),
	)

	log.WithField("workflow_id", updateID.GetValue()).
		WithField("job_id", j.id.GetValue()).
		WithField("instances_added", len(opts.instanceAdded)).
		WithField("instances_updated", len(opts.instanceUpdated)).
		WithField("instances_removed", len(opts.instanceRemoved)).
		WithField("workflow_type", workflowType.String()).
		Debug("workflow is created")

	return updateID, newEntityVersion, err
}

func (j *job) getCurrentUpdate(ctx context.Context) (*models.UpdateModel, error) {
	if err := j.populateRuntime(ctx); err != nil {
		return nil, err
	}

	if len(j.runtime.GetUpdateID().GetValue()) == 0 {
		return nil, nil
	}

	return j.jobFactory.updateStore.GetUpdate(ctx, j.runtime.GetUpdateID())
}

// isWorkflowNoop checks if the new workflow to be created
// is a noop and can be safely ignored.
// The function checks if there is an active update
// with the same job config and update config.
func (j *job) isWorkflowNoop(
	ctx context.Context,
	prevJobConfig *pbjob.JobConfig,
	targetJobConfig *pbjob.JobConfig,
	updateConfig *pbupdate.UpdateConfig,
	workflowType models.WorkflowType,
	currentUpdate *models.UpdateModel,
) bool {
	if currentUpdate == nil {
		return false
	}

	// only check for active update
	if !IsUpdateStateActive(currentUpdate.GetState()) {
		return false
	}

	if currentUpdate.GetType() != workflowType {
		return false
	}

	if !isJobConfigEqual(prevJobConfig, targetJobConfig) {
		return false
	}

	if !isUpdateConfigEqual(currentUpdate.GetUpdateConfig(), updateConfig) {
		return false
	}

	return true
}

func isJobConfigEqual(
	prevJobConfig *pbjob.JobConfig,
	targetJobConfig *pbjob.JobConfig,
) bool {
	if prevJobConfig.GetInstanceCount() != targetJobConfig.GetInstanceCount() {
		return false
	}

	if taskconfig.HasPelotonLabelsChanged(prevJobConfig.GetLabels(), targetJobConfig.GetLabels()) {
		return false
	}

	for i := uint32(0); i < prevJobConfig.GetInstanceCount(); i++ {
		prevTaskConfig := taskconfig.Merge(
			prevJobConfig.GetDefaultConfig(),
			prevJobConfig.GetInstanceConfig()[i])
		targetTaskConfig := taskconfig.Merge(
			targetJobConfig.GetDefaultConfig(),
			targetJobConfig.GetInstanceConfig()[i])
		if taskconfig.HasTaskConfigChanged(prevTaskConfig, targetTaskConfig) {
			return false
		}
	}

	prevJobConfigCopy := *prevJobConfig
	targetJobConfigCopy := *targetJobConfig

	prevJobConfigCopy.ChangeLog = nil
	targetJobConfigCopy.ChangeLog = nil

	prevJobConfigCopy.Labels = nil
	targetJobConfigCopy.Labels = nil

	return proto.Equal(&prevJobConfigCopy, &targetJobConfigCopy)
}

func isUpdateConfigEqual(
	prevUpdateConfig *pbupdate.UpdateConfig,
	targetUpdateConfig *pbupdate.UpdateConfig,
) bool {
	return proto.Equal(prevUpdateConfig, targetUpdateConfig)
}

func (j *job) PauseWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	currentWorkflow, err := j.getCurrentWorkflow(ctx)
	if err != nil {
		return nil, nil, err
	}
	if currentWorkflow == nil {
		return nil, nil, yarpcerrors.NotFoundErrorf("no workflow found")
	}

	opts := &workflowOpts{}
	for _, option := range options {
		option.apply(opts)
	}

	// update workflow version before mutating workflow, so
	// when workflow state changes, entity version must be changed
	// as well
	if err := j.updateWorkflowVersion(ctx, entityVersion); err != nil {
		return nil, nil, err
	}

	newEntityVersion := versionutil.GetJobEntityVersion(
		j.runtime.GetConfigurationVersion(),
		j.runtime.GetDesiredStateVersion(),
		j.runtime.GetWorkflowVersion(),
	)
	err = currentWorkflow.Pause(ctx, opts.opaqueData)

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, currentWorkflow.ID())

	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) ResumeWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	currentWorkflow, err := j.getCurrentWorkflow(ctx)
	if err != nil {
		return nil, nil, err
	}
	if currentWorkflow == nil {
		return nil, nil, yarpcerrors.NotFoundErrorf("no workflow found")
	}

	opts := &workflowOpts{}
	for _, option := range options {
		option.apply(opts)
	}

	// update workflow version before mutating workflow, so
	// when workflow state changes, entity version must be changed
	// as well
	if err := j.updateWorkflowVersion(ctx, entityVersion); err != nil {
		return nil, nil, err
	}

	newEntityVersion := versionutil.GetJobEntityVersion(
		j.runtime.GetConfigurationVersion(),
		j.runtime.GetDesiredStateVersion(),
		j.runtime.GetWorkflowVersion(),
	)
	err = currentWorkflow.Resume(ctx, opts.opaqueData)

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, currentWorkflow.ID())

	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) AbortWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	currentWorkflow, err := j.getCurrentWorkflow(ctx)
	if err != nil {
		return nil, nil, err
	}
	if currentWorkflow == nil {
		return nil, nil, yarpcerrors.NotFoundErrorf("no workflow found")
	}

	opts := &workflowOpts{}
	for _, option := range options {
		option.apply(opts)
	}

	// update workflow version before mutating workflow, so
	// when workflow state changes, entity version must be changed
	// as well
	if err := j.updateWorkflowVersion(ctx, entityVersion); err != nil {
		return nil, nil, err
	}

	newEntityVersion := versionutil.GetJobEntityVersion(
		j.runtime.GetConfigurationVersion(),
		j.runtime.GetDesiredStateVersion(),
		j.runtime.GetWorkflowVersion(),
	)
	err = currentWorkflow.Cancel(ctx, opts.opaqueData)

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, currentWorkflow.ID())

	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) RollbackWorkflow(ctx context.Context) error {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.Lock()
	defer j.Unlock()

	currentWorkflow, err := j.getCurrentWorkflow(ctx)
	if err != nil {
		return err
	}
	if currentWorkflow == nil {
		return yarpcerrors.NotFoundErrorf("no workflow found")
	}

	// make sure workflow cache is populated
	if err := currentWorkflow.Recover(ctx); err != nil {
		return err
	}

	if IsUpdateStateTerminal(currentWorkflow.GetState().State) {
		return nil
	}

	// make sure runtime cache is populated
	if err := j.populateRuntime(ctx); err != nil {
		return err
	}

	if currentWorkflow.GetState().State == pbupdate.State_ROLLING_BACKWARD {
		// config version in runtime is already set to the target
		// job version of rollback. This can happen due to error retry.
		if j.runtime.GetConfigurationVersion() ==
			currentWorkflow.GetGoalState().JobVersion {
			return nil
		}

		// just update job runtime config version
		err := j.updateJobRuntime(
			ctx,
			currentWorkflow.GetGoalState().JobVersion,
			j.runtime.GetWorkflowVersion(),
			currentWorkflow,
		)

		if err != nil {
			return err
		}

		jobTypeCopy = j.jobType
		jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

		return nil
	}

	// get the old job config before the workflow is run
	prevObj, err := j.jobFactory.jobConfigOps.GetResult(
		ctx,
		j.ID(),
		currentWorkflow.GetState().JobVersion,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to get job config to copy for workflow rolling back")
	}

	// copy the old job config and get the config which
	// the workflow can "rollback" to
	configCopy, err := j.copyJobAndTaskConfig(
		ctx,
		prevObj.JobConfig,
		prevObj.ConfigAddOn,
		prevObj.JobSpec,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to copy job and task config for workflow rolling back")
	}

	// get the job config the workflow is targeted at before rollback
	currentConfig, _, err := j.jobFactory.jobConfigOps.Get(
		ctx,
		j.ID(),
		currentWorkflow.GetGoalState().JobVersion,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to get current job config for workflow rolling back")
	}

	if err := currentWorkflow.Rollback(ctx, currentConfig, configCopy); err != nil {
		return err
	}

	err = j.updateJobRuntime(
		ctx,
		configCopy.GetChangeLog().GetVersion(),
		j.runtime.GetWorkflowVersion(),
		currentWorkflow,
	)

	if err != nil {
		return err
	}

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

	return nil
}

func (j *job) WriteWorkflowProgress(
	ctx context.Context,
	updateID *peloton.UpdateID,
	state pbupdate.State,
	instancesDone []uint32,
	instanceFailed []uint32,
	instancesCurrent []uint32,
) error {
	var (
		jobTypeCopy     pbjob.JobType
		jobSummaryCopy  *pbjob.JobSummary
		updateModelCopy *models.UpdateModel
	)
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)
	}()

	j.RLock()
	defer j.RUnlock()

	workflow, ok := j.workflows[updateID.GetValue()]
	if !ok {
		return nil
	}

	err := workflow.WriteProgress(
		ctx,
		state,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	)
	if err != nil {
		return err
	}

	jobTypeCopy = j.jobType
	jobSummaryCopy, updateModelCopy = j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

	return nil
}

func (j *job) AddWorkflow(updateID *peloton.UpdateID) Update {
	if workflow := j.GetWorkflow(updateID); workflow != nil {
		return workflow
	}

	j.Lock()
	defer j.Unlock()

	if workflow, ok := j.workflows[updateID.GetValue()]; ok {
		return workflow
	}

	workflow := newUpdate(updateID, j.jobFactory)
	j.workflows[updateID.GetValue()] = workflow
	return workflow
}

func (j *job) GetWorkflow(updateID *peloton.UpdateID) Update {
	j.RLock()
	defer j.RUnlock()
	if workflow, ok := j.workflows[updateID.GetValue()]; ok {
		return workflow
	}
	return nil
}

func (j *job) ClearWorkflow(updateID *peloton.UpdateID) {
	j.Lock()
	defer j.Unlock()

	j.prevUpdateID = updateID.GetValue()
	j.prevWorkflow = j.workflows[updateID.GetValue()]

	delete(j.workflows, updateID.GetValue())
}

func (j *job) GetTaskStateCount() (
	taskCount map[TaskStateSummary]int,
	throttledTasks int,
	spread JobSpreadCounts,
) {
	taskCount = make(map[TaskStateSummary]int)

	spreadHosts := make(map[string]struct{})

	j.RLock()
	defer j.RUnlock()

	for _, t := range j.tasks {
		stateSummary := t.StateSummary()

		taskCount[stateSummary]++

		if j.config != nil {
			if j.config.GetType() == pbjob.JobType_SERVICE &&
				util.IsPelotonStateTerminal(stateSummary.CurrentState) &&
				util.IsTaskThrottled(stateSummary.CurrentState, t.GetCacheRuntime().GetMessage()) {
				throttledTasks++
			}
			if j.config.placementStrategy == pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB {
				runtime := t.GetCacheRuntime()
				if runtime.GetHost() != "" {
					spreadHosts[runtime.GetHost()] = struct{}{}
					spread.taskCount++
				}
			}
		}
	}
	spread.hostCount = len(spreadHosts)

	return
}

func (j *job) GetWorkflowStateCount() map[pbupdate.State]int {
	workflowCount := make(map[pbupdate.State]int)

	j.RLock()
	defer j.RUnlock()

	for _, u := range j.workflows {
		curState := u.GetState().State
		workflowCount[curState]++
	}

	return workflowCount
}

// copyJobAndTaskConfig copies the provided job config and
// create task configs for the copy. It returns the job config
// copy with change log version updated.
func (j *job) copyJobAndTaskConfig(
	ctx context.Context,
	jobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	spec *stateless.JobSpec,
) (*pbjob.JobConfig, error) {
	// set config changeLog version to that of current config for
	// concurrency control
	if err := j.populateRuntime(ctx); err != nil {
		return nil, err
	}
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: j.runtime.GetConfigurationVersion(),
	}

	// copy job config
	configCopy, err := j.compareAndSetConfig(
		ctx,
		jobConfig,
		configAddOn,
		spec,
	)
	if err != nil {
		return nil, errors.Wrap(err,
			"failed to set job config for workflow rolling back")
	}

	// change the job config version to that of config copy,
	// so task config would have the right version
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: configCopy.GetChangeLog().GetVersion(),
	}
	// copy task configs
	if err = j.CreateTaskConfigs(
		ctx,
		j.id,
		jobConfig,
		configAddOn,
		spec,
	); err != nil {
		return nil, errors.Wrap(err,
			"failed to create task configs for workflow rolling back")
	}

	return jobConfig, nil
}

// updateJobRuntime updates job runtime with newConfigVersion and
// set the runtime updateID to u.id. It validates if the workflowType
// update is valid. It also updates the job goal state if necessary.
// It must be called with job lock held.
func (j *job) updateJobRuntime(
	ctx context.Context,
	newConfigVersion uint64,
	newWorkflowVersion uint64,
	newWorkflow Update,
) error {
	if err := j.populateRuntime(ctx); err != nil {
		return err
	}

	if j.runtime.GetGoalState() == pbjob.JobState_DELETED {
		return _updateDeleteJobErr
	}

	if err := j.validateWorkflowOverwrite(
		ctx,
		newWorkflow.GetWorkflowType(),
	); err != nil {
		return err
	}

	// TODO: check entity version and bump the version
	runtime := proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	runtime.UpdateID = &peloton.UpdateID{
		Value: newWorkflow.ID().GetValue(),
	}
	runtime.Revision = &peloton.ChangeLog{
		Version:   j.runtime.GetRevision().GetVersion() + 1,
		CreatedAt: j.runtime.GetRevision().GetCreatedAt(),
		UpdatedAt: uint64(time.Now().UnixNano()),
	}
	runtime.ConfigurationVersion = newConfigVersion
	runtime.WorkflowVersion = newWorkflowVersion
	if newWorkflow.GetWorkflowType() == models.WorkflowType_RESTART ||
		newWorkflow.GetUpdateConfig().GetStartTasks() {
		runtime.GoalState = pbjob.JobState_RUNNING
	}
	if err := j.jobFactory.jobRuntimeOps.Upsert(
		ctx,
		j.id,
		runtime,
	); err != nil {
		j.invalidateCache()
		return err
	}
	if err := j.jobFactory.jobIndexOps.Update(
		ctx,
		j.id,
		nil,
		runtime,
	); err != nil {
		j.invalidateCache()
		return err
	}

	j.runtime = runtime

	return nil
}

// validateWorkflowOverwrite validates if the new workflow type can override
// existing workflow in job runtime. It returns an error if the validation
// fails.
func (j *job) validateWorkflowOverwrite(
	ctx context.Context,
	workflowType models.WorkflowType,
) error {
	currentWorkflow, err := j.getCurrentWorkflow(ctx)
	if err != nil || currentWorkflow == nil {
		return err
	}

	// make sure workflow cache is populated
	if err := currentWorkflow.Recover(ctx); err != nil {
		return err
	}

	// workflow update should succeed if previous update is terminal
	if IsUpdateStateTerminal(currentWorkflow.GetState().State) {
		return nil
	}

	// an overwrite is only valid if both current and new workflow
	// type is update
	if currentWorkflow.GetWorkflowType() == models.WorkflowType_UPDATE &&
		workflowType == models.WorkflowType_UPDATE {
		return nil
	}

	return yarpcerrors.InvalidArgumentErrorf(
		"workflow %s cannot overwrite workflow %s",
		workflowType.String(), currentWorkflow.GetWorkflowType().String())
}

func (c *cachedConfig) GetInstanceCount() uint32 {
	return c.instanceCount
}

func (c *cachedConfig) GetType() pbjob.JobType {
	return c.jobType
}

func (c *cachedConfig) GetRespoolID() *peloton.ResourcePoolID {
	if c.respoolID == nil {
		return nil
	}
	tmpRespoolID := *c.respoolID
	return &tmpRespoolID
}

func (c *cachedConfig) GetChangeLog() *peloton.ChangeLog {
	if c.changeLog == nil {
		return nil
	}
	tmpChangeLog := *c.changeLog
	return &tmpChangeLog
}

func (c *cachedConfig) GetSLA() *pbjob.SlaConfig {
	if c.sla == nil {
		return nil
	}
	tmpSLA := *c.sla
	return &tmpSLA
}

func (c *cachedConfig) HasControllerTask() bool {
	return c.hasControllerTask
}

func (c *cachedConfig) GetLabels() []*peloton.Label {
	return c.labels
}

func (c *cachedConfig) GetName() string {
	return c.name
}

func (c *cachedConfig) GetPlacementStrategy() pbjob.PlacementStrategy {
	return c.placementStrategy
}

func (c *cachedConfig) GetOwner() string {
	return c.owner
}

func (c *cachedConfig) GetOwningTeam() string {
	return c.owningTeam
}

// HasControllerTask returns if a job has controller task in it,
// it can accept both cachedConfig and full JobConfig
func HasControllerTask(config jobmgrcommon.JobConfig) bool {
	if castedCachedConfig, ok := config.(JobConfigCache); ok {
		return castedCachedConfig.HasControllerTask()
	}

	return hasControllerTask(config.(*pbjob.JobConfig))
}

func hasControllerTask(config *pbjob.JobConfig) bool {
	return taskconfig.Merge(
		config.GetDefaultConfig(),
		config.GetInstanceConfig()[0]).GetController()
}

func getIdsFromRuntimeMap(input map[uint32]*pbtask.RuntimeInfo) []uint32 {
	result := make([]uint32, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

func getIdsFromTaskInfoMap(input map[uint32]*pbtask.TaskInfo) []uint32 {
	result := make([]uint32, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

func getIdsFromDiffs(input map[uint32]jobmgrcommon.RuntimeDiff) []uint32 {
	result := make([]uint32, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

// UpdateResourceUsage updates the resource usage of a job by adding the task
// resource usage numbers to it. UpdateResourceUsage is called every time a
// task enters a terminal state.
func (j *job) UpdateResourceUsage(taskResourceUsage map[string]float64) {
	j.Lock()
	defer j.Unlock()

	for k, v := range taskResourceUsage {
		j.resourceUsage[k] += v
	}
}

// GetResourceUsage returns the resource usage of a job
func (j *job) GetResourceUsage() map[string]float64 {
	j.RLock()
	defer j.RUnlock()

	return j.resourceUsage
}

func (j *job) GetAllWorkflows() map[string]Update {
	j.RLock()
	defer j.RUnlock()

	result := make(map[string]Update)
	for id, workflow := range j.workflows {
		result[id] = workflow
	}

	return result
}

// RecalculateResourceUsage recalculates the resource usage of a job by adding
// together resource usage numbers of all terminal tasks of this job.
// RecalculateResourceUsage should be called ONLY during job recovery to
// initialize the job runtime with a correct baseline resource usage.
// It is not safe to call this for a running job except from recovery code when
// the event stream has not started and the task resource usages will not be
// updated.
func (j *job) RecalculateResourceUsage(ctx context.Context) {
	j.Lock()
	defer j.Unlock()

	// start with resource usage set to an empty map with 0 values for CPU, GPU
	// and memory
	j.resourceUsage = createEmptyResourceUsageMap()
	for id, task := range j.tasks {
		if runtime, err := task.GetRuntime(ctx); err == nil {
			for k, v := range runtime.GetResourceUsage() {
				j.resourceUsage[k] += v
			}
		} else {
			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      j.id.GetValue(),
					"instance_id": id}).
				Error("error adding task resource usage to job")
		}
	}
}

func (j *job) populateRuntime(ctx context.Context) error {
	if j.runtime == nil {
		runtime, err := j.jobFactory.jobRuntimeOps.Get(ctx, j.ID())
		if err != nil {
			return err
		}
		j.runtime = runtime
	}
	return nil
}

// getCurrentWorkflow return the current workflow of the job.
// it is possible that the workflow returned was not in goal state engine
// and cache, caller needs to make sure the workflow returned is
func (j *job) getCurrentWorkflow(ctx context.Context) (Update, error) {
	// make sure runtime is in cache
	if err := j.populateRuntime(ctx); err != nil {
		return nil, err
	}

	if len(j.runtime.GetUpdateID().GetValue()) == 0 {
		return nil, nil
	}

	if workflow, ok := j.workflows[j.runtime.GetUpdateID().GetValue()]; ok {
		return workflow, nil
	}

	// workflow not found in cache, create a new one and let called
	// to recover the update state
	return newUpdate(j.runtime.GetUpdateID(), j.jobFactory), nil
}

func (j *job) ValidateEntityVersion(
	ctx context.Context,
	version *v1alphapeloton.EntityVersion,
) error {
	if err := j.populateRuntime(ctx); err != nil {
		return err
	}

	curEntityVersion := versionutil.GetJobEntityVersion(
		j.runtime.GetConfigurationVersion(),
		j.runtime.GetDesiredStateVersion(),
		j.runtime.GetWorkflowVersion())

	if curEntityVersion.GetValue() != version.GetValue() {
		return jobmgrcommon.InvalidEntityVersionError
	}
	return nil
}

// updateWorkflowVersion updates workflow version field in job runtime and persist
// job runtime into db
func (j *job) updateWorkflowVersion(
	ctx context.Context,
	version *v1alphapeloton.EntityVersion,
) error {
	if err := j.ValidateEntityVersion(ctx, version); err != nil {
		return err
	}
	_, _, workflowVersion, err := versionutil.ParseJobEntityVersion(version)
	if err != nil {
		return err
	}

	runtimeDiff := &pbjob.RuntimeInfo{WorkflowVersion: workflowVersion + 1}
	newRuntime := j.mergeRuntime(runtimeDiff)

	if err := j.jobFactory.jobRuntimeOps.Upsert(ctx, j.id, newRuntime); err != nil {
		j.runtime = nil
		return err
	}
	if err := j.jobFactory.jobIndexOps.Update(
		ctx,
		j.id,
		nil,
		newRuntime,
	); err != nil {
		j.runtime = nil
		return err
	}

	j.runtime = newRuntime
	return nil
}

// Delete deletes the job from DB and clears the cache
func (j *job) Delete(ctx context.Context) error {
	// It is possible to receive a timeout error although the delete was
	// successful. Hence, invalidate the cache irrespective of whether an error
	// occurred or not
	defer func() {
		j.Lock()
		defer j.Unlock()

		jobTypeCopy := j.jobType
		jobSummaryCopy, updateModelCopy := j.generateJobSummaryFromCache(j.runtime, j.runtime.GetUpdateID())

		if jobSummaryCopy != nil && jobSummaryCopy.Runtime != nil {
			jobSummaryCopy.Runtime.State = pbjob.JobState_DELETED
		}

		// Not doing nil check here since we are expecting the listener
		// to filter out the nil objects.
		j.jobFactory.notifyJobSummaryChanged(
			j.ID(),
			jobTypeCopy,
			jobSummaryCopy,
			updateModelCopy,
		)

		j.invalidateCache()
	}()

	// delete from job_index
	if err := j.jobFactory.jobIndexOps.Delete(ctx, j.ID()); err != nil {
		return err
	}

	if err := j.jobFactory.jobStore.DeleteJob(
		ctx,
		j.ID().GetValue(),
	); err != nil {
		return err
	}

	// delete from active_jobs
	return j.jobFactory.activeJobsOps.Delete(ctx, j.ID())
}

func createEmptyResourceUsageMap() map[string]float64 {
	return map[string]float64{
		common.CPU:    float64(0),
		common.GPU:    float64(0),
		common.MEMORY: float64(0),
	}
}
