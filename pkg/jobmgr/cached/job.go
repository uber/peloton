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

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/taskconfig"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	stringsutil "github.com/uber/peloton/pkg/common/util/strings"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstateutil "github.com/uber/peloton/pkg/jobmgr/util/goalstate"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"

	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

var _updateDeleteJobErr = yarpcerrors.InvalidArgumentErrorf("job is going to be deleted")

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
	) error

	// CreateTaskRuntimes creates the task runtimes in cache and DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	CreateTaskRuntimes(ctx context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, owner string) error

	// PatchTasks patch runtime diff to the existing task cache. runtimeDiffs
	// is a kv map with key as the instance_id of the task to be updated.
	// Value of runtimeDiffs is RuntimeDiff, of which key is the field name
	// to be update, and value is the new value of the field. PatchTasks
	// would save the change in both cache and DB. If persisting to DB fails,
	// cache would be invalidated as well.
	PatchTasks(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) error

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
	Create(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn, createBy string) error

	// RollingCreate is used to create the job configuration and runtime in DB.
	// It would create a workflow to manage the job creation, therefore the creation
	// process can be paused/resumed/aborted.
	RollingCreate(
		ctx context.Context,
		config *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		updateConfig *pbupdate.UpdateConfig,
		opaqueData *peloton.OpaqueData,
		createBy string,
	) error

	// Update updates job with the new runtime and config. If the request is to update
	// both DB and cache, it first attempts to persist the request in storage,
	// If that fails, it just returns back the error for now.
	// If successful, the cache is updated as well.
	// TODO: no config update should go through this API, divide this API into
	// config and runtime part
	Update(ctx context.Context, jobInfo *pbjob.JobInfo, configAddOn *models.ConfigAddOn, req UpdateRequest) error

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
	CompareAndSetConfig(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn) (jobmgrcommon.JobConfig, error)

	// IsPartiallyCreated returns if job has not been fully created yet
	IsPartiallyCreated(config jobmgrcommon.JobConfig) bool

	// ValidateEntityVersion validates the entity version of the job is the
	// same as provided in the input, and if not, then returns an error.
	ValidateEntityVersion(ctx context.Context, version *v1alphapeloton.EntityVersion) error

	// GetRuntime returns the runtime of the job
	GetRuntime(ctx context.Context) (*pbjob.RuntimeInfo, error)

	// GetConfig returns the current config of the job
	GetConfig(ctx context.Context) (jobmgrcommon.JobConfig, error)

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

	// GetStateCount returns the state/goal state count of all
	// tasks in a job
	GetStateCount() map[pbtask.TaskState]map[pbtask.TaskState]int
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
}

func (j *job) ID() *peloton.JobID {
	return j.id
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
		config, _, err := j.jobFactory.jobStore.GetJobConfigWithVersion(
			ctx,
			j.ID().GetValue(),
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

func (j *job) AddTask(
	ctx context.Context,
	id uint32) (Task, error) {
	if t := j.GetTask(id); t != nil {
		return t, nil
	}

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
) error {
	if jobConfig.GetDefaultConfig() != nil {
		// Create default task config in DB
		if err := j.jobFactory.taskStore.CreateTaskConfig(
			ctx,
			jobID,
			common.DefaultTaskConfigID,
			jobConfig.GetDefaultConfig(),
			configAddOn,
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
		var ok bool
		if cfg, ok = jobConfig.GetInstanceConfig()[id]; !ok {
			return yarpcerrors.NotFoundErrorf(
				"failed to get instance config for instance %v", id,
			)
		}
		taskConfig := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)

		return j.jobFactory.taskStore.CreateTaskConfig(
			ctx,
			jobID,
			int64(id),
			taskConfig,
			configAddOn,
			jobConfig.GetChangeLog().GetVersion(),
		)
	}

	var instanceIDList []uint32
	for i := uint32(0); i < jobConfig.GetInstanceCount(); i++ {
		if _, ok := jobConfig.GetInstanceConfig()[i]; ok {
			instanceIDList = append(instanceIDList, i)
		}
	}

	return taskutil.RunInParallel(
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
		return t.CreateTask(ctx, runtime, owner)
	}
	return taskutil.RunInParallel(
		j.ID().GetValue(),
		getIdsFromRuntimeMap(runtimes),
		createSingleTask)
}

func (j *job) PatchTasks(
	ctx context.Context,
	runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) error {

	patchSingleTask := func(id uint32) error {
		t, err := j.AddTask(ctx, id)
		if err != nil {
			return err
		}
		return t.PatchTask(ctx, runtimeDiffs[id])
	}

	return taskutil.RunInParallel(
		j.ID().GetValue(),
		getIdsFromDiffs(runtimeDiffs),
		patchSingleTask)
}

func (j *job) ReplaceTasks(
	taskInfos map[uint32]*pbtask.TaskInfo,
	forceReplace bool) error {

	replaceSingleTask := func(id uint32) error {
		t := j.addTaskToJobMap(id)
		return t.ReplaceTask(
			taskInfos[id].GetRuntime(),
			taskInfos[id].GetConfig(),
			forceReplace,
		)
	}

	return taskutil.RunInParallel(
		j.ID().GetValue(),
		getIdsFromTaskInfoMap(taskInfos),
		replaceSingleTask)
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
		t.DeleteTask()
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

func (j *job) Create(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn, createBy string) error {
	var runtimeCopy *pbjob.RuntimeInfo
	var jobType pbjob.JobType
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobRuntimeChanged(j.ID(), jobType,
			runtimeCopy)
	}()
	j.Lock()
	defer j.Unlock()

	if config == nil {
		return yarpcerrors.InvalidArgumentErrorf("missing config in jobInfo")
	}

	config = populateConfigChangeLog(config)

	// Add jobID to active jobs table before creating job runtime. This should
	// happen every time a job is first created.
	if err := j.jobFactory.jobStore.AddActiveJob(
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
	err := j.createJobConfig(ctx, config, configAddOn, createBy)
	if err != nil {
		j.invalidateCache()
		return err
	}
	jobType = j.jobType

	// both config and runtime are created, move the state to INITIALIZED
	j.runtime.State = pbjob.JobState_INITIALIZED
	if err := j.jobFactory.jobStore.UpdateJobRuntime(
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
		j.config.GetSLA(),
	); err != nil {
		j.invalidateCache()
		return err
	}

	runtimeCopy = proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	return nil
}

func (j *job) RollingCreate(
	ctx context.Context,
	config *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	updateConfig *pbupdate.UpdateConfig,
	opaqueData *peloton.OpaqueData,
	createBy string,
) error {
	var runtimeCopy *pbjob.RuntimeInfo
	var jobType pbjob.JobType

	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobRuntimeChanged(j.ID(), jobType,
			runtimeCopy)
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
	if err := j.jobFactory.jobStore.AddActiveJob(
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
	if err := j.createJobConfig(ctx, dummyConfig, configAddOn, createBy); err != nil && yarpcerrors.IsAlreadyExists(errors.Cause(err)) {
		j.invalidateCache()
		return err
	}

	// create the real config as the target config for update workflow.
	// Once the config is persisted successfully in db, the job is considered
	// as created successfully, and should be able to recover from
	// rest of the error. Calling RollingCreate after this call succeeds again,
	// would result in AlreadyExist error
	if err := j.createJobConfig(ctx, config, configAddOn, createBy); err != nil {
		j.invalidateCache()
		return err
	}
	jobType = j.jobType

	// both config and runtime are created, move the state to PENDING
	j.runtime.State = pbjob.JobState_PENDING
	if err := j.jobFactory.jobStore.UpdateJobRuntime(
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
		j.config.GetSLA(),
	); err != nil {
		j.invalidateCache()
		return err
	}

	j.workflows[updateID.GetValue()] = newWorkflow
	runtimeCopy = proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	return nil
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

// createJobConfig creates job config in db and cache
func (j *job) createJobConfig(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn, createBy string) error {
	if err := j.jobFactory.jobStore.CreateJobConfig(ctx, j.id, config, configAddOn, config.ChangeLog.Version, createBy); err != nil {
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

	err := j.jobFactory.jobStore.CreateJobRuntime(ctx, j.id, initialJobRuntime)
	if err != nil {
		return err
	}
	j.runtime = initialJobRuntime
	return nil
}

func (j *job) CompareAndSetRuntime(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error) {
	if jobRuntime == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf("unexpected nil jobRuntime")
	}

	var runtimeCopy *pbjob.RuntimeInfo
	var jobType pbjob.JobType
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobRuntimeChanged(j.ID(), jobType,
			runtimeCopy)
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
		return nil, jobmgrcommon.UnexpectedVersionError
	}

	// version matches, update the input changeLog
	newRuntime := *jobRuntime
	newRuntime.Revision = &peloton.ChangeLog{
		Version:   jobRuntime.GetRevision().GetVersion() + 1,
		CreatedAt: jobRuntime.GetRevision().GetCreatedAt(),
		UpdatedAt: uint64(time.Now().UnixNano()),
	}

	if err := j.jobFactory.jobStore.UpdateJobRuntime(
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
	runtimeCopy = proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
	jobType = j.jobType
	return runtimeCopy, nil
}

func (j *job) CompareAndSetConfig(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn) (jobmgrcommon.JobConfig, error) {
	j.Lock()
	defer j.Unlock()

	return j.compareAndSetConfig(ctx, config, configAddOn)
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

func (j *job) compareAndSetConfig(ctx context.Context, config *pbjob.JobConfig, configAddOn *models.ConfigAddOn) (jobmgrcommon.JobConfig, error) {
	// first make sure current config is in cache
	if err := j.populateCurrentJobConfig(ctx); err != nil {
		return nil, err
	}

	// then validate and merge config
	updatedConfig, err := j.validateAndMergeConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	// write the config into DB
	if err := j.jobFactory.jobStore.
		UpdateJobConfig(ctx, j.ID(), updatedConfig, configAddOn); err != nil {
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

// The runtime being passed should only set the fields which the caller intends to change,
// the remaining fields should be left unfilled.
// The config would be updated to the config passed in (except changeLog)
func (j *job) Update(ctx context.Context, jobInfo *pbjob.JobInfo, configAddOn *models.ConfigAddOn, req UpdateRequest) error {
	var runtimeCopy *pbjob.RuntimeInfo
	var jobType pbjob.JobType
	// notify listeners after dropping the lock
	defer func() {
		j.jobFactory.notifyJobRuntimeChanged(j.ID(), jobType,
			runtimeCopy)
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
			err := j.jobFactory.jobStore.UpdateJobConfig(ctx, j.ID(), updatedConfig, configAddOn)
			if err != nil {
				j.invalidateCache()
				return err
			}
		}

		if updatedRuntime != nil {
			err := j.jobFactory.jobStore.UpdateJobRuntime(ctx, j.ID(), updatedRuntime)
			if err != nil {
				j.invalidateCache()
				return err
			}
			runtimeCopy = proto.Clone(j.runtime).(*pbjob.RuntimeInfo)
		}

		if updatedConfig != nil || updatedRuntime != nil {
			if err := j.jobFactory.jobIndexOps.Update(
				ctx,
				j.ID(),
				updatedConfig,
				updatedRuntime,
			); err != nil {
				j.invalidateCache()
				runtimeCopy = nil
				return err
			}
		}
	}
	jobType = j.jobType
	return nil
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
			config, _, err := j.jobFactory.jobStore.GetJobConfig(ctx, j.ID().GetValue())
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

	j.config.hasControllerTask = hasControllerTask(config)

	j.config.jobType = config.GetType()
	j.jobType = j.config.jobType
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

	if len(newRuntime.GetTaskStatsByConfigurationVersion()) > 0 {
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

// Option to create a workflow
type Option interface {
	apply(*workflowOpts)
}

type workflowOpts struct {
	jobConfig       *pbjob.JobConfig
	prevJobConfig   *pbjob.JobConfig
	configAddOn     *models.ConfigAddOn
	instanceAdded   []uint32
	instanceUpdated []uint32
	instanceRemoved []uint32
	opaqueData      *peloton.OpaqueData
}

// WithConfig defines the original
// config and target config for the workflow.
// Workflow could use the configs to calculate
// the instances it would need to work on.
func WithConfig(
	jobConfig *pbjob.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
) Option {
	return &configOpt{
		jobConfig:     jobConfig,
		prevJobConfig: prevJobConfig,
		configAddOn:   configAddOn,
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
}

func (o *configOpt) apply(opts *workflowOpts) {
	opts.jobConfig = o.jobConfig
	opts.prevJobConfig = o.prevJobConfig
	opts.configAddOn = o.configAddOn
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
	j.Lock()
	defer j.Unlock()

	if err := j.ValidateEntityVersion(ctx, entityVersion); err != nil {
		return nil, nil, err
	}

	opts := &workflowOpts{}
	for _, option := range options {
		option.apply(opts)
	}

	newConfig, err := j.compareAndSetConfig(ctx, opts.jobConfig, opts.configAddOn)
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

func (j *job) PauseWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
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
	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) ResumeWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
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
	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) AbortWorkflow(
	ctx context.Context,
	entityVersion *v1alphapeloton.EntityVersion,
	options ...Option,
) (*peloton.UpdateID, *v1alphapeloton.EntityVersion, error) {
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
	return currentWorkflow.ID(), newEntityVersion, err
}

func (j *job) RollbackWorkflow(ctx context.Context) error {
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
		return j.updateJobRuntime(
			ctx,
			currentWorkflow.GetGoalState().JobVersion,
			j.runtime.GetWorkflowVersion(),
			currentWorkflow,
		)
	}

	// get the old job config before the workflow is run
	prevJobConfig, configAddOn, err := j.jobFactory.jobStore.
		GetJobConfigWithVersion(ctx, j.id.GetValue(), currentWorkflow.GetState().JobVersion)
	if err != nil {
		return errors.Wrap(err,
			"failed to get job config to copy for workflow rolling back")
	}

	// copy the old job config and get the config which
	// the workflow can "rollback" to
	configCopy, err := j.copyJobAndTaskConfig(ctx, prevJobConfig, configAddOn)
	if err != nil {
		return errors.Wrap(err,
			"failed to copy job and task config for workflow rolling back")
	}

	// get the job config the workflow is targeted at before rollback
	currentConfig, _, err := j.jobFactory.jobStore.
		GetJobConfigWithVersion(ctx, j.id.GetValue(), currentWorkflow.GetGoalState().JobVersion)
	if err != nil {
		return errors.Wrap(err,
			"failed to get current job config for workflow rolling back")
	}

	if err := currentWorkflow.Rollback(ctx, currentConfig, configCopy); err != nil {
		return err
	}

	return j.updateJobRuntime(
		ctx,
		configCopy.GetChangeLog().GetVersion(),
		j.runtime.GetWorkflowVersion(),
		currentWorkflow,
	)
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

	delete(j.workflows, updateID.GetValue())
}

func (j *job) GetStateCount() map[pbtask.TaskState]map[pbtask.TaskState]int {
	result := make(map[pbtask.TaskState]map[pbtask.TaskState]int)

	j.RLock()
	defer j.RUnlock()

	for _, t := range j.tasks {
		curState := t.CurrentState().State
		goalState := t.GoalState().State
		if _, ok := result[curState]; !ok {
			result[curState] = make(map[pbtask.TaskState]int)
		}
		result[curState][goalState]++
	}

	return result
}

// copyJobAndTaskConfig copies the provided job config and
// create task configs for the copy. It returns the job config
// copy with change log version updated.
func (j *job) copyJobAndTaskConfig(
	ctx context.Context,
	jobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
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
	configCopy, err := j.compareAndSetConfig(ctx, jobConfig, configAddOn)
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
	if err = j.CreateTaskConfigs(ctx, j.id, jobConfig, configAddOn); err != nil {
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
	if err := j.jobFactory.jobStore.UpdateJobRuntime(
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
		runtime, err := j.jobFactory.jobStore.GetJobRuntime(ctx, j.ID().GetValue())
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

	if err := j.jobFactory.jobStore.UpdateJobRuntime(ctx, j.id, newRuntime); err != nil {
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
	defer j.invalidateCache()

	if err := j.jobFactory.jobStore.DeleteJob(
		ctx,
		j.ID().GetValue(),
	); err != nil {
		return err
	}

	// delete from job_index
	if err := j.jobFactory.jobIndexOps.Delete(ctx, j.ID()); err != nil {
		return err
	}

	// delete from active_jobs
	return j.jobFactory.jobStore.DeleteActiveJob(ctx, j.ID())
}

func createEmptyResourceUsageMap() map[string]float64 {
	return map[string]float64{
		common.CPU:    float64(0),
		common.GPU:    float64(0),
		common.MEMORY: float64(0),
	}
}
