package cached

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"
	stringsutil "code.uber.internal/infra/peloton/util/strings"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// writeTaskRuntimeToDB defines the interface of a function which will be used
// to write task runtimes to DB in parallel
type writeTaskRuntimeToDB func(ctx context.Context, instanceID uint32,
	runtime *pbtask.RuntimeInfo, req UpdateRequest,
	owner string) error

type singleTask func(id uint32) error

// RuntimeDiff to be applied to the runtime struct.
// key is the field name to be updated,
// value is the value to be updated to.
type RuntimeDiff map[string]interface{}

// Job in the cache.
// TODO there a lot of methods in this interface. To determine if
// this can be broken up into smaller pieces.
type Job interface {
	// Identifier of the job.
	ID() *peloton.JobID

	// CreateTasks creates the task runtimes in cache and DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	CreateTasks(ctx context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, owner string) error

	// UpdateTasks updates all tasks with the new runtime info. If the request
	// is to update both DB and cache, it first attempts to persist it in storage,
	// and then storing it in the cache. If the attempt to persist fails, the local cache is cleaned up.
	UpdateTasks(ctx context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, req UpdateRequest) error

	// PatchTasks patch runtime diff to the existing task cache. runtimeDiffs
	// is a kv map with key as the instance_id of the task to be updated.
	// Value of runtimeDiffs is RuntimeDiff, of which key is the field name
	// to be update, and value is the new value of the field. PatchTasks
	// would save the change in both cache and DB. If persisting to DB fails,
	// cache would be invalidated as well.
	PatchTasks(ctx context.Context, runtimeDiffs map[uint32]RuntimeDiff) error

	// ReplaceTasks replaces task runtime with runtimes in cache.
	// If forceReplace is false, it would check Revision version
	// and decide whether to replace the runtime.
	// If forceReplace is true, the func would always replace the runtime,
	ReplaceTasks(runtimes map[uint32]*pbtask.RuntimeInfo, forceReplace bool) error

	// AddTask adds a new task to the job, and if already present, just returns it
	AddTask(id uint32) Task

	// GetTask from the task id.
	GetTask(id uint32) Task

	// GetAllTasks returns all tasks for the job
	GetAllTasks() map[uint32]Task

	// Create will be used to create the job configuration and runtime in DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	Create(ctx context.Context, config *pbjob.JobConfig, createBy string) error

	// Update updates job with the new runtime and config. If the request is to update
	// both DB and cache, it first attempts to persist the request in storage,
	// If that fails, it just returns back the error for now.
	// If successful, the cache is updated as well.
	Update(ctx context.Context, jobInfo *pbjob.JobInfo, req UpdateRequest) error

	// IsPartiallyCreated returns if job has not been fully created yet
	IsPartiallyCreated(config JobConfig) bool

	// GetRuntime returns the runtime of the job
	GetRuntime(ctx context.Context) (*pbjob.RuntimeInfo, error)

	// GetConfig returns the config of the job
	GetConfig(ctx context.Context) (JobConfig, error)

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
}

//JobConfig stores the job configurations in cache which is fetched multiple
// times during normal job/task operations.
// JobConfig makes the job interface cleaner by having the caller request
// for the configuration first (which can fail due to Cassandra errors
// if cache is invalid or not populated yet), and then fetch the needed
// configuration from the interface. Otherwise, caller needs to deal with
// context and err for each config related call.
// The interface exposes get methods only so that the caller cannot
// overwrite any of these configurations.
type JobConfig interface {
	// GetInstanceCount returns the instance count
	// in the job config stored in the cache
	GetInstanceCount() uint32
	// GetType returns the type of the job stored in the cache
	GetType() pbjob.JobType
	// GetRespoolID returns the respool id stored in the cache
	GetRespoolID() *peloton.ResourcePoolID
	// GetSLA returns the SLA configuration
	// in the job config stored in the cache
	GetSLA() *pbjob.SlaConfig
	// GetChangeLog returns the changeLog in the job config stored in the cache
	GetChangeLog() *peloton.ChangeLog
}

// newJob creates a new cache job object
func newJob(id *peloton.JobID, jobFactory *jobFactory) *job {
	return &job{
		id: id,
		// jobFactory is stored in the job instead of using the singleton object
		// because job needs access to the different stores in the job factory
		// which are private variables and not available to other packages.
		jobFactory: jobFactory,
		tasks:      map[uint32]*task{},
	}
}

// cachedConfig structure holds the config fields need to be cached
type cachedConfig struct {
	instanceCount uint32                  // Instance count in the job configuration
	sla           *pbjob.SlaConfig        // SLA configuration in the job configuration
	jobType       pbjob.JobType           // Job type (batch or service) in the job configuration
	changeLog     *peloton.ChangeLog      // ChangeLog in the job configuration
	respoolID     *peloton.ResourcePoolID // Resource Pool ID in the job configuration
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

	jobFactory *jobFactory // Pointer to the parent job factory object

	tasks map[uint32]*task // map of all job tasks

	// time at which the first mesos task update was received (indicates when a job starts running)
	firstTaskUpdateTime float64
	// time at which the last mesos task update was received (helps determine when job completes)
	lastTaskUpdateTime float64
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) AddTask(id uint32) Task {
	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTask(j.ID(), id, j.jobFactory)
		j.tasks[id] = t
	}
	return t
}

// updateTasksInParallel runs go routines which will create/update tasks
// in parallel to improve performance.
func (j *job) updateTasksInParallel(
	ctx context.Context,
	runtimes map[uint32]*pbtask.RuntimeInfo,
	owner string, req UpdateRequest,
	write writeTaskRuntimeToDB) error {

	var instanceIDList []uint32
	var transientError int32

	nTasks := uint32(len(runtimes))
	transientError = 0 // indicates if the runtime create/update hit a transient error

	for instanceID := range runtimes {
		instanceIDList = append(instanceIDList, instanceID)
	}

	// how many tasks failed to update due to errors
	tasksNotUpdated := uint32(0)

	// Each go routine will update at least (nTasks / _defaultMaxParallelBatches)
	// number of tasks. In addition if nTasks % _defaultMaxParallelBatches > 0,
	// the first increment number of go routines are going to run
	// one additional task.
	increment := nTasks % _defaultMaxParallelBatches

	timeStart := time.Now()
	wg := new(sync.WaitGroup)
	prevEnd := uint32(0)

	// run the parallel batches
	for i := uint32(0); i < _defaultMaxParallelBatches; i++ {
		// start of the batch
		updateStart := prevEnd
		// end of the batch
		updateEnd := updateStart + (nTasks / _defaultMaxParallelBatches)
		if increment > 0 {
			updateEnd++
			increment--
		}

		if updateEnd > nTasks {
			updateEnd = nTasks
		}
		prevEnd = updateEnd
		if updateStart == updateEnd {
			continue
		}
		wg.Add(1)

		// Start a go routine to update all tasks in a batch
		go func() {
			defer wg.Done()
			for k := updateStart; k < updateEnd; k++ {
				instanceID := instanceIDList[k]
				runtime := runtimes[instanceID]
				if runtime == nil {
					continue
				}

				err := write(ctx, instanceID, runtimes[instanceID], req, owner)
				if err != nil {
					log.WithError(err).
						WithFields(log.Fields{
							"job_id":      j.ID().GetValue(),
							"instance_id": instanceID,
						}).Info("failed to write task runtime")
					atomic.AddUint32(&tasksNotUpdated, 1)
					if common.IsTransientError(err) {
						atomic.StoreInt32(&transientError, 1)
					}
					return
				}
			}
		}()
	}
	// wait for all batches to complete
	wg.Wait()

	if tasksNotUpdated != 0 {
		msg := fmt.Sprintf(
			"Updated %d task runtimes for %v, and was unable to write %d tasks in %v",
			nTasks-tasksNotUpdated,
			j.ID(),
			tasksNotUpdated,
			time.Since(timeStart))
		if transientError > 0 {
			// return a transient error if a transient error is encountered
			// while creating.updating any task
			return yarpcerrors.AbortedErrorf(msg)
		}
		return yarpcerrors.InternalErrorf(msg)
	}
	return nil
}

// createSingleTask is a helper function to crate a single task
func (j *job) createSingleTask(
	ctx context.Context,
	instanceID uint32,
	runtime *pbtask.RuntimeInfo,
	req UpdateRequest,
	owner string) error {
	now := time.Now().UTC()
	runtime.Revision = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   1,
	}

	if j.GetTask(instanceID) != nil {
		return yarpcerrors.InvalidArgumentErrorf("task %d already exists", instanceID)
	}

	t := j.AddTask(instanceID)
	return t.CreateRuntime(ctx, runtime, owner)
}

func (j *job) CreateTasks(
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
			return yarpcerrors.InvalidArgumentErrorf("task %d already exists", id)
		}

		t := j.AddTask(id)
		return t.CreateRuntime(ctx, runtime, owner)
	}
	return j.runInParallel(getIdsFromRuntimeMap(runtimes), createSingleTask)
}

// updateSingleTask is a helper function to update a single task
func (j *job) updateSingleTask(
	ctx context.Context,
	instanceID uint32,
	runtime *pbtask.RuntimeInfo,
	req UpdateRequest,
	owner string) error {
	t := j.AddTask(instanceID)
	return t.UpdateRuntime(ctx, runtime, req)
}

func (j *job) UpdateTasks(
	ctx context.Context,
	runtimes map[uint32]*pbtask.RuntimeInfo,
	req UpdateRequest) error {
	return j.updateTasksInParallel(ctx, runtimes, "", req, j.updateSingleTask)
}

// PatchTasks replace UpdateTasks with UpdateCacheAndDB
// TODO(zhixin): replace UpdateTasks
func (j *job) PatchTasks(
	ctx context.Context,
	runtimeDiffs map[uint32]RuntimeDiff) error {

	patchSingleTask := func(id uint32) error {
		t := j.AddTask(id)
		return t.PatchRuntime(ctx, runtimeDiffs[id])
	}

	return j.runInParallel(getIdsFromDiffs(runtimeDiffs), patchSingleTask)
}

func (j *job) ReplaceTasks(
	runtimes map[uint32]*pbtask.RuntimeInfo,
	forceReplace bool) error {

	replaceSingleTask := func(id uint32) error {
		t := j.AddTask(id)
		return t.ReplaceRuntime(runtimes[id], forceReplace)
	}

	return j.runInParallel(getIdsFromRuntimeMap(runtimes), replaceSingleTask)
}

// runInParallel runs go routines which will create/update tasks
// TODO(zhixin): updateTasksInParallel
func (j *job) runInParallel(idList []uint32, task singleTask) error {
	var transientError int32

	nTasks := uint32(len(idList))
	// indicates if the runtime create/update hit a transient error
	transientError = 0

	// how many tasks failed to run due to errors
	tasksNotRun := uint32(0)

	// Each go routine will update at least (nTasks / _defaultMaxParallelBatches)
	// number of tasks. In addition if nTasks % _defaultMaxParallelBatches > 0,
	// the first increment number of go routines are going to run
	// one additional task.
	increment := nTasks % _defaultMaxParallelBatches

	timeStart := time.Now()
	wg := new(sync.WaitGroup)
	prevEnd := uint32(0)

	// run the parallel batches
	for i := uint32(0); i < _defaultMaxParallelBatches; i++ {
		// start of the batch
		updateStart := prevEnd
		// end of the batch
		updateEnd := updateStart + (nTasks / _defaultMaxParallelBatches)
		if increment > 0 {
			updateEnd++
			increment--
		}

		if updateEnd > nTasks {
			updateEnd = nTasks
		}
		prevEnd = updateEnd
		if updateStart == updateEnd {
			continue
		}
		wg.Add(1)

		// Start a go routine to update all tasks in a batch
		go func() {
			defer wg.Done()
			for k := updateStart; k < updateEnd; k++ {
				id := idList[k]
				err := task(id)
				if err != nil {
					log.WithError(err).
						WithFields(log.Fields{
							"job_id":      j.ID().GetValue(),
							"instance_id": id,
						}).Info("failed to write task runtime")
					atomic.AddUint32(&tasksNotRun, 1)
					if common.IsTransientError(err) {
						atomic.StoreInt32(&transientError, 1)
					}
					return
				}
			}
		}()
	}
	// wait for all batches to complete
	wg.Wait()

	if tasksNotRun != 0 {
		msg := fmt.Sprintf(
			"Updated %d task runtimes for %v, and was unable to write %d tasks in %v",
			nTasks-tasksNotRun,
			j.ID(),
			tasksNotRun,
			time.Since(timeStart))
		if transientError > 0 {
			// return a transient error if a transient error is encountered
			// while creating.updating any task
			return yarpcerrors.AbortedErrorf(msg)
		}
		return yarpcerrors.InternalErrorf(msg)
	}
	return nil
}

func (j *job) GetTask(id uint32) Task {
	j.RLock()
	defer j.RUnlock()

	if t, ok := j.tasks[id]; ok {
		return t
	}

	return nil
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

func (j *job) Create(ctx context.Context, config *pbjob.JobConfig, createBy string) error {
	j.Lock()
	defer j.Unlock()

	if config == nil {
		return yarpcerrors.InvalidArgumentErrorf("missing config in jobInfo")
	}

	config, err := j.createJobConfig(ctx, config, createBy)
	if err != nil {
		j.invalidateCache()
		return err
	}

	err = j.createJobRuntime(ctx, config)
	if err != nil {
		j.invalidateCache()
		return err
	}
	return nil
}

// createJobConfig creates job config in db and cache
func (j *job) createJobConfig(ctx context.Context, config *pbjob.JobConfig, createBy string) (*pbjob.JobConfig, error) {
	newConfig := *config
	now := time.Now().UTC()
	newConfig.ChangeLog = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   1,
	}
	err := j.jobFactory.jobStore.CreateJobConfig(ctx, j.id, &newConfig, newConfig.ChangeLog.Version, createBy)
	if err != nil {
		return nil, err
	}
	j.populateJobConfigCache(&newConfig)
	return &newConfig, nil
}

// createJobRuntime creates and initialize job runtime in db and cache
func (j *job) createJobRuntime(ctx context.Context, config *pbjob.JobConfig) error {
	goalState := goalstateutil.GetDefaultJobGoalState(config.Type)
	now := time.Now().UTC()
	initialJobRuntime := &pbjob.RuntimeInfo{
		State:        pbjob.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: config.GetChangeLog().GetVersion(),
	}
	// Init the task stats to reflect that all tasks are in initialized state
	initialJobRuntime.TaskStats[pbtask.TaskState_INITIALIZED.String()] = config.InstanceCount

	err := j.jobFactory.jobStore.CreateJobRuntimeWithConfig(ctx, j.id, initialJobRuntime, config)
	if err != nil {
		return err
	}
	j.runtime = initialJobRuntime
	return nil
}

// The runtime being passed should only set the fields which the caller intends to change,
// the remaining fields should be left unfilled.
// The config would be updated to the config passed in (except changeLog)
func (j *job) Update(ctx context.Context, jobInfo *pbjob.JobInfo, req UpdateRequest) error {
	j.Lock()
	defer j.Unlock()

	var updatedConfig *pbjob.JobConfig
	var err error
	if jobInfo.GetConfig() != nil {
		updatedConfig, err = j.getUpdatedJobConfigCache(ctx, jobInfo.GetConfig(), req)
		if err != nil {
			j.invalidateCache()
			return err
		}
		if updatedConfig != nil {
			j.populateJobConfigCache(updatedConfig)

			// if changeLog revision is not nil, config is recovered instead of
			// updated. No need to update ConfigurationVersion, when config is recovered
			if jobInfo.GetConfig().GetChangeLog() == nil {
				// update runtime Configuration version with changeLog revision
				if jobInfo.GetRuntime() == nil {
					jobInfo.Runtime = &pbjob.RuntimeInfo{}
				}
				jobInfo.Runtime.ConfigurationVersion = updatedConfig.GetChangeLog().GetVersion()
			}
		}
	}

	var updatedRuntime *pbjob.RuntimeInfo
	if jobInfo.GetRuntime() != nil {
		updatedRuntime, err = j.getUpdatedJobRuntimeCache(ctx, jobInfo.GetRuntime(), req)
		if err != nil {
			j.invalidateCache()
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
			err := j.jobFactory.jobStore.UpdateJobConfig(ctx, j.ID(), updatedConfig)
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
		}
	}
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
			config, err := j.jobFactory.jobStore.GetJobConfig(ctx, j.ID())
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
	if !j.validateConfig(config) {
		log.WithField("current_revision", j.config.GetChangeLog().GetVersion()).
			WithField("new_revision", config.GetChangeLog().GetVersion()).
			WithField("job_id", j.id.Value).
			Info("failed job config validation")
		return nil, nil
	}

	newConfig := *config
	// ChangeLog is nil when update the config,
	if newConfig.ChangeLog == nil {
		maxVersion, err := j.jobFactory.jobStore.GetMaxJobConfigVersion(ctx, j.id)
		if err != nil {
			return nil, err
		}

		currentChangeLog := *j.config.changeLog
		newConfig.ChangeLog = &currentChangeLog
		// update version to maxVersion + 1,
		// instead of config.ChangeLog.Version + 1
		// If config update succeed and runtime update
		// fails, runtime would still points to the old config.
		// If user tries to update the config again, job manager will
		// read the old config version and update based on
		// the old config would cause a 'config already exists' error
		newConfig.ChangeLog.Version = maxVersion + 1
		newConfig.ChangeLog.UpdatedAt = uint64(time.Now().UnixNano())
	}
	return &newConfig, nil
}

// validateConfig validates whether the input config is valid
// to update the exisiting config cache
func (j *job) validateConfig(newConfig *pbjob.JobConfig) bool {
	currentConfig := j.config

	if newConfig == nil {
		return false
	}

	// changeLog is not nil, newConfig is from db
	if newConfig.GetChangeLog() != nil {
		// Make sure that not overwriting with old or same version
		if newConfig.GetChangeLog().GetVersion() <= currentConfig.GetChangeLog().GetVersion() {
			return false
		}
	}
	return true
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

	j.config.jobType = config.GetType()
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
		if j.runtime == nil {
			runtime, err := j.jobFactory.jobStore.GetJobRuntime(ctx, j.ID())
			if err != nil {
				return nil, err
			}
			j.runtime = runtime
		}
	}

	if j.runtime != nil {
		newRuntime = j.validateAndMergeRuntime(runtime, req)
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

	if newRuntime.GetConfigVersion() > 0 {
		runtime.ConfigVersion = newRuntime.GetConfigVersion()
	}

	if newRuntime.GetConfigurationVersion() > 0 {
		runtime.ConfigurationVersion = newRuntime.GetConfigurationVersion()
	}

	if newRuntime.GetGoalState() != pbjob.JobState_UNKNOWN {
		runtime.GoalState = newRuntime.GetGoalState()
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
	runtime.Revision.Version++
	runtime.Revision.UpdatedAt = uint64(time.Now().UnixNano())

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

func (j *job) IsPartiallyCreated(config JobConfig) bool {
	j.RLock()
	defer j.RUnlock()

	if config.GetInstanceCount() == uint32(len(j.tasks)) {
		return false
	}
	return true
}

func (j *job) GetRuntime(ctx context.Context) (*pbjob.RuntimeInfo, error) {
	j.Lock()
	defer j.Unlock()

	if j.runtime == nil {
		runtime, err := j.jobFactory.jobStore.GetJobRuntime(ctx, j.ID())
		if err != nil {
			return nil, err
		}
		j.runtime = runtime
	}
	return j.runtime, nil
}

func (j *job) GetConfig(ctx context.Context) (JobConfig, error) {
	j.Lock()
	defer j.Unlock()

	if j.config == nil {
		config, err := j.jobFactory.jobStore.GetJobConfig(ctx, j.ID())
		if err != nil {
			return nil, err
		}
		j.populateJobConfigCache(config)
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

func getIdsFromRuntimeMap(input map[uint32]*pbtask.RuntimeInfo) []uint32 {
	result := make([]uint32, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

func getIdsFromDiffs(input map[uint32]RuntimeDiff) []uint32 {
	result := make([]uint32, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}
