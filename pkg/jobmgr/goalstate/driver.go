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

package goalstate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/recovery"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

// driverState indicates the running state of driver
type driverState int32

const (
	stopped driverState = iota + 1
	stopping
	starting
	started
)

// driverCacheState indicates the cache state of driver
type driverCacheState int32

const (
	cleaned driverCacheState = iota + 1
	populated
)

// Driver is the interface to enqueue jobs and tasks into the goal state engine
// for evaluation and then run the corresponding actions.
// The caller is also responsible for deleting from the goal state engine once
// the job/task is untracked from the cache.
type Driver interface {
	// EnqueueJob is used to enqueue a job into the goal state. It takes the job identifier
	// and the time at which the job should be evaluated by the goal state engine as inputs.
	EnqueueJob(jobID *peloton.JobID, deadline time.Time)
	// EnqueueTask is used to enqueue a task into the goal state. It takes the job identifier,
	// the instance identifier and the time at which the task should be evaluated by the
	// goal state engine as inputs.
	EnqueueTask(jobID *peloton.JobID, instanceID uint32, deadline time.Time)
	// EnqueueUpdate is used to enqueue a job update into the goal state. As
	// its input, it takes the job identifier, update identifier, and the
	// time at which the job update should be evaluated by the
	// goal state engine.
	EnqueueUpdate(
		jobID *peloton.JobID,
		updateID *peloton.UpdateID,
		deadline time.Time,
	)
	// DeleteJob deletes the job state from the goal state engine.
	DeleteJob(jobID *peloton.JobID)
	// DeleteTask deletes the task state from the goal state engine.
	DeleteTask(jobID *peloton.JobID, instanceID uint32)
	// DeleteUpdate deletes the job update state from the goal state engine.
	DeleteUpdate(jobID *peloton.JobID, updateID *peloton.UpdateID)
	// IsScheduledTask is a helper function to check if a given task is scheduled
	// for evaluation in the goal state engine.
	IsScheduledTask(jobID *peloton.JobID, instanceID uint32) bool
	// JobRuntimeDuration returns the mimimum inter-run duration between job
	// runtime updates. This duration is different for batch and service jobs.
	JobRuntimeDuration(jobType job.JobType) time.Duration
	// Start is used to start processing items in the goal state engine.
	Start()
	// Stop is used to stop the goal state engine.
	// If cleanUpCache is set to true, then all of the cache would be removed,
	// and it would be recovered when start again.
	// If cleanUpCache is not set, cache would be kept in the memory, and start
	// would skip cache recovery.
	Stop(cleanUpCache bool)
	// Started returns true if goal state engine has finished start process
	Started() bool
	// GetLockable returns an interface which controls lock/unlock operations in goal state engine
	GetLockable() lifecyclemgr.Lockable
}

// NewDriver returns a new goal state driver object.
func NewDriver(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	updateStore storage.UpdateStore,
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	jobType job.JobType,
	parentScope tally.Scope,
	cfg Config,
	hmVersion api.Version,
) Driver {
	cfg.normalize()
	scope := parentScope.SubScope("goalstate")
	jobScope := scope.SubScope("job")
	taskScope := scope.SubScope("task")
	workflowScope := scope.SubScope("workflow")

	driver := &driver{
		jobEngine: goalstate.NewEngine(
			cfg.NumWorkerJobThreads,
			cfg.FailureRetryDelay,
			cfg.MaxRetryDelay,
			jobScope),
		taskEngine: goalstate.NewEngine(
			cfg.NumWorkerTaskThreads,
			cfg.FailureRetryDelay,
			cfg.MaxRetryDelay,
			taskScope),
		updateEngine: goalstate.NewEngine(
			cfg.NumWorkerUpdateThreads,
			cfg.FailureRetryDelay,
			cfg.MaxRetryDelay,
			workflowScope),
		lm: lifecyclemgr.New(hmVersion, d, scope),
		resmgrClient: resmgrsvc.NewResourceManagerServiceYARPCClient(
			d.ClientConfig(common.PelotonResourceManager)),
		jobStore:        jobStore,
		taskStore:       taskStore,
		volumeStore:     volumeStore,
		updateStore:     updateStore,
		podEventsOps:    ormobjects.NewPodEventsOps(ormStore),
		activeJobsOps:   ormobjects.NewActiveJobsOps(ormStore),
		jobConfigOps:    ormobjects.NewJobConfigOps(ormStore),
		jobIndexOps:     ormobjects.NewJobIndexOps(ormStore),
		jobRuntimeOps:   ormobjects.NewJobRuntimeOps(ormStore),
		taskConfigV2Ops: ormobjects.NewTaskConfigV2Ops(ormStore),
		jobFactory:      jobFactory,
		mtx:             NewMetrics(scope),
		cfg:             &cfg,
		jobType:         jobType,
		jobScope:        jobScope,
		taskKillRateLimiter: rate.NewLimiter(
			cfg.RateLimiterConfig.TaskKill.Rate,
			cfg.RateLimiterConfig.TaskKill.Burst),
		executorShutShutdownRateLimiter: rate.NewLimiter(
			cfg.RateLimiterConfig.ExecutorShutdown.Rate,
			cfg.RateLimiterConfig.ExecutorShutdown.Burst),
	}

	driver.setState(stopped)
	driver.setCacheState(cleaned)
	return driver
}

// EnqueueJobWithDefaultDelay is a helper function to enqueue a job into the
// goal state engine with the default interval at which the job runtime
// updater is run. Using this function ensures that same job does not
// get enqueued too many times when multiple task updates for the job
// are received in a short duration of time.
// TODO(zhixin): pass in job type instead of cachedJob,
// remove GetJobType from the interface
func EnqueueJobWithDefaultDelay(
	jobID *peloton.JobID,
	goalStateDriver Driver,
	cachedJob cached.Job) {
	goalStateDriver.EnqueueJob(jobID, time.Now().Add(
		goalStateDriver.JobRuntimeDuration(cachedJob.GetJobType())))
}

type driver struct {
	// mutex to access jobEngine and taskEngine in this structure
	sync.RWMutex

	// jobEngine is the goal state engine for processing jobs and taskEngine is the goal
	// state engine for processing tasks. They are kept separate because there are much
	// fewer jobs than tasks, and job actions tend to run for more duration than
	// task actions and may require larger/more number of parallel Cassandra transactions.
	// Hence, setting one value for the engine properties like the number
	// of worker threads leads to problems. If the value is too small, then it results in
	// job starvation as well as not being able to run multiple tasks in parallel slowing
	// down the entire goal state engine processing. If the value is too large, then if all
	// the items scheduled are jobs, that can bring down Cassandra.
	jobEngine    goalstate.Engine
	taskEngine   goalstate.Engine
	updateEngine goalstate.Engine

	lm           lifecyclemgr.Manager
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient

	// jobStore, taskStore and volumeStore are the objects to the storage interface.
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	volumeStore     storage.PersistentVolumeStore
	updateStore     storage.UpdateStore
	podEventsOps    ormobjects.PodEventsOps
	activeJobsOps   ormobjects.ActiveJobsOps   // DB ops for active_jobs table
	jobConfigOps    ormobjects.JobConfigOps    // DB ops for job_config table
	jobRuntimeOps   ormobjects.JobRuntimeOps   // DB ops for job_runtime table
	jobIndexOps     ormobjects.JobIndexOps     // DB ops for job_index table
	taskConfigV2Ops ormobjects.TaskConfigV2Ops // DB ops for task_config_v2_table

	// jobFactory is the in-memory cache object fpr jobs and tasks
	jobFactory cached.JobFactory

	cfg        *Config  // goal state engine configuration
	mtx        *Metrics // goal state metrics
	running    int32    // whether driver is running or not
	cacheState int32    // the state of driver cache

	jobType job.JobType // the type of the job for the driver
	// job scope for goalstate driver
	jobScope tally.Scope

	// rate limiter for goal state engine initiated task stop
	taskKillRateLimiter *rate.Limiter

	//  rate limiter for goal state engine initiated executor shutdown
	executorShutShutdownRateLimiter *rate.Limiter
}

func (d *driver) EnqueueJob(jobID *peloton.JobID, deadline time.Time) {
	jobEntity := NewJobEntity(jobID, d)

	d.RLock()
	defer d.RUnlock()

	d.jobEngine.Enqueue(jobEntity, deadline)
}

func (d *driver) EnqueueTask(jobID *peloton.JobID, instanceID uint32, deadline time.Time) {
	taskEntity := NewTaskEntity(jobID, instanceID, d)

	d.RLock()
	defer d.RUnlock()

	d.taskEngine.Enqueue(taskEntity, deadline)
}

func (d *driver) EnqueueUpdate(
	jobID *peloton.JobID,
	updateID *peloton.UpdateID,
	deadline time.Time) {
	updateEntity := NewUpdateEntity(updateID, jobID, d)

	d.RLock()
	defer d.RUnlock()

	d.updateEngine.Enqueue(updateEntity, deadline)
}

func (d *driver) DeleteJob(jobID *peloton.JobID) {
	jobEntity := NewJobEntity(jobID, d)

	d.RLock()
	defer d.RUnlock()

	d.jobEngine.Delete(jobEntity)
}

func (d *driver) DeleteTask(jobID *peloton.JobID, instanceID uint32) {
	taskEntity := NewTaskEntity(jobID, instanceID, d)

	d.RLock()
	defer d.RUnlock()

	d.taskEngine.Delete(taskEntity)
}

func (d *driver) DeleteUpdate(jobID *peloton.JobID, updateID *peloton.UpdateID) {
	updateEntity := NewUpdateEntity(updateID, jobID, d)

	d.RLock()
	defer d.RUnlock()

	d.updateEngine.Delete(updateEntity)
}

func (d *driver) IsScheduledTask(jobID *peloton.JobID, instanceID uint32) bool {
	taskEntity := NewTaskEntity(jobID, instanceID, d)

	d.RLock()
	defer d.RUnlock()

	return d.taskEngine.IsScheduled(taskEntity)
}

func (d *driver) JobRuntimeDuration(jobType job.JobType) time.Duration {
	if jobType == job.JobType_BATCH {
		return d.cfg.JobBatchRuntimeUpdateInterval
	}
	return d.cfg.JobServiceRuntimeUpdateInterval
}

// recoverTasks recovers the job and tasks from DB when job manager instance
// gains leadership. The jobs and tasks are loaded into the cached and enqueued
// to the goal state engine for evaluation.
func (d *driver) recoverTasks(
	ctx context.Context,
	id string,
	jobConfig *job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *job.RuntimeInfo,
	batch recovery.TasksBatch,
	errChan chan<- error,
) {

	jobID := &peloton.JobID{Value: id}

	cachedJob := d.jobFactory.GetJob(jobID)
	// Do not set the job again if it already exists.
	if cachedJob == nil {
		cachedJob = d.jobFactory.AddJob(jobID)
		cachedJob.Update(ctx, &job.JobInfo{
			Runtime: jobRuntime,
			Config:  jobConfig,
		}, configAddOn,
			nil,
			cached.UpdateCacheOnly)
	}

	// Enqueue job into goal state
	d.EnqueueJob(jobID, time.Now().Add(d.JobRuntimeDuration(jobConfig.GetType())))

	taskInfos, err := d.taskStore.GetTasksForJobByRange(
		ctx,
		jobID,
		&task.InstanceRange{
			From: batch.From,
			To:   batch.To,
		})
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			WithField("from", batch.From).
			WithField("to", batch.To).
			Error("failed to fetch task infos")
		if yarpcerrors.IsNotFound(err) {
			// Due to task_config table deprecation, we might see old jobs
			// fail to recover due to their task config was created in
			// task_config table instead of task_config_v2. Only log the
			// error here instead of crashing jobmgr.
			return
		}
		errChan <- err
		return
	}

	for instanceID, taskInfo := range taskInfos {
		d.mtx.taskMetrics.TaskRecovered.Inc(1)
		runtime := taskInfo.GetRuntime()
		// Do not add the task again if it already exists
		if cachedJob.GetTask(instanceID) == nil {
			cachedJob.ReplaceTasks(
				map[uint32]*task.TaskInfo{instanceID: taskInfo},
				false,
			)
		}

		// Do not evaluate goal state for tasks which will be evaluated using job create tasks action.
		if runtime.GetState() != task.TaskState_INITIALIZED || jobRuntime.GetState() != job.JobState_INITIALIZED {
			d.EnqueueTask(jobID, instanceID, time.Now())
		}
	}

	// Recalculate the job resourceusage. Do this after recovering every task to
	// avoid getting into inconsistent state in case jobmgr restarts while a job
	// is partially recovered.
	if jobConfig.GetType() == job.JobType_BATCH {
		cachedJob.RecalculateResourceUsage(ctx)
	}

	// recover update if the job has an update,
	// and update is not already in cache.
	updateID := jobRuntime.GetUpdateID()
	if len(updateID.GetValue()) > 0 {
		d.EnqueueUpdate(
			jobID,
			updateID,
			time.Now().Add(d.JobRuntimeDuration(jobConfig.GetType())))
	}
	return
}

// syncFromDB syncs the jobs and tasks in DB when job manager instance
// gains leadership.
// TODO find the right place to run recovery in job manager.
func (d *driver) syncFromDB(ctx context.Context) error {
	log.Info("syncing cache and goal state with db")
	startRecoveryTime := time.Now()

	if err := recovery.RecoverActiveJobs(
		ctx,
		d.jobScope,
		d.activeJobsOps,
		d.jobConfigOps,
		d.jobRuntimeOps,
		d.recoverTasks,
	); err != nil {
		return err
	}

	log.WithField("time_spent", time.Since(startRecoveryTime)).
		Info("syncing cache and goal state with db is finished")
	d.mtx.jobMetrics.JobRecoveryDuration.Update(float64(time.Since(startRecoveryTime) / time.Millisecond))

	return nil
}

func (d *driver) Start() {
	for {
		state := d.getState()
		if state == starting || state == started {
			return
		}

		// make sure state did not change in-between
		if d.compareAndSwapState(state, starting) {
			break
		}
	}

	// only need to sync from DB if cache was cleaned up
	if d.getCacheState() == cleaned {
		if err := d.syncFromDB(context.Background()); err != nil {
			log.WithError(err).
				Fatal("failed to sync job manager with DB")
		}
		d.setCacheState(populated)
	}

	d.Lock()
	d.jobEngine.Start()
	d.taskEngine.Start()
	d.updateEngine.Start()
	d.Unlock()

	d.setState(started)
	log.Info("goalstate driver started")
}

func (d *driver) Started() bool {
	return d.getState() == started
}

func (d *driver) Stop(cleanUpCache bool) {
	for {
		// if cleanUpCache is set to true, but cache was not cleaned up,
		// continue the stopping process, no matter the current driver state,
		// because driver needs to get rid of the job factory cache
		if cleanUpCache && d.getCacheState() == populated {
			break
		}

		state := d.getState()
		if state == stopping || state == stopped {
			return
		}

		// make sure state did not change in-between
		if d.compareAndSwapState(state, stopping) {
			break
		}
	}

	d.Lock()
	d.updateEngine.Stop()
	d.taskEngine.Stop()
	d.jobEngine.Stop()
	d.Unlock()

	if cleanUpCache {
		d.cleanUpJobFactory()
		d.setCacheState(cleaned)
	}

	d.setState(stopped)
	log.Info("goalstate driver stopped")
}

// GetLockable returns the lockable interface of goal state driver,
// which defines the components that can be locked.
func (d *driver) GetLockable() lifecyclemgr.Lockable {
	return d.lm
}

func (d *driver) cleanUpJobFactory() {
	jobs := d.jobFactory.GetAllJobs()
	for jobID, cachedJob := range jobs {
		tasks := cachedJob.GetAllTasks()
		for instID := range tasks {
			d.DeleteTask(&peloton.JobID{
				Value: jobID,
			}, instID)
		}
		d.DeleteJob(&peloton.JobID{
			Value: jobID,
		})

		workflows := cachedJob.GetAllWorkflows()
		for updateID := range workflows {
			d.DeleteUpdate(&peloton.JobID{
				Value: jobID,
			},
				&peloton.UpdateID{
					Value: updateID,
				})
		}
	}
}

// getState returns the running state of the driver
func (d *driver) getState() driverState {
	return driverState(atomic.LoadInt32(&d.running))
}

func (d *driver) compareAndSwapState(oldState driverState, newState driverState) bool {
	return atomic.CompareAndSwapInt32(&d.running, int32(oldState), int32(newState))
}

func (d *driver) setState(state driverState) {
	atomic.StoreInt32(&d.running, int32(state))
}

func (d *driver) setCacheState(state driverCacheState) {
	atomic.StoreInt32(&d.cacheState, int32(state))
}

func (d *driver) getCacheState() driverCacheState {
	return driverCacheState(atomic.LoadInt32(&d.cacheState))
}
