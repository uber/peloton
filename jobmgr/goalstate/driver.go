package goalstate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/recovery"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// _sleepRetryCheckRunningState is the duration to wait during stop/start while waiting for
// the driver to be running/to be not running..
const (
	_sleepRetryCheckRunningState = 10 * time.Millisecond
)

// driverState indicates whether driver is running or not
type driverState int32

const (
	notRunning driverState = iota + 1 // not running
	running                           // running
)

var (
	// jobStatesToRecover represents the job states which need recovery
	jobStatesToRecover = []job.JobState{
		job.JobState_INITIALIZED,
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_KILLING,
		// Get failed jobs in-case service jobs need to be restarted
		// Only killed and succeeded jobs are not recovered as of now.
		// TODO uncomment this after archiver has been put in to delete old jobs.
		//job.JobState_FAILED,
		// TODO remove recovery of UNKNOWN state after all old jobs created
		// before job goal state engine was added have terminated.
		job.JobState_UNKNOWN,
	}
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
	// DeleteJob deletes the job state from the goal state engine.
	DeleteJob(jobID *peloton.JobID)
	// DeleteTask deletes the task state from the goal state engine.
	DeleteTask(jobID *peloton.JobID, instanceID uint32)
	// IsScheduledTask is a helper function to check if a given task is scheduled
	// for evaluation in the goal state engine.
	IsScheduledTask(jobID *peloton.JobID, instanceID uint32) bool
	// Start is used to start processing items in the goal state engine.
	Start()
	// Stop is used to clean all items and then stop the goal state engine.
	Stop()
}

// NewDriver returns a new goal state driver object.
func NewDriver(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	jobFactory cached.JobFactory,
	taskLauncher launcher.Launcher,
	parentScope tally.Scope,
	cfg Config) Driver {
	cfg.normalize()
	scope := parentScope.SubScope("goalstate")

	return &driver{
		jobEngine:     goalstate.NewEngine(cfg.NumWorkerJobThreads, cfg.FailureRetryDelay, cfg.MaxRetryDelay, scope),
		taskEngine:    goalstate.NewEngine(cfg.NumWorkerTaskThreads, cfg.FailureRetryDelay, cfg.MaxRetryDelay, scope),
		hostmgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:  resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   volumeStore,
		jobFactory:    jobFactory,
		taskLauncher:  taskLauncher,
		mtx:           NewMetrics(scope),
		cfg:           &cfg,
	}
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
	jobEngine  goalstate.Engine
	taskEngine goalstate.Engine

	// hostmgrClient and resmgrClient are the host manager and resource manager clients.
	hostmgrClient hostsvc.InternalHostServiceYARPCClient
	resmgrClient  resmgrsvc.ResourceManagerServiceYARPCClient

	// jobStore, taskStore and volumeStore are the objects to the storage interface.
	jobStore    storage.JobStore
	taskStore   storage.TaskStore
	volumeStore storage.PersistentVolumeStore

	// jobFactory is the in-memory cache object
	jobFactory cached.JobFactory

	// taskLauncher is used to launch tasks to host manager
	taskLauncher launcher.Launcher

	cfg     *Config  // goal state engine configuration
	mtx     *Metrics // goal state metrics
	running int32    // whether driver is running or not
}

func (d *driver) EnqueueJob(jobID *peloton.JobID, deadline time.Time) {
	jobEntity := NewJobEntity(jobID, d)

	d.RLock()
	defer d.RUnlock()

	d.jobEngine.Enqueue(jobEntity, deadline)
}

func (d *driver) EnqueueTask(jobID *peloton.JobID, instanceID uint32, deadline time.Time) {
	taskEntity := NewTaskEntity(jobID, instanceID, d)
	jobEntity := NewJobEntity(jobID, d)

	cachedJob := d.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		// job has been untracked. Ignore.
		return
	}
	jobType := cachedJob.GetJobType()

	log.WithField("job_id", jobID.GetValue()).
		Info("enqueue job called")

	d.RLock()
	defer d.RUnlock()

	d.taskEngine.Enqueue(taskEntity, deadline)

	// When a task state changes, also enqueue the job to goal state for evaluation
	d.jobEngine.Enqueue(jobEntity, time.Now().Add(d.getJobRuntimeDuration(jobType)))
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

func (d *driver) IsScheduledTask(jobID *peloton.JobID, instanceID uint32) bool {
	taskEntity := NewTaskEntity(jobID, instanceID, d)

	d.RLock()
	defer d.RUnlock()

	return d.taskEngine.IsScheduled(taskEntity)
}

// getJobRuntimeDuration returns the mimimum inter-run duration between job
// runtime updates. This duration is different for batch and service jobs.
func (d *driver) getJobRuntimeDuration(jobType job.JobType) time.Duration {
	if jobType == job.JobType_BATCH {
		return d.cfg.JobBatchRuntimeUpdateInterval
	}
	return d.cfg.JobServiceRuntimeUpdateInterval
}

// recoverTasks recovers the job and tasks from DB when job manager instance
// gains leadership. The jobs and tasks are loaded into the cached and enqueued
// to the goal state engine for evaluation.
func (d *driver) recoverTasks(ctx context.Context, id string, jobConfig *job.JobConfig,
	jobRuntime *job.RuntimeInfo, batch recovery.TasksBatch, errChan chan<- error) {

	jobID := &peloton.JobID{Value: id}

	cachedJob := d.jobFactory.GetJob(jobID)
	// Do not set the job again if it already exists.
	if cachedJob == nil {
		cachedJob = d.jobFactory.AddJob(jobID)
		cachedJob.Update(ctx, &job.JobInfo{
			Runtime: jobRuntime,
			Config:  jobConfig,
		}, cached.UpdateCacheOnly)
	}

	// Enqueue job into goal state
	d.EnqueueJob(jobID, time.Now().Add(d.getJobRuntimeDuration(jobConfig.GetType())))

	runtimes, err := d.taskStore.GetTaskRuntimesForJobByRange(
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
			Error("failed to fetch task runtimes")
		errChan <- err
		return
	}

	for instanceID, runtime := range runtimes {
		d.mtx.taskMetrics.TaskRecovered.Inc(1)
		// Do not add the task again if it already exists
		if cachedJob.GetTask(instanceID) == nil {
			cachedJob.UpdateTasks(ctx, map[uint32]*task.RuntimeInfo{instanceID: runtime}, cached.UpdateCacheOnly)
		}

		// Do not evaluate goal state for tasks which will be evaluated using job create tasks action.
		if runtime.GetState() != task.TaskState_INITIALIZED || jobRuntime.GetState() != job.JobState_INITIALIZED {
			d.EnqueueTask(jobID, instanceID, time.Now())
		}
	}

	return
}

// syncFromDB syncs the jobs and tasks in DB when job manager instance
// gains leadership.
// TODO find the right place to run recovery in job manager.
func (d *driver) syncFromDB(ctx context.Context) error {
	log.Info("syncing cache and goal state with db")
	startRecoveryTime := time.Now()

	err := recovery.RecoverJobsByState(ctx, d.jobStore, jobStatesToRecover, d.recoverTasks)
	if err != nil {
		return err
	}

	log.WithField("time_spent", time.Since(startRecoveryTime)).
		Info("syncing cache and goal state with db is finished")
	d.mtx.jobMetrics.JobRecoveryDuration.Update(float64(time.Since(startRecoveryTime) / time.Millisecond))

	return nil
}

// runningState returns the running state of the driver
// (1 is not running, 2 if runing and 0 is invalid).
func (d *driver) runningState() int32 {
	return atomic.LoadInt32(&d.running)
}

func (d *driver) Start() {
	// Ensure that driver is not already running
	for {
		if d.runningState() != int32(running) {
			break
		}
		time.Sleep(_sleepRetryCheckRunningState)
	}

	d.Lock()
	d.jobEngine.Start()
	d.taskEngine.Start()
	d.Unlock()

	if err := d.syncFromDB(context.Background()); err != nil {
		// To be decided: Should this be Fatal?
		log.WithError(err).Error("failed to sync cache and goal state with DB")
	}

	atomic.StoreInt32(&d.running, int32(running))
	log.Info("goalstate driver started")
}

func (d *driver) Stop() {
	// Ensure that driver is running
	for {
		if d.runningState() != int32(notRunning) {
			break
		}
		time.Sleep(_sleepRetryCheckRunningState)
	}

	d.Lock()
	d.taskEngine.Stop()
	d.jobEngine.Stop()
	d.Unlock()

	// Cleanup tasks and jobs from the goal state machine
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
	}

	atomic.StoreInt32(&d.running, int32(notRunning))
	log.Info("goalstate driver stopped")
}
