package cached

import (
	"context"
	"sync"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// UpdateRequest is used to indicate whether the caller wants to update only
// cache or update both database and cache.
// TODO this needs to be removed once all create and update calls
// job manager go throuch cache instead of directly calling storage.
type UpdateRequest int

const (
	// UpdateCacheOnly updates only the cache.
	UpdateCacheOnly UpdateRequest = iota
	// UpdateCacheAndDB updates both DB and cache.
	UpdateCacheAndDB
)

// Job in the cache.
type Job interface {
	// Identifier of the job.
	ID() *peloton.JobID

	// CleanAllTasks cleans up all tasks in the job
	ClearAllTasks()

	// CreateTasks creates the task runtimes in cache and DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	CreateTasks(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo) error

	// UpdateTasks updates all tasks with the new runtime info. If the request
	// is to update both DB and cache, it first attempts to persist it in storage,
	// and then storing it in the cache. If the attempt to persist fails, the local cache is cleaned up.
	UpdateTasks(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest) error

	// GetTask from the task id.
	GetTask(id uint32) Task

	// TODO remove this after deadline tracker is removed from job manager
	// GetAllTasks returns all tasks for the job
	GetAllTasks() map[uint32]Task

	// Create will be used to create the job configuration and runtime in DB.
	// Create and Update need to be different functions as the backing
	// storage calls are different.
	Create(ctx context.Context, jobInfo *pb_job.JobInfo) error

	// Update updates job with the new runtime. If the request is to update
	// both DB and cache, it first attempts to persist the request in storage,
	// If that fails, it just returns back the error for now.
	// If successful, the cache is updated as well.
	// TODO persist both job configuration and runtime. Only runtime is persisted with this call.
	// Job configuration persistence can be implemented after all create and update calls
	// go through the cache.
	Update(ctx context.Context, jobInfo *pb_job.JobInfo, req UpdateRequest) error

	// ClearRuntime sets the cached job runtime to nil
	// TODO remove after write-through cache
	ClearRuntime()

	// IsPartiallyCreated returns if job has not been fully created yet
	IsPartiallyCreated() bool

	// GetRuntime returns the runtime of the job
	GetRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error)

	// GetInstanceCount returns the instance count in the job config stored in the cache
	GetInstanceCount() uint32

	// GetSLAConfig returns the SLS configuration in the job config stored in the cache
	GetSLAConfig() *pb_job.SlaConfig

	// GetJobType returns the job type in the job config stored in the cache
	GetJobType() pb_job.JobType

	// SetTaskUpdateTime updates the task update times in the job cache
	SetTaskUpdateTime(t *float64)

	// GetFirstTaskUpdateTime gets the first task update time
	GetFirstTaskUpdateTime() float64

	// GetLastTaskUpdateTime gets the last task update time
	GetLastTaskUpdateTime() float64

	// SetLastDelay sets the last delay value.
	// Last delay is used by goal state to track expoenential backoff of scheduling duration
	// in case a job action keeps returning an error.
	SetLastDelay(delay time.Duration)

	// GetLastDelay gets the last delay value
	GetLastDelay() time.Duration
}

// newJob creates a new cache job object
func newJob(id *peloton.JobID, jobFactory *jobFactory) *job {
	return &job{
		id:         id,
		jobFactory: jobFactory,
		tasks:      map[uint32]*task{},
		lastDelay:  0,
	}
}

// job structure holds the information about a given active job
// in the cache. It should only hold information which either
// (i) a job manager component needs often and is expensive to
// fetch from the DB, or (ii) storing a view of underlying tasks
// which help with job lifecycle management.
type job struct {
	sync.RWMutex // Mutex to acquire before accessing any job information in cache

	id            *peloton.JobID   // The job identifier
	instanceCount uint32           // Instance count in the job configuration
	sla           pb_job.SlaConfig // SLA configuration in the job configuration
	jobType       pb_job.JobType   // Job type (batch or service) in the job configuration
	//TODO make job factory a singleton object so that it does not need to passed around
	jobFactory *jobFactory         // Pointer to the parent job factory object
	runtime    *pb_job.RuntimeInfo // Runtime information of the job
	lastDelay  time.Duration       // last intra-run duration of goal state action

	tasks map[uint32]*task // map of all job tasks

	// time at which the first mesos task update was received (indicates when a job starts running)
	firstTaskUpdateTime float64
	// time at which the last mesos task update was received (helps determine when job completes)
	lastTaskUpdateTime float64
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) updateTaskRuntimesInCache(runtimes map[uint32]*pb_task.RuntimeInfo) {
	for id, runtime := range runtimes {
		t, ok := j.tasks[id]
		if !ok {
			t = newTask(j.ID(), id)
			j.tasks[id] = t
		}
		t.UpdateRuntime(runtime)
	}
	return
}

// TODO implement this function.
func (j *job) CreateTasks(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo) error {
	return nil
}

func (j *job) UpdateTasks(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest) error {
	j.Lock()
	defer j.Unlock()

	if req == UpdateCacheAndDB {
		if err := j.jobFactory.taskStore.UpdateTaskRuntimes(ctx, j.ID(), runtimes); err != nil {
			// Clear the runtime in the cache
			for instID := range runtimes {
				j.updateTaskRuntimesInCache(map[uint32]*pb_task.RuntimeInfo{instID: nil})
			}
			return err
		}
	}

	j.updateTaskRuntimesInCache(runtimes)
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

func (j *job) updateJobInCache(jobInfo *pb_job.JobInfo) {
	j.runtime = jobInfo.GetRuntime()
	if jobInfo.GetConfig() != nil {
		j.instanceCount = jobInfo.GetConfig().GetInstanceCount()
		j.sla = *jobInfo.GetConfig().GetSla()
		j.jobType = jobInfo.GetConfig().GetType()
	}
}

// TODO implement
func (j *job) Create(ctx context.Context, jobInfo *pb_job.JobInfo) error {
	return nil
}

func (j *job) Update(ctx context.Context, jobInfo *pb_job.JobInfo, req UpdateRequest) error {
	j.Lock()
	defer j.Unlock()

	if req == UpdateCacheAndDB {
		if err := j.jobFactory.jobStore.UpdateJobRuntime(ctx, j.ID(), jobInfo.GetRuntime()); err != nil {
			return err
		}
	}
	j.updateJobInCache(jobInfo)
	return nil
}

func (j *job) ClearRuntime() {
	j.Lock()
	defer j.Unlock()
	j.runtime = nil
}

func (j *job) SetTaskUpdateTime(t *float64) {
	j.Lock()
	defer j.Unlock()

	if j.firstTaskUpdateTime == 0 {
		j.firstTaskUpdateTime = *t
	}

	j.lastTaskUpdateTime = *t
}

func (j *job) ClearAllTasks() {
	j.Lock()
	defer j.Unlock()

	for instID := range j.tasks {
		delete(j.tasks, instID)
	}
}

func (j *job) IsPartiallyCreated() bool {
	j.RLock()
	defer j.RUnlock()

	if j.instanceCount == uint32(len(j.tasks)) {
		return false
	}
	return true
}

func (j *job) GetRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error) {
	j.Lock()
	defer j.Unlock()

	if j.runtime != nil {
		return j.runtime, nil
	}
	return j.jobFactory.jobStore.GetJobRuntime(ctx, j.ID())
}

func (j *job) GetInstanceCount() uint32 {
	j.RLock()
	defer j.RUnlock()
	return j.instanceCount
}

func (j *job) GetSLAConfig() *pb_job.SlaConfig {
	j.RLock()
	defer j.RUnlock()
	// return a copy
	sla := j.sla
	return &sla
}

func (j *job) GetJobType() pb_job.JobType {
	j.RLock()
	defer j.RUnlock()
	return j.jobType
}

func (j *job) GetFirstTaskUpdateTime() float64 {
	j.RLock()
	j.RUnlock()

	return j.firstTaskUpdateTime
}

func (j *job) GetLastTaskUpdateTime() float64 {
	j.RLock()
	j.RUnlock()

	return j.lastTaskUpdateTime
}

func (j *job) SetLastDelay(delay time.Duration) {
	j.Lock()
	defer j.Unlock()
	j.lastDelay = delay
}

func (j *job) GetLastDelay() time.Duration {
	j.RLock()
	defer j.RUnlock()
	return j.lastDelay
}
