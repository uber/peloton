package tracked

import (
	"context"
	"sync"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	cmn_recovery "code.uber.internal/infra/peloton/common/recovery"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	_defaultMetricsUpdateTick = 10 * time.Second
)

// UpdateRequest is used to inform manager whether the caller wants to only update the DB
// or both DB update and scheduling the job/task for evaluation is needed
type UpdateRequest int

const (
	// UpdateOnly indicates only DB update is requested
	UpdateOnly UpdateRequest = iota
	// UpdateAndSchedule indicates both updating the DB and schedule the job/task for evaluation is requested
	UpdateAndSchedule
)

// Manager for tracking jobs and tasks. The manager has built in scheduler,
// for marking tasks as dirty and ready for being processed by the goal state
// engine.
type Manager interface {

	// GetJob will return the current tracked Job, nil if currently not tracked.
	GetJob(id *peloton.JobID) Job

	// GetAllJobs returns the list of all jobs in manager
	GetAllJobs() map[string]Job

	// SetTasks sets a map of tasks to new runtime info.
	// If req is set to UpdateAndSchedule, this will also scheduled these tasks for immediate evaluation.
	// UpdateOnly will be used by the caller is the caller is trying to execute a batch task operation.
	// In this case, SetTasks will merely update the DB and the cache, and let the caller complete the batch operation.
	// An example is the launching of tasks in a placement. It is a batch task operation because all tasks
	// in the placement need to go to host manager in one API call instead of being launched on a task by task basis.
	SetTasks(jobID *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest)

	// SetJob to the state in the JobInfo.
	SetJob(jobID *peloton.JobID, jobInfo *pb_job.JobInfo)

	// UpdateTaskRuntimes updates all tasks with th new runtime info, by first attempting to persist it,
	// and then storing it in the cache. If the attempt to persist fails, the local cache is cleaned up.
	// If successful and the request is to schedule the task for evaluation, then it also queues the
	// task into the deadline queue for immediate evaluation
	UpdateTaskRuntimes(ctx context.Context, jobID *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest) error

	// UpdateTaskRuntime is a helper function for UpdateTaskRuntimes which can be used to update a single
	// task without having the caller to create a map. It internally invokes UpdateTaskRuntimes.
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo, req UpdateRequest) error

	// UpdateJobRuntime updates job with the new runtime, by first attempting to persist
	// it. If that fails, it just returns back the error for now.
	// If successful, the job will get scheduled in the goal state for next action.
	UpdateJobRuntime(ctx context.Context, jobID *peloton.JobID, runtime *pb_job.RuntimeInfo, config *pb_job.JobConfig) error

	// ScheduleTask to be evaluated by the goal state engine, at deadline.
	ScheduleTask(t Task, deadline time.Time)

	// ScheduleJob to be evaluated by the goal state engine, at deadline.
	ScheduleJob(j Job, deadline time.Time)

	// WaitForScheduledTask blocked until a scheduled task is ready or the
	// stopChan is closed.
	WaitForScheduledTask(stopChan <-chan struct{}) Task

	// WaitForScheduledJob blocked until a scheduled job is ready or the
	// stopChan is closed.
	WaitForScheduledJob(stopChan <-chan struct{}) Job

	// Start syncs jobs and tasks from DB, starts emitting metrics.
	Start()

	// Stop clears the current tracked jobs and tasks, stops metrics.
	Stop()
}

// NewManager returns a new tracked manager.
func NewManager(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	taskLauncher launcher.Launcher,
	parentScope tally.Scope) Manager {
	scope := parentScope.SubScope("tracked")
	return &manager{
		jobs:          map[string]*job{},
		taskScheduler: newScheduler(NewQueueMetrics(scope.SubScope("tasks"))),
		jobScheduler:  newScheduler(NewQueueMetrics(scope.SubScope("jobs"))),
		hostmgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:  resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   volumeStore,
		taskLauncher:  taskLauncher,
		mtx:           NewMetrics(scope),
	}
}

type manager struct {
	sync.RWMutex

	// jobs maps from from peloton job id -> tracked job.
	jobs map[string]*job

	running bool

	taskScheduler *scheduler
	jobScheduler  *scheduler

	hostmgrClient hostsvc.InternalHostServiceYARPCClient
	resmgrClient  resmgrsvc.ResourceManagerServiceYARPCClient

	jobStore    storage.JobStore
	taskStore   storage.TaskStore
	volumeStore storage.PersistentVolumeStore

	taskLauncher launcher.Launcher

	mtx      *Metrics
	stopChan chan struct{}
}

func (m *manager) GetJob(id *peloton.JobID) Job {
	m.RLock()
	defer m.RUnlock()

	if j, ok := m.jobs[id.GetValue()]; ok {
		return j
	}

	return nil
}

func (m *manager) getJobStruct(id *peloton.JobID) *job {
	m.RLock()
	defer m.RUnlock()

	if j, ok := m.jobs[id.GetValue()]; ok {
		return j
	}

	return nil
}

func (m *manager) GetAllJobs() map[string]Job {
	m.RLock()
	defer m.RUnlock()
	jobMap := make(map[string]Job)
	for k, v := range m.jobs {
		jobMap[k] = v
	}
	return jobMap
}

func (m *manager) SetJob(jobID *peloton.JobID, jobInfo *pb_job.JobInfo) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	j := m.addJob(jobID)
	// Ok to add tracking of a job without any info.
	// Runtime will be dynamically obtained from DB when scheduled.
	if jobInfo != nil {
		j.updateRuntime(jobInfo)
	}
	m.jobScheduler.schedule(j, time.Now())
}

func (m *manager) SetTasks(jobID *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	j := m.addJob(jobID)
	taskMap := j.setTasks(runtimes)
	if req == UpdateOnly {
		return
	}
	for _, t := range taskMap {
		m.taskScheduler.schedule(t, time.Now())
	}
}

func (m *manager) UpdateJobRuntime(ctx context.Context, jobID *peloton.JobID, runtime *pb_job.RuntimeInfo, config *pb_job.JobConfig) error {
	if err := m.jobStore.UpdateJobRuntime(ctx, jobID, runtime); err != nil {
		return err
	}
	m.SetJob(jobID, &pb_job.JobInfo{Runtime: runtime, Config: config})
	return nil
}

func (m *manager) UpdateTaskRuntimes(ctx context.Context, jobID *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo, req UpdateRequest) error {
	if err := m.taskStore.UpdateTaskRuntimes(ctx, jobID, runtimes); err != nil {
		// Clear the runtime in the cache
		j := m.addJob(jobID)
		for instID := range runtimes {
			j.setTask(instID, nil)
		}
		return err
	}

	m.SetTasks(jobID, runtimes, req)
	return nil
}

func (m *manager) UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo, req UpdateRequest) error {
	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[instanceID] = runtime
	return m.UpdateTaskRuntimes(ctx, jobID, runtimes, req)
}

func (m *manager) ScheduleJob(j Job, deadline time.Time) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	m.jobScheduler.schedule(j.(*job), deadline)
}

func (m *manager) ScheduleTask(t Task, deadline time.Time) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	m.taskScheduler.schedule(t.(*task), deadline)
}

func (m *manager) WaitForScheduledJob(stopChan <-chan struct{}) Job {
	r := m.jobScheduler.waitForReady(stopChan)
	if r == nil {
		return nil
	}
	return r.(*job)
}

func (m *manager) WaitForScheduledTask(stopChan <-chan struct{}) Task {
	r := m.taskScheduler.waitForReady(stopChan)
	if r == nil {
		return nil
	}
	return r.(*task)
}

// addJob to the manager, if missing. The manager lock must be hold when called.
func (m *manager) addJob(id *peloton.JobID) *job {
	j, ok := m.jobs[id.GetValue()]
	if !ok {
		j = newJob(id, m)
		m.jobs[id.GetValue()] = j
	}

	return j
}

// clearTask from the manager, and remove job if needed.
func (m *manager) clearTask(t *task) {
	// Take both locks to ensure we don't clear job while concurrently adding to
	// it, but also ensuring we never take the manager lock while holding a job
	// lock, to avoid deadlocks.
	m.Lock()
	defer m.Unlock()

	// First check if the task is already scheduled. If it is, ignore.
	if t.index() != -1 {
		return
	}

	t.job.Lock()
	defer t.job.Unlock()

	if (t.job.config != nil) && (t.job.config.sla.GetMaximumRunningInstances() > 0) {
		if t.job.currentScheduledTasks > 0 {
			t.job.currentScheduledTasks--
		}
		err := t.job.evaluateJobSLA()
		if err != nil {
			log.WithError(err).
				WithField("job_id", t.job.ID()).
				WithField("instance_id", t.ID()).
				Error("failed to evaluate sla for the job")
			t.job.currentScheduledTasks++
			m.ScheduleTask(t, time.Now())
			return
		}
	}

	delete(t.job.tasks, t.id)
	// TODO fix delete of job.
	if len(t.job.tasks) == 0 {
		delete(m.jobs, t.job.id.GetValue())
	}
}

// Start syncs jobs and tasks from DB, starts emitting metrics.
func (m *manager) Start() {
	m.Lock()

	if m.running {
		m.Unlock()
		return
	}
	m.running = true

	m.stopChan = make(chan struct{})
	go m.runPublishMetrics(m.stopChan)

	m.Unlock()

	if err := m.syncFromDB(context.Background()); err != nil {
		log.WithError(err).Warn("failed to sync with DB in tracked manager")
	}

	log.Info("tracked.Manager started")
}

// Stop clears the current tracked jobs and tasks, stops emitting metrics.
func (m *manager) Stop() {
	m.Lock()
	defer m.Unlock()

	// Do not do anything if not runnning
	if !m.running {
		log.Info("tracked.Manager stopped")
		return
	}

	m.running = false

	for _, job := range m.jobs {
		for _, t := range job.tasks {
			m.taskScheduler.schedule(t, time.Time{})
		}
		m.jobScheduler.schedule(job, time.Time{})
	}

	m.jobs = map[string]*job{}

	close(m.stopChan)

	log.Info("tracked.Manager stopped")
}

func (m *manager) recoverTasks(ctx context.Context, jobID string, jobConfig *pb_job.JobConfig,
	jobRuntime *pb_job.RuntimeInfo, batch cmn_recovery.TasksBatch, errChan chan<- error) {

	id := &peloton.JobID{Value: jobID}
	// Do not set the job again if it already exists.
	if m.GetJob(id) == nil {
		m.SetJob(id, &pb_job.JobInfo{
			Runtime: jobRuntime,
			Config:  jobConfig,
		})
	}

	runtimes, err := m.taskStore.GetTaskRuntimesForJobByRange(
		ctx,
		id,
		&pb_task.InstanceRange{
			From: batch.From,
			To:   batch.To,
		})
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("from", batch.From).
			WithField("to", batch.To).
			Error("failed to fetch task runtimes")
		errChan <- err
		return
	}

	maxInstances := jobConfig.GetSla().GetMaximumRunningInstances()

	j := m.getJobStruct(id)
	scheduleRuntimes := make(map[uint32]*pb_task.RuntimeInfo)
	for instanceID, runtime := range runtimes {
		m.mtx.taskMetrics.TaskRecovered.Inc(1)
		// Do not add the task again if it already exists
		if j.GetTask(instanceID) == nil {
			if maxInstances > 0 {
				// Only add to job tracker, do not schedule to run it.
				j.setTask(instanceID, runtime)
				noTasks := uint32(len(j.GetAllTasks()))
				if noTasks == jobConfig.InstanceCount {
					// all tasks are present in tracker. now determine which ones need to start.
					j.recoverJobWithSLA()
				}
			} else {
				scheduleRuntimes[instanceID] = runtime
			}
		}
	}

	if len(scheduleRuntimes) > 0 {
		m.SetTasks(id, scheduleRuntimes, UpdateAndSchedule)
	}
	return
}

func (m *manager) syncFromDB(ctx context.Context) error {
	log.Info("syncing tracked manager with db")
	startRecoveryTime := time.Now()

	// jobStates represents the job states which need recovery
	jobStates := []pb_job.JobState{
		pb_job.JobState_INITIALIZED,
		pb_job.JobState_PENDING,
		pb_job.JobState_RUNNING,
		// Get failed and killed jobs in-case service jobs need to be restarted
		pb_job.JobState_FAILED,
		pb_job.JobState_KILLED,
		pb_job.JobState_UNKNOWN,
	}
	err := cmn_recovery.RecoverJobsByState(ctx, m.jobStore, jobStates, m.recoverTasks)
	if err != nil {
		return err
	}

	log.WithField("time_spent", time.Since(startRecoveryTime)).
		Info("syncing tracked manager with db is finished")
	m.mtx.jobMetrics.JobRecoveryDuration.Update(float64(time.Since(startRecoveryTime) / time.Millisecond))

	return nil
}

func (m *manager) runPublishMetrics(stopChan <-chan struct{}) {
	ticker := time.NewTicker(_defaultMetricsUpdateTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.publishMetrics()
		case <-stopChan:
			return
		}
	}
}

func (m *manager) publishMetrics() {
	// Initialise tasks count map for all possible pairs of (state, goal_state)
	tCount := map[pb_task.TaskState]map[pb_task.TaskState]float64{}
	for s := range pb_task.TaskState_name {
		tCount[pb_task.TaskState(s)] = map[pb_task.TaskState]float64{}
		for gs := range pb_task.TaskState_name {
			tCount[pb_task.TaskState(s)][pb_task.TaskState(gs)] = 0.0
		}
	}

	// Iterate through jobs, tasks and count
	m.RLock()
	jCount := float64(len(m.jobs))
	for _, j := range m.jobs {
		j.RLock()
		for _, t := range j.tasks {
			t.RLock()
			tCount[t.runtime.GetState()][t.runtime.GetGoalState()]++
			t.RUnlock()
		}
		j.RUnlock()
	}
	m.RUnlock()

	// Publish
	m.mtx.scope.Gauge("jobs_count").Update(jCount)
	for s, sm := range tCount {
		for gs, tc := range sm {
			m.mtx.scope.Tagged(map[string]string{"state": s.String(), "goal_state": gs.String()}).Gauge("tasks_count").Update(tc)
		}
	}
}
