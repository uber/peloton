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
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	_defaultMetricsUpdateTick = 10 * time.Second
)

// Manager for tracking jobs and tasks. The manager has built in scheduler,
// for marking tasks as dirty and ready for being processed by the goal state
// engine.
type Manager interface {
	// GetJob will return the current tracked Job, nil if currently not tracked.
	GetJob(id *peloton.JobID) Job

	// GetAllJobs returns the list of all jobs in manager
	GetAllJobs() map[string]Job

	// SetTask to the new runtime info. This will also schedule the task for
	// immediate evaluation.
	SetTask(jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo)

	// SetJob to the state in the JobInfo.
	SetJob(jobID *peloton.JobID, jobInfo *pb_job.JobInfo)

	// UpdateTaskRuntime with the new runtime info, by first attempting to persit
	// it. If it fail in persisting the change due to a data race, an
	// AlreadyExists error is returned.
	// If succesfull, this will also schedule the task for immediate evaluation.
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) error

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
		taskScheduler: newScheduler(newMetrics(scope.SubScope("tasks"))),
		jobScheduler:  newScheduler(newMetrics(scope.SubScope("jobs"))),
		hostmgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:  resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   volumeStore,
		taskLauncher:  taskLauncher,
		mtx:           newMetrics(scope),
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

	mtx      *metrics
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
	j.updateRuntime(jobInfo)
	m.jobScheduler.schedule(j, time.Now())
}

func (m *manager) SetTask(jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	j := m.addJob(jobID)
	t := j.setTask(instanceID, runtime)
	m.taskScheduler.schedule(t, time.Now())
}

func (m *manager) UpdateJobRuntime(ctx context.Context, jobID *peloton.JobID, runtime *pb_job.RuntimeInfo, config *pb_job.JobConfig) error {
	if err := m.jobStore.UpdateJobRuntime(ctx, jobID, runtime); err != nil {
		return err
	}
	m.SetJob(jobID, &pb_job.JobInfo{Runtime: runtime, Config: config})
	return nil
}

func (m *manager) UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) error {
	// TODO: We need to figure out how to handle this case, where we modify in
	// the non-leader.
	if err := m.taskStore.UpdateTaskRuntime(ctx, jobID, instanceID, runtime); err != nil {
		return err
	}

	m.SetTask(jobID, instanceID, runtime)
	return nil
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

	delete(t.job.tasks, t.id)
	// TODO fix delete of job.
	if len(t.job.tasks) == 0 {
		delete(m.jobs, t.job.id.GetValue())
	}
}

// Start syncs jobs and tasks from DB, starts emitting metrics.
func (m *manager) Start() {
	m.Lock()
	defer m.Unlock()

	if m.running {
		return
	}
	m.running = true

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		if err := m.syncFromDB(ctx); err != nil {
			log.WithError(err).Warn("failed to sync with DB in tracked manager")
		}
	}()

	m.stopChan = make(chan struct{})
	go m.runPublishMetrics(m.stopChan)

	log.Info("tracked.Manager started")
}

// Stop clears the current tracked jobs and tasks, stops emitting metrics.
func (m *manager) Stop() {
	m.Lock()
	defer m.Unlock()

	if !m.running {
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

func (m *manager) syncFromDB(ctx context.Context) error {
	log.Info("syncing tracked manager with db")
	numberTasks := 0
	startRecoveryTime := time.Now()

	// TODO: Skip completed jobs.
	jobs, err := m.jobStore.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	for id, jobRuntime := range jobs {
		jobID := &peloton.JobID{Value: id}
		// ignore jobs with terminal goal state and in terminal state
		switch jobRuntime.GetState() {
		case
			pb_job.JobState_FAILED,
			pb_job.JobState_KILLED,
			pb_job.JobState_SUCCEEDED:
			continue
		}

		jobConfig, err := m.jobStore.GetJobConfig(ctx, jobID)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id).
				Error("failed to load job config into goal state engine")
			continue
		}

		m.SetJob(jobID, &pb_job.JobInfo{
			Runtime: jobRuntime,
			Config:  jobConfig,
		})

		tasks, err := m.taskStore.GetTasksForJob(ctx, jobID)
		if err != nil {
			delete(m.jobs, id)
			log.WithError(err).
				WithField("job_id", id).
				Error("failed to load tasks for job into goal state engine")
			continue
		}

		for instanceID, task := range tasks {
			m.SetTask(jobID, instanceID, task.GetRuntime())
			numberTasks++
		}
	}

	log.WithField("no_of_tasks", numberTasks).
		WithField("time_spent", time.Since(startRecoveryTime)).
		Info("syncing tracked manager with db is finished")

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
