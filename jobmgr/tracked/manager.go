package tracked

import (
	"context"
	"fmt"
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

	// SetTask to the new runtime info. This will also schedule the task for
	// immediate evaluation.
	SetTask(jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo)

	// UpdateTaskRuntime with the new runtime info, by first attempting to persit
	// it. If it fail in persisting the change due to a data race, an
	// AlreadyExists error is returned.
	// If succesfull, this will also schedule the task for immediate evaluation.
	UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) error

	// GetTaskRuntime by reading it from the DB. If it was able to read it, it
	// update the task with the read value.
	GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*pb_task.RuntimeInfo, error)

	// ScheduleTask to be evaluated by the goal state engine, at deadline.
	ScheduleTask(t Task, deadline time.Time)

	// WaitForScheduledTask blocked until a scheduled task is ready or the
	// stopChan is closed.
	WaitForScheduledTask(stopChan <-chan struct{}) Task

	// RunTaskAction runs the action on the task id, maintaining the job SLA.
	// As a consequence of the SLA, the action may be buffered, or ignored.
	RunTaskAction(ctx context.Context, id *peloton.JobID, instanceID uint32, action TaskAction) error

	// Start syncs jobs and tasks from DB, starts emitting metrics.
	Start() error

	// Stop clears the current tracked jobs and tasks, stops metrics.
	Stop() error
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
	if j := m.getJob(id); j != nil {
		return j
	}

	return nil
}

func (m *manager) getJob(id *peloton.JobID) *job {
	m.RLock()
	defer m.RUnlock()

	return m.jobs[id.GetValue()]
}

func (m *manager) SetTask(jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	cv := UnknownVersion
	if runtime != nil {
		cv = runtime.ConfigVersion
	}

	// TODO: the job state should actually be set and maintained by the job GSE.
	// We set here a default state in case this is the first time we see the
	// job.
	j := m.addJob(jobID, &jobState{
		state:         pb_job.JobState_UNKNOWN,
		configVersion: cv,
	})
	t := j.setTask(instanceID, runtime)
	m.taskScheduler.schedule(t, time.Now())
}

func (m *manager) UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) error {
	// TODO: We need to figure out how to handle this case, where we modify in
	// the non-leader.
	if err := m.taskStore.UpdateTaskRuntime(ctx, jobID, instanceID, runtime); err != nil {
		// If update failed, clear runtime so latest will be read.
		m.SetTask(jobID, instanceID, nil)
		return err
	}

	m.SetTask(jobID, instanceID, runtime)
	return nil
}

func (m *manager) GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*pb_task.RuntimeInfo, error) {
	// TODO: We need to figure out how to handle this case, where we modify in
	// the non-leader.
	runtime, err := m.taskStore.GetTaskRuntime(ctx, jobID, instanceID)
	if err != nil {
		return nil, err
	}

	m.SetTask(jobID, instanceID, runtime)
	return runtime, nil
}

func (m *manager) ScheduleTask(t Task, deadline time.Time) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	m.taskScheduler.schedule(t.(*task), deadline)
}

func (m *manager) WaitForScheduledTask(stopChan <-chan struct{}) Task {
	r := m.taskScheduler.waitForReady(stopChan)
	if r == nil {
		return nil
	}

	return r.(*task)
}

func (m *manager) RunTaskAction(ctx context.Context, id *peloton.JobID, instanceID uint32, action TaskAction) error {
	defer m.mtx.scope.Tagged(map[string]string{"action": string(action)}).Timer("run_duration").Start().Stop()

	log.WithFields(log.Fields{
		"jobID":      id.Value,
		"instanceID": instanceID,
		"action":     action,
	}).Info("RunTaskAction")

	j := m.getJob(id)
	if j == nil {
		return nil
	}

	var err error
	switch action {
	case StartAction:
		err = j.tryStartTask(ctx, instanceID)
	default:
		err = m.runTaskAction(ctx, id, instanceID, action)
	}

	return err
}

func (m *manager) runTaskAction(ctx context.Context, id *peloton.JobID, instanceID uint32, action TaskAction) error {
	j := m.getJob(id)
	if j == nil {
		return nil
	}

	j.Lock()
	t := j.tasks[instanceID]
	j.Unlock()
	if t == nil {
		return nil
	}

	t.Lock()
	t.lastAction = action
	t.lastActionTime = time.Now()
	t.Unlock()

	cs := t.CurrentState()
	gs := t.GoalState()
	log.WithField("action", action).
		WithField("current_state", cs.State.String()).
		WithField("current_config", cs.ConfigVersion).
		WithField("goal_state", gs.State.String()).
		WithField("goal_version", gs.ConfigVersion).
		WithField("job_id", t.job.id.GetValue()).
		WithField("instance_id", t.id).
		Info("running action for task")

	var err error
	switch action {
	case NoAction:

	case UntrackAction:
		m.clearTask(t)

	case ReloadRuntime:
		_, err = t.job.m.GetTaskRuntime(ctx, t.job.id, t.id)

	case InitializeAction:
		err = t.initialize(ctx)

	case StopAction:
		err = t.stop(ctx)

	case UseGoalConfigVersionAction:
		err = t.useGoalConfigVersion(ctx)

	default:
		err = fmt.Errorf("no command configured for running task action `%v`", action)
	}

	return err
}

// addJob to the manager, if missing. The manager lock must be hold when called.
func (m *manager) addJob(id *peloton.JobID, state *jobState) *job {
	j, ok := m.jobs[id.GetValue()]
	if !ok {
		j = newJob(id, m, state)
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
	if len(t.job.tasks) == 0 {
		delete(m.jobs, t.job.id.GetValue())
	}
}

// Start syncs jobs and tasks from DB, starts emitting metrics.
func (m *manager) Start() error {
	m.Lock()
	defer m.Unlock()

	if m.running {
		return nil
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
	return nil
}

// Stop clears the current tracked jobs and tasks, stops emitting metrics.
func (m *manager) Stop() error {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return nil
	}
	m.running = false

	for _, job := range m.jobs {
		for _, t := range job.tasks {
			m.taskScheduler.schedule(t, time.Time{})
		}
	}

	m.jobs = map[string]*job{}

	close(m.stopChan)

	log.Info("tracked.Manager stopped")
	return nil
}

func (m *manager) syncFromDB(ctx context.Context) error {
	log.Info("syncing tracked manager with DB goalstates")

	// TODO: Skip completed jobs.
	jobs, err := m.jobStore.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	for id, runtime := range jobs {
		// Job recovery path will read in initialized jobs.
		if runtime.GetState() == pb_job.JobState_INITIALIZED {
			continue
		}

		jobID := &peloton.JobID{Value: id}
		tasks, err := m.taskStore.GetTasksForJob(ctx, jobID)
		if err != nil {
			return err
		}

		for instanceID, task := range tasks {
			m.SetTask(jobID, instanceID, task.GetRuntime())
		}
	}

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
	tCount := map[pb_task.TaskState]map[pb_task.TaskGoalState]float64{}
	for s := range pb_task.TaskState_name {
		tCount[pb_task.TaskState(s)] = map[pb_task.TaskGoalState]float64{}
		for gs := range pb_task.TaskGoalState_name {
			tCount[pb_task.TaskState(s)][pb_task.TaskGoalState(gs)] = 0.0
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
