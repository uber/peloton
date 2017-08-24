package tracked

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/storage"
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

	// UpdateTask with the new runtime info, by first attempting to persit it.
	// If it fail in persisting the change due to a data race, an AlreadyExists
	// error is returned.
	// If succesfull, this will also schedule the task for immediate evaluation.
	// TODO: Should only take the task runtime, not info.
	UpdateTask(ctx context.Context, jobID *peloton.JobID, instanceID uint32, info *pb_task.TaskInfo) error

	// ScheduleTask to be evaluated by the goal state engine, at deadline.
	ScheduleTask(t Task, deadline time.Time)

	// WaitForScheduledTask blocked until a scheduled task is ready or the
	// stopChan is closed.
	WaitForScheduledTask(stopChan <-chan struct{}) Task
}

// NewManager returns a new tracked manager.
func NewManager(d *yarpc.Dispatcher, taskStore storage.TaskStore) Manager {
	return &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(),
		taskQueueChanged: make(chan struct{}, 1),
		hostmgrClient:    hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:     resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		taskStore:        taskStore,
	}
}

type manager struct {
	sync.RWMutex

	// jobs maps from from peloton job id -> tracked job.
	jobs map[string]*job

	progress atomic.Uint64

	taskQueue        *deadlineQueue
	taskQueueChanged chan struct{}

	hostmgrClient hostsvc.InternalHostServiceYARPCClient
	resmgrClient  resmgrsvc.ResourceManagerServiceYARPCClient
	taskStore     storage.TaskStore
}

func (m *manager) Start() {}
func (m *manager) Stop()  {}

func (m *manager) addJob(id *peloton.JobID) Job {
	m.Lock()
	defer m.Unlock()

	j, ok := m.jobs[id.GetValue()]
	if !ok {
		j = newJob(id, m)
		m.jobs[id.GetValue()] = j
	}

	return j
}

func (m *manager) GetJob(id *peloton.JobID) Job {
	m.RLock()
	defer m.RUnlock()

	if j, ok := m.jobs[id.GetValue()]; ok {
		return j
	}

	return nil
}

func (m *manager) SetTask(jobID *peloton.JobID, instanceID uint32, runtime *pb_task.RuntimeInfo) {
	j := m.addJob(jobID).(*job)
	t := j.setTask(instanceID, runtime)
	m.ScheduleTask(t, time.Now())
}

func (m *manager) UpdateTask(ctx context.Context, jobID *peloton.JobID, instanceID uint32, info *pb_task.TaskInfo) error {
	if err := m.taskStore.UpdateTask(ctx, info); err != nil {
		return err
	}

	m.SetTask(jobID, instanceID, info.GetRuntime())
	return nil
}

func (m *manager) ScheduleTask(t Task, deadline time.Time) {
	m.Lock()
	defer m.Unlock()

	it := t.(*task)
	it.Lock()
	defer it.Unlock()

	// Override if newer.
	if it.deadline().IsZero() || deadline.Before(it.deadline()) {
		it.setDeadline(deadline)
		m.taskQueue.update(it)
		select {
		case m.taskQueueChanged <- struct{}{}:
		default:
		}
	}
}

func (m *manager) WaitForScheduledTask(stopChan <-chan struct{}) Task {
	for {
		m.RLock()
		deadline := m.taskQueue.nextDeadline()
		m.RUnlock()

		var timer *time.Timer
		var timerChan <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerChan = timer.C
		}

		select {
		case <-timerChan:
			m.Lock()
			r := m.taskQueue.popIfReady()
			m.Unlock()

			if r != nil {
				it := r.(*task)
				// Clear deadline.
				it.Lock()
				it.setDeadline(time.Time{})
				it.Unlock()
				return it
			}

		case <-m.taskQueueChanged:

		case <-stopChan:
			return nil
		}

		if timer != nil {
			timer.Stop()
		}
	}
}
