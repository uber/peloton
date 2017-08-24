package tracked

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// Manager for tracking jobs and tasks. The manager has built in scheduler,
// for marking tasks as dirty and ready for being processed by the goal state
// engine.
type Manager interface {
	eventstream.EventHandler

	// Noop functions to implement the event.Listener (this should only be a
	// eventstream.EventHandler).
	Start()
	Stop()

	// AddJob will create the job in the manger if it doesn't already exist, and
	// return the job.
	AddJob(id *peloton.JobID) Job

	// GetJob will return the current tracked Job, nil if currently not tracked.
	GetJob(id *peloton.JobID) Job

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

func (m *manager) AddJob(id *peloton.JobID) Job {
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

// OnEvent callback.
func (m *manager) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback.
func (m *manager) OnEvents(events []*pb_eventstream.Event) {
	for _, event := range events {
		m.processEvent(event)
		// TODO: We want to retry the event later on, if processing failed.
		m.progress.Store(event.Offset)
	}
}

func (m *manager) processEvent(event *pb_eventstream.Event) {
	if event.GetType() != pb_eventstream.Event_MESOS_TASK_STATUS {
		log.WithField("type", event.GetType()).
			Warn("unhandled event type in goalstate engine")
		return
	}

	mesosTaskID := event.GetMesosTaskStatus().GetTaskId().GetValue()
	jobID, instanceID, err := util.ParseJobAndInstanceID(mesosTaskID)
	if err != nil {
		log.WithError(err).
			WithField("mesos_task_id", mesosTaskID).
			Error("Failed to ParseTaskIDFromMesosTaskID")
		return
	}

	j := m.AddJob(&peloton.JobID{Value: jobID})

	// TODO: Only read runtime.
	taskInfo, err := m.taskStore.GetTaskByID(context.Background(), fmt.Sprintf("%s-%d", jobID, instanceID))
	if err != nil {
		log.WithError(err).
			WithField("mesos_task_id", mesosTaskID).
			Error("failed reading task info when updating with mesos event")
		return
	}

	newState := util.MesosStateToPelotonState(event.GetMesosTaskStatus().GetState())
	if newState == taskInfo.Runtime.State {
		log.WithField("mesos_task_id", mesosTaskID).Debug("skip status update for mesos task with unchagned state")
		return
	}
	taskInfo.Runtime.State = newState

	// TODO: Use UpdateTask when TaskUpdater doesn't update the task.
	j.SetTask(uint32(instanceID), taskInfo.Runtime)
}

// GetEventProgress returns the progress.
func (m *manager) GetEventProgress() uint64 {
	return m.progress.Load()
}
