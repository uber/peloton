// Task state machine

package task

import (
	sched "mesos/v1/scheduler"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// InitTaskStateManager init the task state manager
func InitTaskStateManager(
	d yarpc.Dispatcher,
	updateBufferSize int,
	updateAckConcurrency int,
	dbWriteConcurrency int,
	jobStore storage.JobStore,
	taskStore storage.TaskStore) {

	var taskUpdateCount int64
	var prevUpdateCount int64
	var taskUpdateAckCount int64
	var prevTaskUpdateAckCount int64
	var taskUpdateDBWrittenCount int64
	var prevTaskUpdateDBWrittenCount int64
	var statusChannelCount int32

	handler := taskStateManager{
		TaskStore:            taskStore,
		JobStore:             jobStore,
		client:               mpb.New(d.ClientConfig("mesos-master"), "x-protobuf"),
		updateAckConcurrency: updateAckConcurrency,
		ackChannel:           make(chan *sched.Event_Update, updateBufferSize),
		taskUpdateCount:      &taskUpdateCount,
		prevUpdateCount:      &prevUpdateCount,
		taskAckCount:         &taskUpdateAckCount,
		prevAckCount:         &prevTaskUpdateAckCount,
		dBWrittenCount:       &taskUpdateDBWrittenCount,
		prevDBWrittenCount:   &prevTaskUpdateDBWrittenCount,
		statusChannelCount:   &statusChannelCount,
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, hostmgr_mesos.ServiceName, mpb.Procedure(name, hdl))
	}
	handler.startAsyncProcessTaskUpdates()
	// TODO: move eventStreamHandler buffer size into config
	handler.eventStreamHandler = initEventStreamHandler(d, 10000)
}

func initEventStreamHandler(d yarpc.Dispatcher, bufferSize int) *eventstream.Handler {
	// TODO: add remgr as another client
	eventStreamHandler := eventstream.NewEventStreamHandler(bufferSize, []string{common.PelotonJobManager}, nil)
	json.Register(d, json.Procedure("EventStream.InitStream", eventStreamHandler.InitStream))
	json.Register(d, json.Procedure("EventStream.WaitForEvents", eventStreamHandler.WaitForEvents))
	return eventStreamHandler
}

type taskStateManager struct {
	JobStore             storage.JobStore
	TaskStore            storage.TaskStore
	client               mpb.Client
	updateAckConcurrency int
	// Buffers the status updates to ack
	ackChannel chan *sched.Event_Update

	taskUpdateCount *int64
	prevUpdateCount *int64
	taskAckCount    *int64
	prevAckCount    *int64
	// TODO: move DB written counts into jobmgr
	dBWrittenCount     *int64
	prevDBWrittenCount *int64
	statusChannelCount *int32

	lastPrintTime      time.Time
	eventStreamHandler *eventstream.Handler
}

// Update is the Mesos callback on mesos state updates
func (m *taskStateManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {
	var err error
	taskUpdate := body.GetUpdate()
	log.WithField("task_update", taskUpdate).
		Debugf("taskManager: Update called")

	err = m.eventStreamHandler.AddStatusUpdate(taskUpdate.GetStatus())
	if err != nil {
		log.WithError(err).
			WithField("status_update", taskUpdate.GetStatus()).
			Error("Cannot add status update")
	} else {
		m.ackChannel <- taskUpdate
	}
	m.updateCounters()
	return err
}

func (m *taskStateManager) updateCounters() {
	atomic.AddInt32(m.statusChannelCount, 1)
	atomic.AddInt64(m.taskUpdateCount, 1)
	if time.Since(m.lastPrintTime).Seconds() > 1.0 {
		m.lastPrintTime = time.Now()
		updateCount := atomic.LoadInt64(m.taskUpdateCount)
		prevCount := atomic.LoadInt64(m.prevUpdateCount)
		log.WithFields(log.Fields{
			"TaskUpdateCount": updateCount,
			"delta":           updateCount - prevCount,
		}).Info("Task updates received")
		atomic.StoreInt64(m.prevUpdateCount, updateCount)

		ackCount := atomic.LoadInt64(m.taskAckCount)
		prevAckCount := atomic.LoadInt64(m.prevAckCount)
		log.WithFields(log.Fields{
			"TaskAckCount": ackCount,
			"delta":        ackCount - prevAckCount,
		}).Info("Task updates acked")
		atomic.StoreInt64(m.prevAckCount, ackCount)

		writtenCount := atomic.LoadInt64(m.dBWrittenCount)
		prevWrittenCount := atomic.LoadInt64(m.prevDBWrittenCount)
		log.WithFields(log.Fields{
			"TaskWrittenCount": writtenCount,
			"delta":            writtenCount - prevWrittenCount,
		}).Info("Task db persisted")
		atomic.StoreInt64(m.prevDBWrittenCount, writtenCount)

		log.WithField("TaskUpdateChannelCount", atomic.LoadInt32(m.statusChannelCount)).Info("TaskUpdate channel size")
	}
}

func (m *taskStateManager) startAsyncProcessTaskUpdates() {
	for i := 0; i < m.updateAckConcurrency; i++ {
		go func() {
			for {
				select {
				case taskUpdate := <-m.ackChannel:
					if len(taskUpdate.GetStatus().GetUuid()) == 0 {
						log.WithField("status_update", taskUpdate).Debug("Skipping acknowledging update with empty uuid")
					} else {
						err := m.acknowledgeTaskUpdate(taskUpdate)
						if err != nil {
							log.WithField("task_status", *taskUpdate).
								WithError(err).
								Error("Failed to acknowledgeTaskUpdate")
						}
					}
				}
			}
		}()
	}
}

func (m *taskStateManager) acknowledgeTaskUpdate(taskUpdate *sched.Event_Update) error {
	atomic.AddInt64(m.taskAckCount, 1)
	atomic.AddInt32(m.statusChannelCount, -1)
	callType := sched.Call_ACKNOWLEDGE
	msg := &sched.Call{
		FrameworkId: hostmgr_mesos.GetSchedulerDriver().GetFrameworkID(),
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			AgentId: taskUpdate.Status.AgentId,
			TaskId:  taskUpdate.Status.TaskId,
			Uuid:    taskUpdate.Status.Uuid,
		},
	}
	msid := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID()
	err := m.client.Call(msid, msg)
	if err != nil {
		log.WithField("task_status", *taskUpdate).
			WithError(err).
			Error("Failed to ack task update")
		return err
	}
	log.WithField("task_status", *taskUpdate).Debug("Acked task update")
	return nil
}
