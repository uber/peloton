// Task state machine

package task

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	sched "mesos/v1/scheduler"
	pb_eventstream "peloton/private/eventstream"
	"peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

var errorWaitInterval = 10 * time.Second

// InitTaskStateManager init the task state manager
func InitTaskStateManager(
	d yarpc.Dispatcher,
	updateBufferSize int,
	updateAckConcurrency int,
	eventDestinationRole string,
	parentScope tally.Scope) {

	var taskUpdateCount int64
	var prevUpdateCount int64
	var taskUpdateAckCount int64
	var prevTaskUpdateAckCount int64
	var taskUpdateDBWrittenCount int64
	var prevTaskUpdateDBWrittenCount int64
	var statusChannelCount int32

	handler := taskStateManager{
		schedulerclient: mpb.NewSchedulerClient(
			d.ClientConfig(common.MesosMasterScheduler),
			mpb.ContentTypeProtobuf,
		),
		updateAckConcurrency: updateAckConcurrency,
		ackChannel:           make(chan *sched.Event_Update, updateBufferSize),
		taskUpdateCount:      &taskUpdateCount,
		prevUpdateCount:      &prevUpdateCount,
		taskAckCount:         &taskUpdateAckCount,
		prevAckCount:         &prevTaskUpdateAckCount,
		dBWrittenCount:       &taskUpdateDBWrittenCount,
		prevDBWrittenCount:   &prevTaskUpdateDBWrittenCount,
		statusChannelCount:   &statusChannelCount,
		scope:                parentScope.SubScope("taskStateManager"),
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, hostmgr_mesos.ServiceName, mpb.Procedure(name, hdl))
	}
	handler.startAsyncProcessTaskUpdates()
	handler.eventStreamHandler = initEventStreamHandler(
		d,
		updateBufferSize,
		handler.scope.SubScope("EventStreamHandler"))
	// initialize the status update event forwarder for resmgr
	initResMgrEventForwarder(d,
		handler.eventStreamHandler,
		eventDestinationRole,
		handler.scope.SubScope("ResourceManagerClient"))
}

// eventForwarder is the struct to forward status update events to
// resource manager. It implements eventstream.EventHandler and it
// forwards the events to remote in the OnEvents function.
type eventForwarder struct {
	// jsonClient to send NotifyTaskUpdatesRequest
	jsonClient json.Client
	// Tracking the progress returned from remote side
	progress *uint64
}

func newEventForwarder(d yarpc.Dispatcher, eventDestinationRole string) eventstream.EventHandler {
	var progress uint64
	return &eventForwarder{
		jsonClient: json.New(d.ClientConfig(eventDestinationRole)),
		progress:   &progress,
	}
}

// OnEvent callback
func (f *eventForwarder) OnEvent(event *pb_eventstream.Event) {
	//Not implemented
}

// OnEvents callback. In this callback, a batch of events are forwarded to
// resource manager.
func (f *eventForwarder) OnEvents(events []*pb_eventstream.Event) {
	if len(events) > 0 {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFunc()
		// Forward events
		request := &resmgrsvc.NotifyTaskUpdatesRequest{
			Events: events,
		}
		var response resmgrsvc.NotifyTaskUpdatesResponse
		for {
			_, err := f.jsonClient.Call(
				ctx,
				yarpc.NewReqMeta().Procedure("ResourceManager.NotifyTaskUpdates"),
				request,
				&response,
			)
			if err == nil {
				break
			} else {
				log.WithError(err).WithField("progress", events[0].Offset).
					Error("Failed to call ResourceManager.NotifyTaskUpdate")
				time.Sleep(errorWaitInterval)
			}
		}
		if response.PurgeOffset > 0 {
			atomic.StoreUint64(f.progress, response.PurgeOffset)
		}
		if response.Error != nil {
			log.WithField("NotifyTaskUpdatesResponse_Error", response.Error).Error("NotifyTaskUpdatesRequest failed")
		}
	}
}

// GetEventProgress returns the event forward progress
func (f *eventForwarder) GetEventProgress() uint64 {
	return atomic.LoadUint64(f.progress)
}

func initResMgrEventForwarder(
	d yarpc.Dispatcher,
	eventStreamHandler *eventstream.Handler,
	eventDestinationRole string,
	scope tally.Scope) {
	eventstream.NewLocalEventStreamClient(
		common.PelotonResourceManager,
		eventStreamHandler,
		newEventForwarder(d, eventDestinationRole),
		scope,
	)
}

func initEventStreamHandler(d yarpc.Dispatcher, bufferSize int, scope tally.Scope) *eventstream.Handler {
	eventStreamHandler := eventstream.NewEventStreamHandler(
		bufferSize,
		[]string{common.PelotonJobManager, common.PelotonResourceManager},
		nil,
		scope,
	)
	json.Register(d, json.Procedure("EventStream.InitStream", eventStreamHandler.InitStream))
	json.Register(d, json.Procedure("EventStream.WaitForEvents", eventStreamHandler.WaitForEvents))
	return eventStreamHandler
}

type taskStateManager struct {
	schedulerclient      mpb.SchedulerClient
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
	scope              tally.Scope
}

// Update is the Mesos callback on mesos state updates
func (m *taskStateManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {
	var err error
	taskUpdate := body.GetUpdate()
	log.WithField("task_update", taskUpdate).
		Debugf("taskManager: Update called")

	event := &pb_eventstream.Event{
		MesosTaskStatus: taskUpdate.GetStatus(),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
	}
	err = m.eventStreamHandler.AddEvent(event)
	if err != nil {
		log.WithError(err).
			WithField("status_update", taskUpdate.GetStatus()).
			Error("Cannot add status update")
	} else {
		m.ackChannel <- taskUpdate
	}
	m.updateCounters()
	// If buffer is full, AddStatusUpdate would fail and peloton would not
	// ack the status update and mesos master would resend the status update.
	// Return nil otherwise the framework would disconnect with the mesos master
	return nil
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
	err := m.schedulerclient.Call(msid, msg)
	if err != nil {
		log.WithField("task_status", *taskUpdate).
			WithError(err).
			Error("Failed to ack task update")
		return err
	}
	log.WithField("task_status", *taskUpdate).Debug("Acked task update")
	return nil
}
