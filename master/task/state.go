// Task state machine

package task

import (
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
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
	handler.applier = newTaskStateUpdateApplier(&handler, dbWriteConcurrency, 10000)
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
	applier              *taskUpdateApplier
	updateAckConcurrency int
	// Buffers the status updates to ack
	ackChannel chan *sched.Event_Update

	taskUpdateCount    *int64
	prevUpdateCount    *int64
	taskAckCount       *int64
	prevAckCount       *int64
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
	log.WithField("Task update", taskUpdate).Debugf(
		"taskManager: Update called")

	m.applier.addTaskStatus(taskUpdate.GetStatus())

	err = m.eventStreamHandler.AddStatusUpdate(taskUpdate.GetStatus())
	if err != nil {
		log.WithError(err).WithField("statusupdate", taskUpdate.GetStatus()).Error("Cannot add status update")
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
		log.WithField("TaskUpdateCount", updateCount).WithField("delta", updateCount-prevCount).Info("Task updates received")
		atomic.StoreInt64(m.prevUpdateCount, updateCount)

		ackCount := atomic.LoadInt64(m.taskAckCount)
		prevAckCount := atomic.LoadInt64(m.prevAckCount)
		log.WithField("TaskAckCount", ackCount).WithField("delta", ackCount-prevAckCount).Info("Task updates acked")
		atomic.StoreInt64(m.prevAckCount, ackCount)

		writtenCount := atomic.LoadInt64(m.dBWrittenCount)
		prevWrittenCount := atomic.LoadInt64(m.prevDBWrittenCount)
		log.WithField("TaskWrittenCount", writtenCount).WithField("delta", writtenCount-prevWrittenCount).Info("Task db persisted")
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
							log.Errorf("Failed to acknowledgeTaskUpdate %v, err=%v", taskUpdate, err)
						}
					}
				}
			}
		}()
	}
}

func (m *taskStateManager) processTaskStatusChange(taskStatus *mesos.TaskStatus) error {
	atomic.AddInt64(m.dBWrittenCount, 1)
	mesosTaskID := taskStatus.GetTaskId().GetValue()
	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		log.Errorf("Fail to parse taskID for mesostaskID %v, err=%v", mesosTaskID, err)
		return err
	}
	taskInfo, err := m.TaskStore.GetTaskByID(taskID)
	if err != nil {
		log.Errorf("Fail to find taskInfo for taskID %v, err=%v",
			taskID, err)
		return err
	}
	state := util.MesosStateToPelotonState(taskStatus.GetState())

	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	taskInfo.GetRuntime().State = state
	err = m.TaskStore.UpdateTask(taskInfo)
	if err != nil {
		log.Errorf("Fail to update taskInfo for taskID %v, new state %v, err=%v",
			taskID, state, err)
		return err
	}
	return nil
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
		log.Errorf("Failed to ack task update %v, err=%v", *taskUpdate, err)
		return err
	}
	log.Debugf("ack task update %v", *taskUpdate)
	return nil
}

// taskUpdateApplier maps taskUpdate to a list of buckets; and each bucket would be consumed by a single go routine
// in which the task updates are processed. This would allow quick response to mesos for those status updates; while
// for each individual task, the events are processed in order
type taskUpdateApplier struct {
	statusBuckets []*taskUpdateBucket
}

// taskUpdateBucket is a bucket of task updates. All updates for one task would end up in one bucket in order; a bucket
// can hold status updates for multiple tasks.
type taskUpdateBucket struct {
	statusChannel   chan *mesos.TaskStatus
	shutdownChannel chan struct{}
	index           int
	processedCount  *int32
}

func newTaskUpdateBucket(size int, index int) *taskUpdateBucket {
	updates := make(chan *mesos.TaskStatus, size)
	var count int32
	return &taskUpdateBucket{
		statusChannel:   updates,
		shutdownChannel: make(chan struct{}, 10),
		index:           index,
		processedCount:  &count,
	}
}

func (t *taskUpdateBucket) shutdown() {
	log.Infof("Shutting down bucket %v", t.index)
	t.shutdownChannel <- struct{}{}
}

func (t *taskUpdateBucket) getProcessedCount() int32 {
	return atomic.LoadInt32(t.processedCount)
}

func newTaskStateUpdateApplier(t *taskStateManager, bucketNum int, chanSize int) *taskUpdateApplier {
	var buckets []*taskUpdateBucket
	// TODO: make me use batch task updates instead of individual updates
	for i := 0; i < bucketNum; i++ {
		bucket := newTaskUpdateBucket(chanSize, i)
		buckets = append(buckets, bucket)
		go func() {
			for {
				select {
				case taskStatus := <-bucket.statusChannel:
					err := t.processTaskStatusChange(taskStatus)
					if err != nil {
						log.Errorf("Error applying taskSatus, err=%v status=%v, bucket=%v", err, taskStatus, bucket.index)
					}
					atomic.AddInt32(bucket.processedCount, 1)
				case <-bucket.shutdownChannel:
					log.Infof("bucket %v is shutdown", bucket.index)
					return
				}
			}
		}()
	}
	return &taskUpdateApplier{
		statusBuckets: buckets,
	}
}

func (t *taskUpdateApplier) addTaskStatus(taskStatus *mesos.TaskStatus) {
	mesosTaskID := taskStatus.GetTaskId().GetValue()
	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		log.Errorf("Failed to ParseTaskIDFromMesosTaskID, mesosTaskID=%v, err=%v", mesosTaskID, err)
		return
	}
	_, instanceID, _ := util.ParseTaskID(taskID)
	index := instanceID % len(t.statusBuckets)
	t.statusBuckets[index].statusChannel <- taskStatus
}

func (t *taskUpdateApplier) shutdown() {
	for _, bucket := range t.statusBuckets {
		bucket.shutdown()
	}
}
