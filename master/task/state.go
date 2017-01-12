// Task state machine

package task

import (
	"code.uber.internal/infra/peloton/master/config"
	master_mesos "code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"sync/atomic"
)

// InitTaskStateManager init the task state manager
func InitTaskStateManager(
	d yarpc.Dispatcher,
	masterConfig *config.MasterConfig,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	mesosClient mpb.Client) {

	handler := taskStateManager{
		TaskStore: taskStore,
		JobStore:  jobStore,
		client:    mesosClient,
		config:    masterConfig,
	}
	handler.applier = newTaskStateUpdateApplier(&handler, masterConfig.DbWriteConcurrency, 10000)
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, master_mesos.ServiceName, mpb.Procedure(name, hdl))
	}
}

type taskStateManager struct {
	JobStore  storage.JobStore
	TaskStore storage.TaskStore
	client    mpb.Client
	applier   *taskUpdateApplier
	config    *config.MasterConfig
	// TODO: cache of task states

}

// Update is the Mesos callback on mesos state updates
func (m *taskStateManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {
	var err error
	taskUpdate := body.GetUpdate()
	log.WithField("Task update", taskUpdate).Debugf(
		"taskManager: Update called")

	m.applier.addTaskStatus(taskUpdate.GetStatus())
	if taskUpdate.Status.Uuid != nil && len(taskUpdate.Status.Uuid) > 0 {
		err = m.acknowledgeTaskUpdate(taskUpdate)
	} else {
		log.Debugf("Skip status update without uuid")
	}
	return err
}

func (m *taskStateManager) processTaskStatusChange(taskStatus *mesos.TaskStatus) error {
	taskID := taskStatus.GetTaskId().GetValue()
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

// TODO: see if we can do batching(check mesos master code) and make this async
func (m *taskStateManager) acknowledgeTaskUpdate(taskUpdate *sched.Event_Update) error {
	callType := sched.Call_ACKNOWLEDGE
	msg := &sched.Call{
		FrameworkId: master_mesos.GetSchedulerDriver().GetFrameworkID(),
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			AgentId: taskUpdate.Status.AgentId,
			TaskId:  taskUpdate.Status.TaskId,
			Uuid:    taskUpdate.Status.Uuid,
		},
	}
	msid := master_mesos.GetSchedulerDriver().GetMesosStreamID()
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
	taskID := taskStatus.GetTaskId().GetValue()
	_, instanceID, _ := util.ParseTaskID(taskID)
	index := instanceID % len(t.statusBuckets)
	t.statusBuckets[index].statusChannel <- taskStatus
}

func (t *taskUpdateApplier) shutdown() {
	for _, bucket := range t.statusBuckets {
		bucket.shutdown()
	}
}
