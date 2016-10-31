// Task state machine

package task

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	master_mesos "code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"go.uber.org/yarpc"
	"mesos/v1/scheduler"
	sched "mesos/v1/scheduler"
)

// InitTaskUpdateManager init the task state manager
func InitTaskStateManager(
	d yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore) {

	handler := taskStateManager{
		TaskStore: taskStore,
		JobStore:  jobStore,
		client:    mjson.New(d.Channel("mesos-master")),
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mjson.Register(d, mesos.ServiceName, mjson.Procedure(name, hdl))
	}
}

type taskStateManager struct {
	JobStore  storage.JobStore
	TaskStore storage.TaskStore
	client    mjson.Client
}

// Update is the Mesos callback on mesos state updates
func (m *taskStateManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	taskUpdate := body.GetUpdate()
	log.WithField("Task update", taskUpdate).Debugf(
		"taskManager: Update called")

	taskId := taskUpdate.GetStatus().GetTaskId().GetValue()
	taskInfo, err := m.TaskStore.GetTaskById(taskId)
	if err != nil {
		log.Errorf("Fail to find taskInfo for taskId %v, err=%v",
			taskId, err)
		return err
	}
	state := util.MesosStateToPelotonState(taskUpdate.GetStatus().GetState())

	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	taskInfo.GetRuntime().State = state
	err = m.TaskStore.UpdateTask(taskInfo)
	if err != nil {
		log.Errorf("Fail to update taskInfo for taskId %v, new state %v, err=%v",
			taskId, state, err)
		return err
	}
	if taskUpdate.Status.Uuid != nil && len(taskUpdate.Status.Uuid) > 0 {
		err = m.acknowledgeTaskUpdate(taskUpdate)
	} else {
		log.Debugf("Skip status update without uuid")
	}
	return err
}

// TODO: see if we can do batching(check mesos master code) and make this async
func (m *taskStateManager) acknowledgeTaskUpdate(taskUpdate *mesos_v1_scheduler.Event_Update) error {
	callType := sched.Call_ACKNOWLEDGE
	msg := &sched.Call{
		FrameworkId: master_mesos.GetSchedulerDriver().GetFrameworkId(),
		Type:        &callType,
		Acknowledge: &sched.Call_Acknowledge{
			AgentId: taskUpdate.Status.AgentId,
			TaskId:  taskUpdate.Status.TaskId,
			Uuid:    taskUpdate.Status.Uuid,
		},
	}
	msid := master_mesos.GetSchedulerDriver().GetMesosStreamId()
	err := m.client.Call(msid, msg)
	if err != nil {
		log.Errorf("Failed to ack task update %v, err=%v", *taskUpdate, err)
		return err
	}
	log.Debugf("ack task update %v", *taskUpdate)
	return nil
}
