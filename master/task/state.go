// Task state machine

package task

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	master_mesos "code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"go.uber.org/yarpc"
	"mesos/v1/scheduler"
	sched "mesos/v1/scheduler"
)

// InitTaskStateManager init the task state manager
func InitTaskStateManager(
	d yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	mesosClient mpb.Client) {

	handler := taskStateManager{
		TaskStore: taskStore,
		JobStore:  jobStore,
		client:    mesosClient,
	}
	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mpb.Register(d, mesos.ServiceName, mpb.Procedure(name, hdl))
	}
}

type taskStateManager struct {
	JobStore  storage.JobStore
	TaskStore storage.TaskStore
	client    mpb.Client
}

// Update is the Mesos callback on mesos state updates
func (m *taskStateManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {

	taskUpdate := body.GetUpdate()
	log.WithField("Task update", taskUpdate).Debugf(
		"taskManager: Update called")

	taskID := taskUpdate.GetStatus().GetTaskId().GetValue()
	taskInfo, err := m.TaskStore.GetTaskByID(taskID)
	if err != nil {
		log.Errorf("Fail to find taskInfo for taskID %v, err=%v",
			taskID, err)
		return err
	}
	state := util.MesosStateToPelotonState(taskUpdate.GetStatus().GetState())

	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	taskInfo.GetRuntime().State = state
	err = m.TaskStore.UpdateTask(taskInfo)
	if err != nil {
		log.Errorf("Fail to update taskInfo for taskID %v, new state %v, err=%v",
			taskID, state, err)
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
