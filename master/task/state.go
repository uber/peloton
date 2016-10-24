// Task state machine

package task

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"go.uber.org/yarpc"
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
	return nil
}
