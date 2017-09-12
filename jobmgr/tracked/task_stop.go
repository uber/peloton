package tracked

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	log "github.com/sirupsen/logrus"
)

func (t *task) stop(ctx context.Context) error {
	t.Lock()
	runtime := t.runtime
	t.Unlock()

	if runtime == nil {
		return fmt.Errorf("tracked task has no runtime info assigned")
	}

	switch {
	case runtime.GetState() == pb_task.TaskState_INITIALIZED:
		return t.stopInitializedTask(ctx)

	case runtime.GetMesosTaskId() != nil:
		return t.stopMesosTask(ctx, runtime)
	}

	return nil
}

func (t *task) stopInitializedTask(ctx context.Context) error {
	// If initializing, store state as killed and remove from resmgr.
	// TODO: Due to missing atomic updates in DB, there is a race
	// where we accidentially may start off the task, even though we
	// have marked it as KILLED.
	taskID := fmt.Sprintf("%s-%d", t.job.ID().GetValue(), t.ID())
	req := &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: taskID,
			},
		},
	}
	// Calling resmgr Kill API
	res, err := t.job.m.resmgrClient.KillTasks(ctx, req)
	if err != nil {
		return err
	}

	if e := res.GetError(); e != nil {
		// TODO: As of now this function supports one task
		// We need to do it for batch
		if e[0].GetNotFound() != nil {
			log.WithFields(log.Fields{
				"Task":  e[0].GetNotFound().Task.Value,
				"Error": e[0].GetNotFound().Message,
			}).Info("Task not found in resmgr")
		} else {
			return fmt.Errorf("Task %s can not be killed due to %s",
				e[0].GetKillError().Task.Value,
				e[0].GetKillError().Message)
		}
	}

	taskInfo, err := t.job.m.taskStore.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	// If it had changed, update to current and abort.
	if taskInfo.Runtime.State != pb_task.TaskState_INITIALIZED {
		t.job.m.SetTask(t.job.id, t.id, taskInfo.Runtime)
		return nil
	}

	taskInfo.Runtime.State = pb_task.TaskState_KILLED

	return t.job.m.UpdateTask(ctx, t.job.id, t.id, taskInfo)
}

func (t *task) stopMesosTask(ctx context.Context, runtime *pb_task.RuntimeInfo) error {
	// Send kill signal to mesos, if the task has a mesos task ID associated
	// to it.

	req := &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{runtime.GetMesosTaskId()},
	}
	res, err := t.job.m.hostmgrClient.KillTasks(ctx, req)
	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.KillFailure != nil:
			return fmt.Errorf(e.KillFailure.Message)
		case e.InvalidTaskIDs != nil:
			return fmt.Errorf(e.InvalidTaskIDs.Message)
		default:
			return fmt.Errorf(e.String())
		}
	}

	return nil
}
