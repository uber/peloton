package tracked

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
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

	runtime, err := t.job.m.taskStore.GetTaskRuntime(ctx, t.Job().ID(), t.ID())
	if err != nil {
		return err
	}

	// If it had changed, update to current and abort.
	if runtime.State != pb_task.TaskState_INITIALIZED {
		runtimes := make(map[uint32]*pb_task.RuntimeInfo)
		runtimes[t.id] = runtime
		t.job.m.SetTasks(t.job.id, runtimes, UpdateAndSchedule)
		return nil
	}

	runtime.State = pb_task.TaskState_KILLED
	runtime.Message = "Non-running task killed"
	runtime.Reason = ""

	return t.job.m.UpdateTaskRuntime(ctx, t.job.id, t.id, runtime, UpdateAndSchedule)
}

func (t *task) stopMesosTask(ctx context.Context, runtime *pb_task.RuntimeInfo) error {
	// Send kill signal to mesos, if the task has a mesos task ID associated
	// to it.
	return jobmgr_task.KillTask(ctx, t.job.m.hostmgrClient, runtime.GetMesosTaskId())
}

func (t *task) shutdownMesosExecutor(ctx context.Context, runtime *pb_task.RuntimeInfo) error {
	// Send shutdown signal to mesos, if the task has a mesos executor ID and agent ID associated
	// to it.

	return jobmgr_task.ShutdownMesosExecutor(ctx,
		t.job.m.hostmgrClient, runtime.GetMesosTaskId(),
		runtime.GetAgentID())
}
