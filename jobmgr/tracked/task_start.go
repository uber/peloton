package tracked

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
)

func (t *task) start(ctx context.Context) error {
	m := t.job.m

	// Retrieves job config and task info from data stores.
	jobConfig, err := m.jobStore.GetJobConfig(ctx, t.job.id)
	if err != nil {
		return fmt.Errorf("job config not found for %v", t.job.id)
	}

	taskID := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", t.job.ID().GetValue(), t.ID()),
	}
	taskInfo, err := m.taskStore.GetTaskByID(ctx, taskID.GetValue())
	if err != nil || taskInfo == nil {
		return fmt.Errorf("task info not found for %v", taskID.GetValue())
	}

	stateful := taskInfo.GetConfig().GetVolume() != nil && len(taskInfo.GetRuntime().GetVolumeID().GetValue()) > 0

	if stateful {
		volumeID := &peloton.VolumeID{
			Value: taskInfo.GetRuntime().GetVolumeID().GetValue(),
		}
		pv, err := m.volumeStore.GetPersistentVolume(ctx, volumeID)
		if err != nil {
			_, ok := err.(*storage.VolumeNotFoundError)
			if !ok {
				return fmt.Errorf("failed to read volume %v for task %v", volumeID, t.id)
			}
			// Volume not exist so enqueue as normal task going through placement.
		} else if pv.GetState() == volume.VolumeState_CREATED {
			// Volume is in CREATED state so launch the task directly to hostmgr.
			if m.taskLauncher == nil {
				return fmt.Errorf("task launcher not available")
			}
			err = m.taskLauncher.LaunchTaskWithReservedResource(ctx, taskInfo)
			if err != nil {
				return err
			}

			// Update task state to PENDING
			runtime := taskInfo.GetRuntime()
			if runtime.GetState() != pb_task.TaskState_PENDING {
				runtime.State = pb_task.TaskState_PENDING
				runtime.Message = "Task sent for placement"
				err = m.taskStore.UpdateTaskRuntime(ctx, t.job.ID(), t.ID(), runtime)
			}
			return err
		}
	}

	// TODO: Investigate how to create proper gangs for scheduling (currently, task are treat independently)
	err = jobmgr_task.EnqueueGangs(ctx, []*pb_task.TaskInfo{taskInfo}, jobConfig, m.resmgrClient)
	if err != nil {
		return err
	}
	// Update task state to PENDING
	runtime := taskInfo.GetRuntime()
	if runtime.GetState() != pb_task.TaskState_PENDING {
		runtime.State = pb_task.TaskState_PENDING
		runtime.Message = "Task sent for placement"
		err = m.taskStore.UpdateTaskRuntime(ctx, t.job.ID(), t.ID(), runtime)
	}
	return err
}
