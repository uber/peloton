package tracked

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

func (t *task) start(ctx context.Context) error {
	m := t.job.m

	taskID := util.BuildTaskID(t.job.ID(), t.ID())
	taskInfo, err := m.taskStore.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	// Retrieves job config and task info from data stores.
	jobConfig, err := m.jobStore.GetJobConfig(ctx, t.job.id, taskInfo.GetRuntime().GetConfigVersion())
	if err != nil {
		return err
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
				return fmt.Errorf("failed to read volume %v for task %v: %v", volumeID, t.id, err)
			}
			// Volume not exist so enqueue as normal task going through placement.
		} else if pv.GetState() == volume.VolumeState_CREATED {
			// Volume is in CREATED state so launch the task directly to hostmgr.
			if m.taskLauncher == nil {
				return fmt.Errorf("task launcher not available")
			}
			return m.taskLauncher.LaunchTaskWithReservedResource(ctx, taskInfo)
		}
	}

	// TODO: Investigate how to create proper gangs for scheduling (currently, task are treat independently)
	return jobmgr_task.EnqueueGangs(ctx, []*pb_task.TaskInfo{taskInfo}, jobConfig, m.resmgrClient)
}
