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

func (j *job) startGang(ctx context.Context, instanceRange *pb_task.InstanceRange) error {
	m := j.m
	taskInfos, err := m.taskStore.GetTasksForJobByRange(ctx, j.id, instanceRange)
	if err != nil || taskInfos == nil {
		return fmt.Errorf("task infos not found for %v, range: %v", j.id.GetValue(), instanceRange)
	}

	for _, ti := range taskInfos {
		hasVolume := ti.GetConfig().GetVolume() != nil && len(ti.GetRuntime().GetVolumeID().GetValue()) > 0
		if hasVolume {
			volumeID := &peloton.VolumeID{
				Value: ti.GetRuntime().GetVolumeID().GetValue(),
			}
			pv, err := m.volumeStore.GetPersistentVolume(ctx, volumeID)
			if err != nil {
				_, ok := err.(*storage.VolumeNotFoundError)
				if !ok {
					return fmt.Errorf("failed to read volume %v for task %v: %v", volumeID, ti.InstanceId, err)
				}
				// Volume not exist so enqueue as normal task going through placement.
			} else if pv.GetState() == volume.VolumeState_CREATED {
				// Volume is in CREATED state so launch the task directly to hostmgr.
				if m.taskLauncher == nil {
					return fmt.Errorf("task launcher not available")
				}

				return m.taskLauncher.LaunchTaskWithReservedResource(ctx, ti)
			}
		}
	}

	tis := make([]*pb_task.TaskInfo, instanceRange.To-instanceRange.From)
	for i := instanceRange.From; i < instanceRange.To; i++ {
		tis[i-instanceRange.From] = taskInfos[i]
	}

	jobConfig, err := m.jobStore.GetJobConfig(ctx, j.id, j.state.configVersion)
	if err != nil {
		return fmt.Errorf("job config not found for %v", j.id)
	}

	return jobmgr_task.EnqueueGangs(ctx, tis, jobConfig.GetRespoolID(), jobConfig.GetSla(), m.resmgrClient)
}
