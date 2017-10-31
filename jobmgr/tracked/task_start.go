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
	runtime, err := t.getRuntime()
	if err != nil {
		return err
	}

	m := t.job.m
	// Fetch corresponding task config.
	config, err := m.taskStore.GetTaskConfig(ctx, t.job.id, t.id, runtime.GetConfigVersion())
	if err != nil {
		return err
	}

	hasVolume := config.GetVolume() != nil && len(runtime.GetVolumeID().GetValue()) > 0
	if hasVolume {
		volumeID := &peloton.VolumeID{
			Value: runtime.GetVolumeID().GetValue(),
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
			return m.taskLauncher.LaunchTaskWithReservedResource(ctx, &pb_task.TaskInfo{
				JobId:      t.job.id,
				InstanceId: t.id,
				Runtime:    runtime,
				Config:     config,
			})
		}
	}

	jobConfig, err := m.jobStore.GetJobConfig(ctx, t.job.id, runtime.GetConfigVersion())
	if err != nil {
		return fmt.Errorf("job config not found for %v", t.job.id)
	}

	return jobmgr_task.EnqueueGangs(ctx, []*pb_task.TaskInfo{{
		JobId:      t.job.id,
		InstanceId: t.id,
		Runtime:    runtime,
		Config:     config,
	}}, jobConfig.GetRespoolID(), jobConfig.GetSla(), m.resmgrClient)
}
