package tracked

import (
	"context"
	"fmt"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// startTask starts (or queue for start) a task.
func (j *job) tryStartTask(ctx context.Context, instanceID uint32) error {
	j.RLock()
	gangStarted := hasStarted(j.state.state)
	sla := j.state.sla
	cv := j.state.configVersion
	j.RUnlock()

	// Retrieve job sla if needed.
	if !gangStarted && sla == nil {
		jobConfig, err := j.m.jobStore.GetJobConfig(ctx, j.id, cv)
		if err != nil {
			return fmt.Errorf("job config not found for %v", j.id.Value)
		}

		j.Lock()
		if j.state.sla == nil {
			if jobConfig.Sla != nil {
				j.state.sla = jobConfig.Sla
			} else {
				j.state.sla = &pb_job.SlaConfig{}
			}
		}
		sla = j.state.sla
		j.Unlock()
	}

	// Consider the gang started if the gang constraint is noop.
	if sla.MinimumRunningInstances <= 1 {
		gangStarted = true
	}

	if !gangStarted {
		// Retrieve job status.
		// TODO: remove when the status is maintained by the job GSE
		jr, err := j.m.jobStore.GetJobRuntime(ctx, j.id)
		if err != nil {
			return fmt.Errorf("job runtime not found for %v", j.id.Value)
		}

		j.Lock()
		j.state.configVersion = uint64(jr.GetConfigVersion())
		j.state.state = jr.GetState()
		gangStarted = hasStarted(j.state.state)
		j.Unlock()
	}

	// If any task of the job minimum scheduling unit has already started,
	// perform start action.
	if gangStarted {
		return j.tasks[instanceID].start(ctx)
	}

	// The gang is not started, and the task is not member of the gang,
	// reschedule.
	if instanceID >= sla.MinimumRunningInstances {
		return nil
	}

	gangReady := true
	j.RLock()
	for i := uint32(0); i < sla.MinimumRunningInstances; i++ {
		t := j.tasks[i]
		gangReady = gangReady && t != nil && t.lastAction == StartAction
	}
	j.RUnlock()

	// Not all tasks of the gang are willing to start, reschedule.
	if !gangReady {
		return nil
	}

	// all tasks of the scheduling gang are ready to start, enqueue the gang
	// TODO: call only once, as even though idempotent, this will be triggered
	// MinimumSchedulingUnitSize times by the tasks tracker
	return j.startGang(ctx, &pb_task.InstanceRange{
		From: 0,
		To:   sla.MinimumRunningInstances,
	})
}

func hasStarted(state pb_job.JobState) bool {
	switch state {
	case pb_job.JobState_UNKNOWN, pb_job.JobState_INITIALIZED, pb_job.JobState_PENDING:
		return false
	default:
		return true
	}
}
