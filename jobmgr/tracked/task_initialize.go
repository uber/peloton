package tracked

import (
	"context"
	"fmt"

	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/util"
)

// Initialize action does the following:
// 1. Sets the current state to TaskState_INITIALIZED
// 2. Sets the goal state depending on the JobType
// 3. Regenerates a new mesos task ID
func (t *task) initialize(ctx context.Context) error {
	runtime, err := t.getRuntime()
	if err != nil {
		return err
	}

	m := t.job.m

	// Retrieves job config and task info from data stores.
	jobConfig, err := m.jobStore.GetJobConfig(ctx, t.job.id)
	if err != nil {
		return fmt.Errorf("job config not found for %v", t.job.id)
	}

	// Shallow copy of the runtime.
	runtime.State = pb_task.TaskState_INITIALIZED
	runtime.GoalState = jobmgr_task.GetDefaultGoalState(jobConfig.GetType())
	util.RegenerateMesosTaskID(t.job.id, t.id, runtime)
	return m.UpdateTaskRuntime(ctx, t.job.id, t.id, runtime)
}
