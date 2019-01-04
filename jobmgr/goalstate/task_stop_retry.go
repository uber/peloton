package goalstate

import (
	"context"
	"time"

	"github.com/uber/peloton/common/goalstate"
	jobmgrtask "github.com/uber/peloton/jobmgr/task"

	log "github.com/sirupsen/logrus"
)

// TaskExecutorShutdown is called when killing task timeout, it would shutdown
// the executor directly
func TaskExecutorShutdown(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)

	runtime, err := cachedTask.GetRunTime(ctx)
	if err != nil {
		return err
	}

	// It is possible that jobmgr crashes or leader election changes when the task waiting on timeout
	// Need to reenqueue the task after jobmgr recovers.
	if time.Now().Sub(cachedTask.GetLastRuntimeUpdateTime()) < _defaultShutdownExecutorTimeout {
		goalStateDriver.EnqueueTask(cachedTask.JobID(), cachedTask.ID(), time.Now().Add(_defaultShutdownExecutorTimeout))
		return nil
	}

	goalStateDriver.mtx.taskMetrics.ExecutorShutdown.Inc(1)
	log.WithField("job_id", taskEnt.jobID).
		WithField("instance_id", taskEnt.instanceID).
		Info("task kill timed out, try to shutdown executor")

	return jobmgrtask.ShutdownMesosExecutor(ctx, goalStateDriver.hostmgrClient, runtime.GetMesosTaskId(), runtime.GetAgentID())
}
