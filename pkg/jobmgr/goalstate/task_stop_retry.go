// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"
	"time"

	"github.com/uber/peloton/pkg/common/goalstate"

	log "github.com/sirupsen/logrus"
)

// TaskExecutorShutdown is called when killing task timeout, it would shutdown
// the executor directly
func TaskExecutorShutdown(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	cachedTask := cachedJob.GetTask(taskEnt.instanceID)

	runtime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	// It is possible that jobmgr crashes or leader election changes when the task waiting on timeout
	// Need to reenqueue the task after jobmgr recovers.
	if time.Now().Sub(time.Unix(0, int64(runtime.GetRevision().GetUpdatedAt()))) < _defaultShutdownExecutorTimeout {
		goalStateDriver.EnqueueTask(cachedTask.JobID(), cachedTask.ID(), time.Now().Add(_defaultShutdownExecutorTimeout))
		return nil
	}

	goalStateDriver.mtx.taskMetrics.ExecutorShutdown.Inc(1)
	log.WithField("job_id", taskEnt.jobID).
		WithField("instance_id", taskEnt.instanceID).
		Info("task kill timed out, try to shutdown executor")

	return goalStateDriver.lm.ShutdownExecutor(
		ctx,
		runtime.GetMesosTaskId().GetValue(),
		runtime.GetAgentID().GetValue(),
		goalStateDriver.executorShutShutdownRateLimiter,
	)
}
