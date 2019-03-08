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

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"
	updateutil "github.com/uber/peloton/pkg/jobmgr/util/update"

	log "github.com/sirupsen/logrus"
)

// TaskTerminatedRetry retries on task that is terminated
func TaskTerminatedRetry(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	// TODO: use jobFactory.AddJob after GetJob and AddJob get cleaned up
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)
	if cachedJob == nil {
		return nil
	}
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	cachedTask, err := cachedJob.AddTask(ctx, taskEnt.instanceID)
	if err != nil {
		return err
	}

	taskRuntime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskStore.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		taskRuntime.GetConfigVersion())
	if err != nil {
		return err
	}

	shouldRetry, err := shouldTaskRetry(
		ctx,
		cachedJob,
		taskEnt.instanceID,
		jobRuntime,
		taskRuntime,
		goalStateDriver,
	)
	if err != nil {
		return err
	}

	if shouldRetry {
		return rescheduleTask(
			ctx,
			cachedJob,
			cachedTask,
			taskRuntime,
			taskConfig,
			goalStateDriver,
			true)
	}

	return nil
}

// shouldTaskRetry returns whether a terminated task should retry given its
// MaxInstanceAttempts config
func shouldTaskRetry(
	ctx context.Context,
	cachedJob cached.Job,
	instanceID uint32,
	jobRuntime *pbjob.RuntimeInfo,
	taskRuntime *pbtask.RuntimeInfo,
	goalStateDriver *driver,
) (bool, error) {
	// no update, should retry
	if len(jobRuntime.GetUpdateID().GetValue()) == 0 {
		return true, nil
	}

	cachedWorkflow := cachedJob.AddWorkflow(jobRuntime.GetUpdateID())

	// TODO: remove after recovery is done when reading state
	if cachedWorkflow.GetState().State == pbupdate.State_INVALID {
		if err := cachedWorkflow.Recover(ctx); err != nil {
			return false, err
		}
	}

	if cached.IsUpdateStateTerminal(cachedWorkflow.GetState().State) {
		// update is terminal, let goal state engine untrack it
		goalStateDriver.EnqueueUpdate(
			cachedJob.ID(),
			cachedWorkflow.ID(),
			time.Now())
		return true, nil
	}

	if !cachedWorkflow.IsTaskInUpdateProgress(instanceID) &&
		!cachedWorkflow.IsTaskInFailed(instanceID) {
		return true, nil
	}

	if taskutil.IsSystemFailure(taskRuntime) {
		goalStateDriver.mtx.taskMetrics.RetryFailedLaunchTotal.Inc(1)
	}

	// If the current failure retry count has reached the maxAttempts, we give up retry
	if updateutil.HasFailedUpdate(
		taskRuntime,
		cachedWorkflow.GetUpdateConfig().GetMaxInstanceAttempts()) {
		log.
			WithField("jobID", cachedJob.ID().GetValue()).
			WithField("instanceID", instanceID).
			WithField("failureCount", taskRuntime.GetFailureCount()).
			Debug("failureCount larger than max attempts, give up retry")
		return false, nil
	}
	return true, nil
}
