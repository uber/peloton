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
	"github.com/uber/peloton/pkg/common/goalstate"
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

	cachedTask, err := cachedJob.AddTask(ctx, taskEnt.instanceID)
	if err != nil {
		return err
	}

	taskRuntime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskConfigV2Ops.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		taskRuntime.GetConfigVersion())
	if err != nil {
		return err
	}

	return rescheduleTask(
		ctx,
		cachedJob,
		taskEnt.instanceID,
		taskRuntime,
		taskConfig,
		goalStateDriver,
		true)
}
