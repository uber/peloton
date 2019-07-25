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
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"

	log "github.com/sirupsen/logrus"
)

// TaskInitialize does the following:
// 1. Sets the current state to TaskState_INITIALIZED
// 2. Sets the goal state depending on the JobType
// 3. Regenerates a new mesos task ID
func TaskInitialize(ctx context.Context, entity goalstate.Entity) error {
	taskEnt := entity.(*taskEntity)
	goalStateDriver := taskEnt.driver
	cachedJob := goalStateDriver.jobFactory.GetJob(taskEnt.jobID)

	if cachedJob == nil {
		return nil
	}

	cachedTask := cachedJob.GetTask(taskEnt.instanceID)
	if cachedTask == nil {
		log.WithFields(log.Fields{
			"job_id":      taskEnt.jobID.GetValue(),
			"instance_id": taskEnt.instanceID,
		}).Error("task is nil in cache with valid job")
		return nil
	}
	runtime, err := cachedTask.GetRuntime(ctx)
	if err != nil {
		return err
	}

	taskConfig, _, err := goalStateDriver.taskConfigV2Ops.GetTaskConfig(
		ctx,
		taskEnt.jobID,
		taskEnt.instanceID,
		runtime.GetDesiredConfigVersion())
	if err != nil {
		return err
	}

	healthState := taskutil.GetInitialHealthState(taskConfig)
	runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
		taskEnt.jobID, taskEnt.instanceID, runtime, healthState)

	// update task runtime
	runtimeDiff[jobmgrcommon.MessageField] = "Initialize task"

	// If the task is being updated, then move the configuration version to
	// the desired configuration version.
	if runtime.GetConfigVersion() != runtime.GetDesiredConfigVersion() {
		// TBD should the failure count be cleaned up as well?
		runtimeDiff[jobmgrcommon.ConfigVersionField] =
			runtime.GetDesiredConfigVersion()
	}

	// we do not need to handle `instancesToBeRetried` here since the task
	// is being requeued to the goalstate. Goalstate will reload the task
	// runtime when the task is evaluated the next time
	_, _, err = cachedJob.PatchTasks(ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{taskEnt.instanceID: runtimeDiff},
		false,
	)
	if err == nil {
		goalStateDriver.EnqueueTask(taskEnt.jobID, taskEnt.instanceID, time.Now())
		EnqueueJobWithDefaultDelay(taskEnt.jobID, goalStateDriver, cachedJob)
	}
	return err
}
