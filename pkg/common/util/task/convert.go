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

package task

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
)

// ConvertToResMgrGangs converts the taskinfo for the tasks comprising
// the config job to resmgr tasks and organizes them into gangs, each
// of which is a set of 1+ tasks to be admitted and placed as a group.
func ConvertToResMgrGangs(
	tasks []*task.TaskInfo,
	jobConfig jobmgrcommon.JobConfig) []*resmgrsvc.Gang {
	var gangs []*resmgrsvc.Gang

	// Gangs of multiple tasks are placed at the front of the returned list for
	// preferential treatment, since they are expected to be both more important
	// and harder to place than gangs comprising a single task.
	var multiTaskGangs []*resmgrsvc.Gang

	for _, t := range tasks {
		resmgrtask := ConvertTaskToResMgrTask(t, jobConfig)
		// Currently a job has at most 1 gang comprising multiple tasks;
		// those tasks have their MinInstances field set > 1.
		if resmgrtask.MinInstances > 1 &&
			!resmgrtask.GetRevocable() &&
			jobConfig.GetType() != job.JobType_SERVICE {
			if len(multiTaskGangs) == 0 {
				var multiTaskGang resmgrsvc.Gang
				multiTaskGangs = append(multiTaskGangs, &multiTaskGang)
			}
			multiTaskGangs[0].Tasks = append(multiTaskGangs[0].Tasks, resmgrtask)
		} else {
			// Gang comprising one task
			var gang resmgrsvc.Gang
			gang.Tasks = append(gang.Tasks, resmgrtask)
			gangs = append(gangs, &gang)
		}
	}
	if len(multiTaskGangs) > 0 {
		gangs = append(multiTaskGangs, gangs...)
	}
	return gangs
}

// ConvertTaskToResMgrTask converts taskinfo to resmgr task.
func ConvertTaskToResMgrTask(
	taskInfo *task.TaskInfo,
	jobConfig jobmgrcommon.JobConfig) *resmgr.Task {
	instanceID := taskInfo.GetInstanceId()
	taskID := &peloton.TaskID{
		Value: fmt.Sprintf(
			"%s-%d",
			taskInfo.GetJobId().GetValue(),
			instanceID),
	}

	slaConfig := jobConfig.GetSLA()
	// If minInstances > 1, instances w/instanceID between 0..minInstances-1 should be gang-scheduled;
	// only pass MinInstances value > 1 for those tasks.
	minInstances := slaConfig.GetMinimumRunningInstances()
	if (minInstances <= 1) || (instanceID >= minInstances) {
		minInstances = 1
	}

	numPorts := 0
	for _, portConfig := range taskInfo.GetConfig().GetPorts() {
		if portConfig.GetValue() == 0 {
			// Dynamic port.
			numPorts++
		}
	}

	// checking if preemption policy is overridden at task level
	var preemptible bool
	switch taskInfo.GetConfig().GetPreemptionPolicy().GetType() {
	case task.PreemptionPolicy_TYPE_PREEMPTIBLE:
		preemptible = true
	case task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE:
		preemptible = false
	default:
		// default to the policy at job level
		preemptible = slaConfig.GetPreemptible()
	}

	resmgrTask := &resmgr.Task{
		Id:                taskID,
		JobId:             taskInfo.GetJobId(),
		TaskId:            taskInfo.GetRuntime().GetMesosTaskId(),
		Name:              taskInfo.GetConfig().GetName(),
		Preemptible:       preemptible,
		Priority:          slaConfig.GetPriority(),
		MinInstances:      minInstances,
		Resource:          taskInfo.GetConfig().GetResource(),
		Constraint:        taskInfo.GetConfig().GetConstraint(),
		NumPorts:          uint32(numPorts),
		Type:              getTaskType(taskInfo.GetConfig(), jobConfig.GetType()),
		Labels:            util.ConvertLabels(taskInfo.GetConfig().GetLabels()),
		Controller:        taskInfo.GetConfig().GetController(),
		Revocable:         taskInfo.GetConfig().GetRevocable(),
		DesiredHost:       taskInfo.GetRuntime().GetDesiredHost(),
		PlacementStrategy: jobConfig.GetPlacementStrategy(),
	}

	taskState := taskInfo.GetRuntime().GetState()
	// Typically, hostname field of resmgr task is set once it is in PLACED.
	// So hostname field is set while the task is in PLACED, LAUNCHING,
	// LAUNCHED, STARTING and RUNNING. However, since hostname is persisted
	// in Runtime on LAUNCHED, we need to set hostname only if task is in
	// LAUNCHED, STARTING or RUNNING states.
	if taskState == task.TaskState_LAUNCHED ||
		taskState == task.TaskState_STARTING ||
		taskState == task.TaskState_RUNNING {
		resmgrTask.Hostname = taskInfo.GetRuntime().GetHost()
	}

	return resmgrTask
}

// returns the task type
func getTaskType(cfg *task.TaskConfig, jobType job.JobType) resmgr.TaskType {
	if cfg.GetVolume() != nil {
		return resmgr.TaskType_STATEFUL
	}

	if jobType == job.JobType_SERVICE {
		return resmgr.TaskType_STATELESS
	}
	// By default task type is batch.
	return resmgr.TaskType_BATCH
}
