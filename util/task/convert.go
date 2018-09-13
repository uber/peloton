package task

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/util"
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
		if resmgrtask.MinInstances > 1 && !resmgrtask.GetRevocable() {
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

	resmgrTask := &resmgr.Task{
		Id:           taskID,
		JobId:        taskInfo.GetJobId(),
		TaskId:       taskInfo.GetRuntime().GetMesosTaskId(),
		Name:         taskInfo.GetConfig().GetName(),
		Preemptible:  slaConfig.GetPreemptible(),
		Priority:     slaConfig.GetPriority(),
		MinInstances: minInstances,
		Resource:     taskInfo.GetConfig().GetResource(),
		Constraint:   taskInfo.GetConfig().GetConstraint(),
		NumPorts:     uint32(numPorts),
		Type:         getTaskType(taskInfo.GetConfig(), jobConfig.GetType()),
		Labels:       util.ConvertLabels(taskInfo.GetConfig().GetLabels()),
		Controller:   taskInfo.GetConfig().GetController(),
		Revocable:    taskInfo.GetConfig().GetRevocable(),
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
