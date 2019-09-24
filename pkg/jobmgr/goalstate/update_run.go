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
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/task"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// UpdateRun is responsible to check which instances have been updated,
// start the next set of instances to update and update the state
// of the job update in cache and DB.
func UpdateRun(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		log.WithFields(log.Fields{
			"update_id": updateEnt.id.GetValue(),
		}).WithError(err).Info("unable to run update")
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	// TODO: remove after recovery is done when reading state
	if cachedWorkflow.GetState().State == pbupdate.State_INVALID {
		return UpdateReload(ctx, entity)
	}

	instancesCurrent, instancesDoneFromLastRun, instancesFailedFromLastRun, err :=
		cached.GetUpdateProgress(
			ctx,
			cachedJob.ID(),
			cachedWorkflow,
			cachedWorkflow.GetGoalState().JobVersion,
			cachedWorkflow.GetInstancesCurrent(),
			goalStateDriver.taskStore,
		)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	instancesFailed := append(
		cachedWorkflow.GetInstancesFailed(),
		instancesFailedFromLastRun...)
	instancesDone := append(
		cachedWorkflow.GetInstancesDone(),
		instancesDoneFromLastRun...)

	// number of failed instances in the workflow exceeds limit and
	// max instance retries is set, process the failed workflow and
	// return directly
	// TODO: use job SLA if GetMaxFailureInstances is not set
	if cachedWorkflow.GetUpdateConfig().GetMaxFailureInstances() != 0 &&
		uint32(len(instancesFailed)) >=
			cachedWorkflow.GetUpdateConfig().GetMaxFailureInstances() {
		err := processFailedUpdate(
			ctx,
			cachedJob,
			cachedWorkflow,
			instancesDone,
			instancesFailed,
			instancesCurrent,
			goalStateDriver,
		)
		if err != nil {
			goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		}
		return err
	}

	instancesToAdd, instancesToUpdate, instancesToRemove :=
		getInstancesForUpdateRun(
			ctx,
			cachedJob,
			cachedWorkflow,
			instancesCurrent,
			instancesDone,
			instancesFailed,
		)

	instancesToAdd, instancesToUpdate, instancesToRemove, instancesRemovedDone, err :=
		confirmInstancesStatus(
			ctx,
			cachedJob,
			cachedWorkflow,
			instancesToAdd,
			instancesToUpdate,
			instancesToRemove,
		)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}
	instancesDone = append(instancesDone, instancesRemovedDone...)

	if err := processUpdate(
		ctx,
		cachedJob,
		cachedWorkflow,
		instancesToAdd,
		instancesToUpdate,
		instancesToRemove,
		goalStateDriver,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if err := writeUpdateProgress(
		ctx,
		cachedJob,
		cachedWorkflow,
		cachedWorkflow.GetState().State,
		instancesDone,
		instancesFailed,
		instancesCurrent,
		instancesToAdd,
		instancesToUpdate,
		instancesToRemove,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if err := postUpdateAction(
		ctx,
		cachedJob,
		cachedWorkflow,
		instancesToAdd,
		instancesToUpdate,
		instancesToRemove,
		instancesDone,
		instancesFailed,
		goalStateDriver); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	// TODO (varung):
	// - Use len for instances current
	// - Remove instances_added, instances_removed and instances_updated
	log.WithFields(log.Fields{
		"update_id":         updateEnt.id.GetValue(),
		"job_id":            cachedJob.ID().GetValue(),
		"update_type":       cachedWorkflow.GetWorkflowType().String(),
		"instances_current": cachedWorkflow.GetInstancesCurrent(),
		"instances_failed":  len(cachedWorkflow.GetInstancesFailed()),
		"instances_done":    len(cachedWorkflow.GetInstancesDone()),
		"instances_added":   len(cachedWorkflow.GetInstancesAdded()),
		"instances_removed": len(cachedWorkflow.GetInstancesRemoved()),
		"instances_updated": len(cachedWorkflow.GetInstancesUpdated()),
	}).Info("update running")

	goalStateDriver.mtx.updateMetrics.UpdateRun.Inc(1)
	return nil
}

// processFailedUpdate is called when the update fails due to
// too many instances fail during the process. It update the
// state to failed and enqueue it to goal state engine directly.
func processFailedUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32,
	driver *driver,
) error {
	// rollback the update if RollbackOnFailure is set and
	// the update itself is not a rollback
	if cachedUpdate.GetUpdateConfig().RollbackOnFailure &&
		!isUpdateRollback(cachedUpdate) {
		// write the progress first, because when rollback happens,
		// workflow does not know the newly finished/failed instances.
		cachedJob.WriteWorkflowProgress(
			ctx,
			cachedUpdate.ID(),
			cachedUpdate.GetState().State,
			instancesDone,
			instancesFailed,
			instancesCurrent,
		)

		if err := cachedJob.RollbackWorkflow(ctx); err != nil {
			log.WithFields(log.Fields{
				"update_id": cachedUpdate.ID().GetValue(),
				"job_id":    cachedJob.ID().GetValue(),
			}).WithError(err).
				Info("fail to rollback update")
			return err
		}

		cachedConfig, err := cachedJob.GetConfig(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"update_id": cachedUpdate.ID().GetValue(),
				"job_id":    cachedJob.ID().GetValue(),
			}).WithError(err).
				Info("fail to get job config to rollback update")
			return err
		}

		if err := handleUnchangedInstancesInUpdate(
			ctx,
			cachedUpdate,
			cachedJob,
			cachedConfig,
		); err != nil {
			log.WithFields(log.Fields{
				"update_id": cachedUpdate.ID().GetValue(),
				"job_id":    cachedJob.ID().GetValue(),
			}).WithError(err).
				Info("fail to update unchanged instances to rollback update")
			return err
		}

		log.WithFields(log.Fields{
			"update_id": cachedUpdate.ID().GetValue(),
			"job_id":    cachedJob.ID().GetValue(),
		}).Info("update rolling back")
	} else {
		if err := cachedJob.WriteWorkflowProgress(
			ctx,
			cachedUpdate.ID(),
			pbupdate.State_FAILED,
			instancesDone,
			instancesFailed,
			instancesCurrent,
		); err != nil {
			return err
		}
	}
	driver.EnqueueUpdate(cachedJob.ID(), cachedUpdate.ID(), time.Now())

	return nil
}

// isUpdateRollback returns if an update is a rolling back to a
// previous version
func isUpdateRollback(cachedUpdate cached.Update) bool {
	if cachedUpdate.GetWorkflowType() != models.WorkflowType_UPDATE {
		return false
	}

	return cachedUpdate.GetState().State == pbupdate.State_ROLLING_BACKWARD
}

// postUpdateAction performs actions after update run is finished for
// one run of UpdateRun. Its job:
// 1. Enqueue update if update is completed finished
// 2. Enqueue update if any task updated/removed in this run has already
// been updated/killed
func postUpdateAction(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesAddedInCurrentRun []uint32,
	instancesUpdatedInCurrentRun []uint32,
	instancesRemovedInCurrentRun []uint32,
	instancesDone []uint32,
	instancesFailed []uint32,
	goalStateDriver Driver,
) error {
	// update finishes, reenqueue the update
	if len(cachedUpdate.GetGoalState().Instances) == len(instancesDone)+len(instancesFailed) {
		goalStateDriver.EnqueueUpdate(
			cachedJob.ID(),
			cachedUpdate.ID(),
			time.Now())
		return nil
	}

	instancesInCurrentRun :=
		append(instancesAddedInCurrentRun,
			append(instancesUpdatedInCurrentRun, instancesRemovedInCurrentRun...)...)

	// if any of the task updated/removed in this round is a killed task or
	// has already finished update/kill, reenqueue the update, because
	// more instances can be updated without receiving task event.
	for _, instanceID := range instancesInCurrentRun {
		cachedTask := cachedJob.GetTask(instanceID)
		if cachedTask == nil {
			continue
		}
		runtime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			return err
		}
		// directly begin the next update because some tasks have already completed update
		// and more update can begin without waiting.
		if isTaskUpdateCompleted(cachedUpdate, runtime) ||
			isTaskTerminated(runtime) {
			goalStateDriver.EnqueueUpdate(
				cachedJob.ID(), cachedUpdate.ID(), time.Now())
			return nil
		}
	}

	return nil
}

// A special case is that UpdateRun is retried multiple times. And
// the task updated in the run have already finished update.
// As a result, no more task event would be received, so JobMgr
// needs to deal with this case separately.
func isTaskUpdateCompleted(cachedUpdate cached.Update, runtime *pbtask.RuntimeInfo) bool {
	return runtime.GetState() == pbtask.TaskState_RUNNING &&
		runtime.GetConfigVersion() == runtime.GetDesiredConfigVersion() &&
		runtime.GetConfigVersion() == cachedUpdate.GetGoalState().JobVersion
}

// isTaskTerminated returns whether a task is terminated and would
// not be started again
func isTaskTerminated(runtime *pbtask.RuntimeInfo) bool {
	return util.IsPelotonStateTerminal(runtime.GetState()) &&
		util.IsPelotonStateTerminal(runtime.GetGoalState())
}

func writeUpdateProgress(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	updateState pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	previousInstancesCurrent []uint32,
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32,
) error {
	newInstancesCurrent := append(previousInstancesCurrent, instancesAdded...)
	newInstancesCurrent = append(newInstancesCurrent, instancesUpdated...)
	newInstancesCurrent = append(newInstancesCurrent, instancesRemoved...)
	// update the state of the job update
	return cachedJob.WriteWorkflowProgress(
		ctx,
		cachedUpdate.ID(),
		updateState,
		instancesDone,
		instancesFailed,
		newInstancesCurrent,
	)
}

func processUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesToAdd []uint32,
	instancesToUpdate []uint32,
	instancesToRemove []uint32,
	goalStateDriver *driver) error {
	// no action needed if there is no instances to update/add
	if len(instancesToUpdate)+len(instancesToAdd)+len(instancesToRemove) == 0 {
		return nil
	}

	jobConfig, _, err := goalStateDriver.jobConfigOps.Get(
		ctx,
		cachedJob.ID(),
		cachedUpdate.GetGoalState().JobVersion)
	if err != nil {
		return err
	}

	err = addInstancesInUpdate(
		ctx,
		cachedJob,
		instancesToAdd,
		jobConfig,
		goalStateDriver)
	if err != nil {
		return err
	}

	err = processInstancesInUpdate(
		ctx,
		cachedJob,
		cachedUpdate,
		instancesToUpdate,
		jobConfig,
		goalStateDriver,
	)
	if err != nil {
		return err
	}

	err = removeInstancesInUpdate(
		ctx,
		cachedJob,
		instancesToRemove,
		jobConfig,
		goalStateDriver,
	)
	return err
}

// addInstancesInUpdate will add instances specified in instancesToAdd
// in cachedJob.
// It would create and send the new tasks to resmgr. And if the job
// is set to KILLED goal state, the function would reset the goal state
// to the default goal state.
func addInstancesInUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	instancesToAdd []uint32,
	jobConfig *pbjob.JobConfig,
	goalStateDriver *driver) error {
	var tasks []*pbtask.TaskInfo
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)

	if len(instancesToAdd) == 0 {
		return nil
	}

	// move job goal state from KILLED to RUNNING
	jobRuntime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	// now lets add the new instances
	for _, instID := range instancesToAdd {
		runtime, err := getTaskRuntimeIfExisted(ctx, cachedJob, instID)
		if err != nil {
			return err
		}

		if runtime != nil {
			if runtime.GetState() == pbtask.TaskState_INITIALIZED {
				// runtime is initialized, do not create the task again and directly
				// send to ResMgr
				taskInfo := &pbtask.TaskInfo{
					JobId:      cachedJob.ID(),
					InstanceId: instID,
					Runtime:    runtime,
					Config: taskconfig.Merge(
						jobConfig.GetDefaultConfig(),
						jobConfig.GetInstanceConfig()[instID]),
				}
				tasks = append(tasks, taskInfo)
			} else {
				log.WithFields(log.Fields{
					"job_id":      cachedJob.ID().GetValue(),
					"instance_id": instID,
					"state":       runtime.GetState().String(),
				}).Info(
					"task added in update has non-nil runtime in uninitialized state")
			}
		} else {
			// runtime is nil, initialize the runtime
			runtime := task.CreateInitializingTask(
				cachedJob.ID(), instID, jobConfig)

			if err = updateWithRecentRunID(
				ctx,
				cachedJob.ID(),
				instID,
				runtime,
				goalStateDriver); err != nil {
				return err
			}

			runtime.ConfigVersion = jobConfig.GetChangeLog().GetVersion()
			runtime.DesiredConfigVersion =
				jobConfig.GetChangeLog().GetVersion()
			// job goal state is KILLED, set task cur and desired state to KILLED to
			// avoid unnecessary task creation
			if jobRuntime.GetGoalState() == pbjob.JobState_KILLED {
				runtime.State = pbtask.TaskState_KILLED
				runtime.GoalState = pbtask.TaskState_KILLED
			}
			// do not send to resmgr if task goal state is KILLED
			if runtime.GetGoalState() != pbtask.TaskState_KILLED {
				taskInfo := &pbtask.TaskInfo{
					JobId:      cachedJob.ID(),
					InstanceId: instID,
					Runtime:    runtime,
					Config: taskconfig.Merge(
						jobConfig.GetDefaultConfig(),
						jobConfig.GetInstanceConfig()[instID]),
				}
				tasks = append(tasks, taskInfo)
			}
			runtimes[instID] = runtime
		}
	}

	// Create the tasks
	if len(runtimes) > 0 {
		if err := cachedJob.CreateTaskRuntimes(ctx, runtimes, "peloton"); err != nil {
			return err
		}
	}

	// send to resource manager
	return sendTasksToResMgr(
		ctx, cachedJob.ID(), tasks, jobConfig, goalStateDriver)
}

// getTaskRuntimeIfExisted returns task runtime if the task is created.
// it would return nil RuntimeInfo and nil error if the task runtime does
// not exist
func getTaskRuntimeIfExisted(
	ctx context.Context,
	cachedJob cached.Job,
	instanceID uint32,
) (*pbtask.RuntimeInfo, error) {
	cachedTask := cachedJob.GetTask(instanceID)
	if cachedTask == nil {
		return nil, nil
	}
	runtime, err := cachedTask.GetRuntime(ctx)
	if yarpcerrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

// processInstancesInUpdate update the existing instances in instancesToUpdate
func processInstancesInUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesToUpdate []uint32,
	jobConfig *pbjob.JobConfig,
	goalStateDriver *driver) error {
	if len(instancesToUpdate) == 0 {
		return nil
	}
	runtimes := make(map[uint32]jobmgrcommon.RuntimeDiff)

	for _, instID := range instancesToUpdate {
		runtimeDiff := cachedUpdate.GetRuntimeDiff(jobConfig)
		if runtimeDiff != nil {
			cachedTask, err := cachedJob.AddTask(ctx, instID)
			if err != nil {
				return err
			}

			runtime, err := cachedTask.GetRuntime(ctx)
			if err != nil {
				return err
			}

			if cachedUpdate.GetUpdateConfig().GetInPlace() {
				runtimeDiff[jobmgrcommon.DesiredHostField] = getDesiredHostField(runtime)
			} else {
				runtimeDiff[jobmgrcommon.DesiredHostField] = ""
			}

			if runtime.GetGoalState() == pbtask.TaskState_DELETED ||
				cachedUpdate.GetUpdateConfig().GetStartTasks() {
				runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_RUNNING
			}

			runtimes[instID] = runtimeDiff
		}
	}

	if len(runtimes) > 0 {
		// we do not need to handle `instancesToBeRetried` here. Since all
		// instances in `instancesToUpdate` are being enqueued into Task
		// goalstate engine, their runtimes will be reloaded into cache when
		// they are evaluated. The update will be retried in the next update cycle.
		if _, _, err := cachedJob.PatchTasks(ctx, runtimes, false); err != nil {
			return err
		}
	}

	for _, instID := range instancesToUpdate {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instID, time.Now())
	}

	return nil
}

func getDesiredHostField(runtime *pbtask.RuntimeInfo) string {
	// desired host field is reset when the task runs again.
	// if host field is not reset when being updated, it means
	// either the task was in LAUNCHED/STARTING state or there
	// is an update overwrite when the task was killed by the prev
	// update. In either case, we should just reuse the previous
	// desired host field
	if len(runtime.GetDesiredHost()) != 0 {
		return runtime.GetDesiredHost()
	}

	// host field is set when the task is launched,
	// it is reset when the task is killed. For all the
	// states in between, the task maybe running on the host
	// already. Therefore, set the current host as desired host.
	if !util.IsPelotonStateTerminal(runtime.GetState()) {
		return runtime.GetHost()
	}

	return ""
}

// removeInstancesInUpdate kills the instances being removed in the update
func removeInstancesInUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	instancesToRemove []uint32,
	jobConfig *pbjob.JobConfig,
	goalStateDriver *driver) error {
	if len(instancesToRemove) == 0 {
		return nil
	}
	runtimes := make(map[uint32]jobmgrcommon.RuntimeDiff)

	for _, instID := range instancesToRemove {
		runtimes[instID] = jobmgrcommon.RuntimeDiff{
			jobmgrcommon.GoalStateField:            pbtask.TaskState_DELETED,
			jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
			jobmgrcommon.MessageField:              "Task Count reduced via API",
			jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
				Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
			},
			jobmgrcommon.FailureCountField: uint32(0),
		}
	}

	if len(runtimes) > 0 {
		// we do not need to handle `instancesToBeRetried` here. Since the
		// instances are being enqueued into Task goalstate engine, their runtimes
		// will be reloaded into cache when they are evaluated. The update will
		// be retried in the next update cycle.
		if _, _, err := cachedJob.PatchTasks(ctx, runtimes, false); err != nil {
			return err
		}
	}

	for _, instID := range instancesToRemove {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instID, time.Now())
	}

	return nil
}

func confirmInstancesStatus(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesToAdd []uint32,
	instancesToUpdate []uint32,
	instancesToRemove []uint32,
) (
	newInstancesToAdd []uint32,
	newInstancesToUpdate []uint32,
	newInstancesToRemove []uint32,
	instancesDone []uint32,
	err error,
) {
	for _, instID := range instancesToAdd {
		var cachedTask cached.Task
		var runtime *pbtask.RuntimeInfo

		cachedTask, err = cachedJob.AddTask(ctx, instID)
		if err == nil {
			runtime, err = cachedTask.GetRuntime(ctx)
			if err != nil {
				if yarpcerrors.IsNotFound(err) {
					// runtime does not exist, lets try to add it
					newInstancesToAdd = append(newInstancesToAdd, instID)
					continue
				}
				// got some error, just retry later
				return
			}

			// instance already exists
			if runtime.GetConfigVersion() == cachedUpdate.GetGoalState().JobVersion {
				// instance exists with correct configuration version
				newInstancesToAdd = append(newInstancesToAdd, instID)
			} else {
				// instance exists with previous configuration version,
				// hence needs to be updated
				newInstancesToUpdate = append(newInstancesToUpdate, instID)
			}
			continue
		}

		if yarpcerrors.IsNotFound(err) ||
			err == cached.InstanceIDExceedsInstanceCountError {
			// instance does not exist
			newInstancesToAdd = append(newInstancesToAdd, instID)
			continue
		}

		// got some error, just retry later
		return
	}

	for _, instID := range instancesToUpdate {
		var cachedTask cached.Task

		cachedTask, err = cachedJob.AddTask(ctx, instID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				// not found, add it
				newInstancesToAdd = append(newInstancesToAdd, instID)
				continue
			}
			// got some error, just retry later
			return
		}

		_, err = cachedTask.GetRuntime(ctx)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				// not found, add it
				newInstancesToAdd = append(newInstancesToAdd, instID)
				continue
			}
			// got some error, just retry later
			return
		}
		newInstancesToUpdate = append(newInstancesToUpdate, instID)
	}

	for _, instID := range instancesToRemove {
		_, err = cachedJob.AddTask(ctx, instID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) ||
				err == cached.InstanceIDExceedsInstanceCountError {
				// not found, already removed
				instancesDone = append(instancesDone, instID)
				continue
			}
			return
		}
		// remove it
		newInstancesToRemove = append(newInstancesToRemove, instID)
	}

	// clear the error and return
	err = nil
	return
}

// getInstancesForUpdateRun returns the instances to update/add in
// the given call of UpdateRun.
func getInstancesForUpdateRun(
	ctx context.Context,
	cachedJob cached.Job,
	update cached.Update,
	instancesCurrent []uint32,
	instancesDone []uint32,
	instancesFailed []uint32,
) (
	instancesToAdd []uint32,
	instancesToUpdate []uint32,
	instancesToRemove []uint32,
) {

	unprocessedInstancesToAdd,
		unprocessedInstancesToUpdate,
		unprocessedInstancesToRemove := getUnprocessedInstances(
		update,
		instancesCurrent,
		instancesDone,
		instancesFailed,
	)

	if len(unprocessedInstancesToUpdate) != 0 {
		unprocessedInstancesToUpdate = sortInstancesByAvailability(
			ctx,
			cachedJob,
			unprocessedInstancesToUpdate,
		)
	}

	// if batch size is 0 or updateConfig is nil, update all of the instances
	if update.GetUpdateConfig().GetBatchSize() == 0 {
		return unprocessedInstancesToAdd,
			unprocessedInstancesToUpdate,
			unprocessedInstancesToRemove
	}

	maxNumOfInstancesToProcess :=
		int(update.GetUpdateConfig().GetBatchSize()) - len(instancesCurrent)
	// if instances being updated are more than batch size, do not update anything
	if maxNumOfInstancesToProcess <= 0 {
		return nil, nil, nil
	}

	// if can process all of the remaining instances
	if maxNumOfInstancesToProcess >
		len(unprocessedInstancesToAdd)+len(unprocessedInstancesToUpdate)+
			len(unprocessedInstancesToRemove) {
		return unprocessedInstancesToAdd,
			unprocessedInstancesToUpdate,
			unprocessedInstancesToRemove
	}

	// if can process all of the instances to add, update
	// and part of instances to remove
	if maxNumOfInstancesToProcess >
		len(unprocessedInstancesToAdd)+len(unprocessedInstancesToUpdate) {
		return unprocessedInstancesToAdd,
			unprocessedInstancesToUpdate,
			unprocessedInstancesToRemove[:maxNumOfInstancesToProcess-
				len(unprocessedInstancesToAdd)-
				len(unprocessedInstancesToUpdate)]

	}

	// if can process all of the instances to add,
	// and part of instances to update
	if maxNumOfInstancesToProcess > len(unprocessedInstancesToAdd) {
		return unprocessedInstancesToAdd,
			unprocessedInstancesToUpdate[:maxNumOfInstancesToProcess-len(unprocessedInstancesToAdd)],
			nil
	}

	// if can process part of the instances to add
	return unprocessedInstancesToAdd[:maxNumOfInstancesToProcess], nil, nil
}

// sortInstancesByAvailability sorts the instances of the job by its availability.
// The sort order is
// 1. unavailable-instances
// 2. killed-instances
// 3. invalid-instances
// 4. available-instances
// This is needed because we need to try to update unhealthy instances
// before healthy ones in order to keep the number of unavailable instances to
// a minimum thereby giving the update workflow the best chance to progress.
func sortInstancesByAvailability(
	ctx context.Context,
	cachedJob cached.Job,
	instances []uint32,
) []uint32 {
	instancesByAvailability := make(map[jobmgrcommon.InstanceAvailability_Type][]uint32)

	instanceAvailabilityByInstance := cachedJob.GetInstanceAvailabilityType(ctx, instances...)
	for _, i := range instances {
		availabilityType := instanceAvailabilityByInstance[i]
		instancesByAvailability[availabilityType] = append(
			instancesByAvailability[availabilityType],
			i,
		)
	}

	var sortedInstances []uint32
	sortedInstances = append(
		sortedInstances,
		instancesByAvailability[jobmgrcommon.InstanceAvailability_UNAVAILABLE]...,
	)
	sortedInstances = append(
		sortedInstances,
		instancesByAvailability[jobmgrcommon.InstanceAvailability_KILLED]...,
	)
	sortedInstances = append(
		sortedInstances,
		instancesByAvailability[jobmgrcommon.InstanceAvailability_INVALID]...,
	)
	sortedInstances = append(
		sortedInstances,
		instancesByAvailability[jobmgrcommon.InstanceAvailability_AVAILABLE]...,
	)

	return sortedInstances
}

// getUnprocessedInstances returns all of the
// instances remaining to update/add
func getUnprocessedInstances(
	update cached.Update,
	instancesCurrent []uint32,
	instancesDone []uint32,
	instancesFailed []uint32,
) (instancesRemainToAdd []uint32,
	instancesRemainToUpdate []uint32,
	instancesRemainToRemove []uint32) {
	instancesProcessed := append(instancesCurrent, instancesDone...)
	instancesProcessed = append(instancesProcessed, instancesFailed...)

	instancesRemainToAdd = util.SubtractSlice(update.GetInstancesAdded(), instancesProcessed)
	instancesRemainToUpdate = util.SubtractSlice(update.GetInstancesUpdated(), instancesProcessed)
	instancesRemainToRemove = util.SubtractSlice(update.GetInstancesRemoved(), instancesProcessed)
	return
}

// updateWithRecentRunID has primary use case to sync runID from persistent storage
// for previously removed instance that is added back again.
//
// 1. Fetches most recent pod event to get last runID
// 2. If RunID exists for this instance, then update the runtime with
//	  last RunID. Primary reason to not start RunID for newly added instance
// 	  is to prevent overwriting previous pod events at storage.
// 3. Starting from most recent RunID enables user to fetch sandbox logs,
//    state transitions for previous instance runs.
func updateWithRecentRunID(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *pbtask.RuntimeInfo,
	goalStateDriver *driver) error {
	podEvents, err := goalStateDriver.podEventsOps.GetAll(
		ctx,
		jobID.GetValue(),
		instanceID)
	if err != nil {
		return err
	}

	// instance removed previously during update is being added back.
	if len(podEvents) > 0 {
		runID, err := util.ParseRunID(podEvents[0].GetTaskId().GetValue())
		if err != nil {
			return err
		}
		runtime.MesosTaskId = util.CreateMesosTaskID(
			jobID,
			instanceID,
			runID+1)
		runtime.DesiredMesosTaskId = runtime.MesosTaskId
		runtime.PrevMesosTaskId = podEvents[0].GetTaskId()
	}
	return nil
}
