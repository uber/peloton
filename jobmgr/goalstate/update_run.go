package goalstate

import (
	"context"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/task"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// UpdateRun is responsible to check which instances have been updated,
// start the next set of instances to update and update the state
// of the job update in cache and DB.
func UpdateRun(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	log.WithField("update_id", updateEnt.id.GetValue()).
		Info("update running")

	cachedUpdate, cachedJob, err := fetchUpdateAndJobFromCache(
		ctx, updateEnt.id, goalStateDriver)
	if err != nil || cachedUpdate == nil || cachedJob == nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	instancesCurrent, instancesDoneFromLastRun, instancesFailedFromLastRun, err :=
		cached.GetUpdateProgress(
			ctx,
			cachedJob,
			cachedUpdate,
			cachedUpdate.GetGoalState().JobVersion,
			cachedUpdate.GetInstancesCurrent(),
		)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	instancesFailed := append(
		cachedUpdate.GetInstancesFailed(),
		instancesFailedFromLastRun...)
	instancesDone := append(
		cachedUpdate.GetInstancesDone(),
		instancesDoneFromLastRun...)

	// number of failed instances in the update exceeds limit and
	// max instance retries is set, process the failed update and
	// return directly
	// TODO: use job SLA if GetMaxFailureInstances is not set
	if cachedUpdate.GetUpdateConfig().GetMaxFailureInstances() != 0 &&
		uint32(len(instancesFailed)) >=
			cachedUpdate.GetUpdateConfig().GetMaxFailureInstances() {
		err := processFailedUpdate(
			ctx,
			cachedJob,
			cachedUpdate,
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
			cachedUpdate, instancesCurrent, instancesDone, instancesFailed)

	if err := processUpdate(
		ctx,
		cachedJob,
		cachedUpdate,
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
		cachedUpdate,
		pbupdate.State_ROLLING_FORWARD,
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
		cachedUpdate,
		instancesToUpdate,
		instancesToRemove,
		instancesDone,
		goalStateDriver); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

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
	if err := cachedUpdate.WriteProgress(
		ctx,
		pbupdate.State_FAILED,
		instancesDone,
		instancesFailed,
		instancesCurrent,
	); err != nil {
		return err
	}

	driver.EnqueueUpdate(
		cachedJob.ID(),
		cachedUpdate.ID(),
		time.Now())
	return nil
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
	instancesUpdatedInCurrentRun []uint32,
	instancesRemovedInCurrentRun []uint32,
	instancesDone []uint32,
	goalStateDriver Driver,
) error {
	// update finishes, reenqueue the update
	if len(cachedUpdate.GetGoalState().Instances) == len(instancesDone) {
		goalStateDriver.EnqueueUpdate(
			cachedJob.ID(),
			cachedUpdate.ID(),
			time.Now())
		return nil
	}

	instancesInCurrentRun := append(instancesUpdatedInCurrentRun,
		instancesRemovedInCurrentRun...)

	// if any of the task updated/removed in this round is a killed task or
	// has already finished update/kill, reenqueue the update, because
	// more instances can be updated without receiving task event.
	for _, instanceID := range instancesInCurrentRun {
		cachedTask := cachedJob.GetTask(instanceID)
		if cachedTask == nil {
			continue
		}
		runtime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			return err
		}
		// directly begin the next update because some tasks have already completed update
		// and more update can begin without waiting.
		if isTaskUpdateCompleted(cachedUpdate, runtime) ||
			isTaskKilled(runtime) {
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

func isTaskKilled(runtime *pbtask.RuntimeInfo) bool {
	return runtime.GetGoalState() == pbtask.TaskState_KILLED &&
		runtime.GetState() == pbtask.TaskState_KILLED
}

func writeUpdateProgress(
	ctx context.Context,
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
	return cachedUpdate.WriteProgress(
		ctx,
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

	jobConfig, _, err := goalStateDriver.jobStore.GetJobConfigWithVersion(
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
	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	if runtime.GetGoalState() == pbjob.JobState_KILLED {
		err = cachedJob.Update(ctx, &pbjob.JobInfo{
			Runtime: &pbjob.RuntimeInfo{
				GoalState: goalstateutil.GetDefaultJobGoalState(
					pbjob.JobType_SERVICE)},
		}, nil,
			cached.UpdateCacheAndDB)
		if err != nil {
			return err
		}
	}

	// now lets add the new instances
	for _, instID := range instancesToAdd {
		runtime, err := getTaskRuntimeIfExisted(ctx, cachedJob, instID)
		if err != nil {
			return err
		}

		if runtime != nil && runtime.GetState() == pbtask.TaskState_INITIALIZED {
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
			runtimes[instID] = runtime

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
	}

	// Create the tasks
	if len(runtimes) > 0 {
		if err := cachedJob.CreateTasks(ctx, runtimes, "peloton"); err != nil {
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
	runtime, err := cachedTask.GetRunTime(ctx)
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
			runtimes[instID] = runtimeDiff
		}
	}

	if len(runtimes) > 0 {
		if err := cachedJob.PatchTasks(ctx, runtimes); err != nil {
			return err
		}
	}

	for _, instID := range instancesToUpdate {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instID, time.Now())
	}

	return nil
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
			jobmgrcommon.GoalStateField:            pbtask.TaskState_KILLED,
			jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
			jobmgrcommon.MessageField:              "Task Count reduced via API",
		}
	}

	if len(runtimes) > 0 {
		if err := cachedJob.PatchTasks(ctx, runtimes); err != nil {
			return err
		}
	}

	for _, instID := range instancesToRemove {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instID, time.Now())
	}

	return nil
}

// getInstancesForUpdateRun returns the instances to update/add in
// the given call of UpdateRun.
func getInstancesForUpdateRun(
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
		unprocessedInstancesToUpdate, unprocessedInstancesToRemove := getUnprocessedInstances(
		update, instancesCurrent, instancesDone, instancesFailed)

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

	instancesRemainToAdd = subtractSlice(update.GetInstancesAdded(), instancesProcessed)
	instancesRemainToUpdate = subtractSlice(update.GetInstancesUpdated(), instancesProcessed)
	instancesRemainToRemove = subtractSlice(update.GetInstancesRemoved(), instancesProcessed)
	return
}

// subtractSlice get return the result of slice1 - slice2
// if an element is in slice2 but not in slice1, it would be ignored
func subtractSlice(slice1 []uint32, slice2 []uint32) []uint32 {
	if slice1 == nil {
		return nil
	}

	var result []uint32
	slice2Set := make(map[uint32]bool)

	for _, v := range slice2 {
		slice2Set[v] = true
	}

	for _, v := range slice1 {
		if !slice2Set[v] {
			result = append(result, v)
		}
	}

	return result
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
	podEvents, err := goalStateDriver.taskStore.GetPodEvents(
		ctx,
		jobID,
		instanceID,
		1)
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
