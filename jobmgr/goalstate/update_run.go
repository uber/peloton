package goalstate

import (
	"context"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/task"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// UpdateRun is responsible to check which instances have been updated,
// start the next set of instances to update and update the state
// of the job update in cache and DB.
func UpdateRun(ctx context.Context, entity goalstate.Entity) error {
	var instancesDone []uint32
	var instancesCurrent []uint32

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

	instancesCurrent, instancesDoneFromLastRun, err := cached.GetUpdateProgress(
		ctx,
		cachedJob,
		cachedUpdate.GetGoalState().JobVersion,
		cachedUpdate.GetInstancesCurrent(),
	)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	instancesDone = append(
		cachedUpdate.GetState().Instances,
		instancesDoneFromLastRun...)

	instancesToAdd, instancesToUpdate :=
		getInstancesForUpdateRun(cachedUpdate, instancesCurrent, instancesDone)

	if err := processUpgrade(
		ctx,
		cachedJob,
		cachedUpdate,
		instancesToAdd,
		instancesToUpdate,
		goalStateDriver,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if err := writeUpdateProgress(
		ctx,
		cachedUpdate,
		instancesDone,
		instancesCurrent,
		instancesToAdd,
		instancesToUpdate,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if err := postUpdateAction(
		ctx,
		cachedJob,
		cachedUpdate,
		instancesToUpdate,
		instancesDone,
		goalStateDriver); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	goalStateDriver.mtx.updateMetrics.UpdateRun.Inc(1)
	return nil
}

// postUpdateAction performs actions after update run is finished for
// one run of UpdateRun. Its job:
// 1. Enqueue update if update is completed finished
// 2. Enqueue update if any task updated in this run has already
// been updated
func postUpdateAction(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesUpdatedInCurrentRun []uint32,
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

	// if any of the task updated in this round is a killed task or
	// has already finished update, reenqueue the update, because
	// more instances can be updated without receiving task event.
	for _, instanceID := range instancesUpdatedInCurrentRun {
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
	instancesDone []uint32,
	previousInstancesCurrent []uint32,
	instancesAdded []uint32,
	instancesUpdated []uint32,
) error {
	state := pbupdate.State_ROLLING_FORWARD
	newInstancesCurrent := append(previousInstancesCurrent, instancesAdded...)
	newInstancesCurrent = append(newInstancesCurrent, instancesUpdated...)
	// update the state of the job update
	return cachedUpdate.WriteProgress(
		ctx,
		state,
		instancesDone,
		newInstancesCurrent,
	)
}

func processUpgrade(
	ctx context.Context,
	cachedJob cached.Job,
	cachedUpdate cached.Update,
	instancesToAdd []uint32,
	instancesToUpdate []uint32,
	goalStateDriver *driver) error {
	// no action needed if there is no instances to update/add
	if len(instancesToUpdate)+len(instancesToAdd) == 0 {
		return nil
	}

	jobConfig, err := goalStateDriver.jobStore.GetJobConfigWithVersion(
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

	err = upgradeInstancesInUpdate(
		ctx,
		cachedJob,
		instancesToUpdate,
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
		}, cached.UpdateCacheAndDB)
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

// upgradeInstancesInUpdate upgrade the existing instances in instancesToUpdate
func upgradeInstancesInUpdate(
	ctx context.Context,
	cachedJob cached.Job,
	instancesToUpdate []uint32,
	jobConfig *pbjob.JobConfig,
	goalStateDriver *driver) error {
	if len(instancesToUpdate) == 0 {
		return nil
	}
	runtimes := make(map[uint32]cached.RuntimeDiff)

	for _, instID := range instancesToUpdate {
		// update the new desired configuration version
		runtimeDiff := cached.RuntimeDiff{
			cached.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
		}
		runtimes[instID] = runtimeDiff
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

// getInstancesForUpdateRun returns the instances to upgrade/add in
// the given call of UpdateRun.
func getInstancesForUpdateRun(
	update cached.Update,
	instancesCurrent []uint32,
	instancesDone []uint32,
) (
	instancesToAdd []uint32,
	instancesToUpdate []uint32,
) {

	unprocessedInstancesToAdd, unprocessedInstancesToUpdate :=
		getUnprocessedInstances(update, instancesCurrent, instancesDone)

	// if batch size is 0 or updateConfig is nil, update all of the instances
	if update.GetUpdateConfig().GetBatchSize() == 0 {
		return unprocessedInstancesToAdd, unprocessedInstancesToUpdate
	}

	maxNumOfInstancesToProcess :=
		int(update.GetUpdateConfig().GetBatchSize()) - len(instancesCurrent)
	// if instances being updated are more than batch size, do not upgrade anything
	if maxNumOfInstancesToProcess <= 0 {
		return nil, nil
	}

	// if can process all of the remaining instances
	if maxNumOfInstancesToProcess >
		len(unprocessedInstancesToAdd)+len(unprocessedInstancesToUpdate) {
		return unprocessedInstancesToAdd, unprocessedInstancesToUpdate
	}

	// if can process all of the instances to add,
	// and part of instances to upgrade
	if maxNumOfInstancesToProcess > len(unprocessedInstancesToAdd) {
		return unprocessedInstancesToAdd,
			unprocessedInstancesToUpdate[:maxNumOfInstancesToProcess-len(unprocessedInstancesToAdd)]
	}

	// if can process part of the instances to add
	return unprocessedInstancesToAdd[:maxNumOfInstancesToProcess], nil
}

// getUnprocessedInstances returns all of the
// instances remaining to upgrade/add
func getUnprocessedInstances(
	update cached.Update,
	instancesCurrent []uint32,
	instancesDone []uint32,
) (instancesRemainToAdd []uint32,
	instancesRemainToUpdate []uint32) {
	instancesProcessed := append(instancesCurrent, instancesDone...)
	instancesRemainToAdd = subtractSlice(update.GetInstancesAdded(), instancesProcessed)
	instancesRemainToUpdate = subtractSlice(update.GetInstancesUpdated(), instancesProcessed)
	return instancesRemainToAdd, instancesRemainToUpdate
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
