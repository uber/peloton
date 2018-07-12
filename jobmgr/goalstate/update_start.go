package goalstate

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/task"
	goalstateutil "code.uber.internal/infra/peloton/jobmgr/util/goalstate"

	log "github.com/sirupsen/logrus"
)

// fetchUpdateAndJobFromCache is a helper function to fetch the
// update and the job from the cache for a given update entity.
func fetchUpdateAndJobFromCache(
	ctx context.Context,
	updateID *peloton.UpdateID,
	goalStateDriver *driver) (
	cachedUpdate cached.Update, cachedJob cached.Job, err error) {
	// first fetch the update
	cachedUpdate = goalStateDriver.updateFactory.GetUpdate(updateID)
	if cachedUpdate == nil {
		return
	}

	// now lets fetch the job
	jobID := cachedUpdate.JobID()
	cachedJob = goalStateDriver.jobFactory.GetJob(jobID)
	if cachedJob == nil {
		// if job has been untracked, cancel the update and then enqueue into
		// goal state to untrack it.
		log.WithFields(
			log.Fields{
				"job_id":    jobID.GetValue(),
				"update_id": updateID.GetValue(),
			}).Info("job has been deleted, so canceling the update as well")
		err = cachedUpdate.Cancel(ctx)
		if err == nil {
			goalStateDriver.EnqueueUpdate(updateID, time.Now())
		}
	}
	return
}

// addInstancesInUpdate will add new instances in the update
func addInstancesInUpdate(
	ctx context.Context,
	cachedUpdate cached.Update,
	cachedJob cached.Job,
	jobConfig *job.JobConfig,
	goalStateDriver *driver) error {
	var tasks []*pbtask.TaskInfo

	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	jobID := cachedUpdate.JobID()

	// first find if there are any new instances to add
	instancesAdded := cachedUpdate.GetInstancesAdded()
	if len(instancesAdded) == 0 {
		return nil
	}

	// move job goal state from KILLED to RUNNING
	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	if runtime.GetGoalState() == job.JobState_KILLED {
		err = cachedJob.Update(ctx, &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				GoalState: goalstateutil.GetDefaultJobGoalState(job.JobType_SERVICE)},
		}, cached.UpdateCacheAndDB)
		if err != nil {
			return err
		}
	}

	// now lets add the new instances
	for _, instID := range instancesAdded {
		// make sure that instance is not already been created
		t := cachedJob.GetTask(instID)
		if t != nil {
			r, err := t.GetRunTime(ctx)
			if err == nil && r != nil {
				// task has already been created, if not already sent to
				// resource manager, then add to be sent to
				// resource manager
				if r.GetState() == pbtask.TaskState_INITIALIZED {
					taskInfo := &pbtask.TaskInfo{
						JobId:      jobID,
						InstanceId: instID,
						Runtime:    r,
						Config: taskconfig.Merge(
							jobConfig.GetDefaultConfig(),
							jobConfig.GetInstanceConfig()[instID]),
					}
					tasks = append(tasks, taskInfo)
				}
				continue
			}
		}

		// initialize the runtime
		runtime := task.CreateInitializingTask(jobID, instID, jobConfig)
		runtime.ConfigVersion = jobConfig.GetChangeLog().GetVersion()
		runtime.DesiredConfigVersion = jobConfig.GetChangeLog().GetVersion()
		runtimes[instID] = runtime

		taskInfo := &pbtask.TaskInfo{
			JobId:      jobID,
			InstanceId: instID,
			Runtime:    runtime,
			Config: taskconfig.Merge(
				jobConfig.GetDefaultConfig(),
				jobConfig.GetInstanceConfig()[instID]),
		}
		tasks = append(tasks, taskInfo)
	}

	// Create the tasks
	if len(runtimes) > 0 {
		if err := cachedJob.CreateTasks(ctx, runtimes, "peloton"); err != nil {
			return err
		}
	}

	// send to resource manager
	return sendTasksToResMgr(ctx, jobID, tasks, jobConfig, goalStateDriver)
}

// handleUnchangedInstancesInUpdate updates the runtime state of the
// instances left unchanged with the given update; essentially, the
// configuration and desired configuration version of all unchanged
// tasks is updated to the newest version.
func handleUnchangedInstancesInUpdate(
	ctx context.Context,
	cachedUpdate cached.Update,
	cachedJob cached.Job,
	jobConfig *job.JobConfig) error {

	runtimes := make(map[uint32]cached.RuntimeDiff)
	instanceCount := jobConfig.GetInstanceCount()
	instancesTotal := cachedUpdate.GetGoalState().Instances

	for i := uint32(0); i < instanceCount; i++ {
		// first find the instances which have not been updated
		found := false
		for _, j := range instancesTotal {
			if i == j {
				// instance has been updated
				found = true
				break
			}
		}

		if found == false {
			// instance is left unchanged with this update
			runtimeDiff := cached.RuntimeDiff{
				cached.ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
				cached.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
			}
			runtimes[i] = runtimeDiff
		}
	}

	if len(runtimes) > 0 {
		// Just update the runtime of the tasks with the
		// new version and move on.
		if err := cachedJob.PatchTasks(ctx, runtimes); err != nil {
			return err
		}
	}

	return nil
}

// upgradeInstancesInUpdate will upgrade the existing instances
// TODO: implement rolling upgrade
// Currently, just upgrade all tasks without respecting the batch size
func upgradeInstancesInUpdate(
	ctx context.Context,
	cachedUpdate cached.Update,
	cachedJob cached.Job,
	jobConfig *job.JobConfig,
	goalStateDriver *driver) error {

	runtimes := make(map[uint32]cached.RuntimeDiff)
	instancesUpdated := cachedUpdate.GetInstancesUpdated()

	for _, instID := range instancesUpdated {
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

	for _, instID := range instancesUpdated {
		goalStateDriver.EnqueueTask(cachedJob.ID(), instID, time.Now())
	}

	return nil
}

// UpdateStart initializes the update. It will move the configuration version
// of the tasks which are not touched by this update to the new version.
// Then it will move the update state to ROLLING_FORWARD and start
// the rolling update process.
func UpdateStart(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	log.WithField("update_id", updateEnt.id.GetValue()).
		Info("update starting")

	// fetch the update and job from the cache
	cachedUpdate, cachedJob, err := fetchUpdateAndJobFromCache(
		ctx, updateEnt.id, goalStateDriver)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}
	if cachedUpdate == nil || cachedJob == nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return nil
	}

	jobID := cachedUpdate.JobID()
	// fetch the job configuration first
	jobConfig, err := goalStateDriver.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// lets write the new task configs first
	if err := goalStateDriver.taskStore.CreateTaskConfigs(
		ctx,
		jobID,
		jobConfig); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// lets first add the new instances
	if err = addInstancesInUpdate(
		ctx, cachedUpdate, cachedJob,
		jobConfig, goalStateDriver); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// then, update the configuration and desired configuration version of
	// all instances which do not need to be upgraded
	if err = handleUnchangedInstancesInUpdate(
		ctx, cachedUpdate, cachedJob, jobConfig); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// now lets work on upgrading existing instances
	if err = upgradeInstancesInUpdate(
		ctx, cachedUpdate, cachedJob,
		jobConfig, goalStateDriver); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// update the state of the job update
	if err = cachedUpdate.WriteProgress(
		ctx,
		pbupdate.State_ROLLING_FORWARD,
		[]uint32{},
		cachedUpdate.GetGoalState().Instances,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// In case there are no instances to upgrade or all instances are stopped,
	// enqueue to goal state immediately once.
	// TODO this can be used with rolling update to move starting the update
	// of existing instances to update from start.
	goalStateDriver.EnqueueUpdate(updateEnt.id, time.Now())

	goalStateDriver.mtx.updateMetrics.UpdateStart.Inc(1)
	return nil
}
