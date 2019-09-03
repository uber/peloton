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

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	log "github.com/sirupsen/logrus"
)

// fetchWorkflowAndJobFromCache is a helper function to fetch the
// workflow and the job from the cache for a given update entity.
func fetchWorkflowAndJobFromCache(
	ctx context.Context,
	jobID *peloton.JobID,
	updateID *peloton.UpdateID,
	goalStateDriver *driver,
) (cachedWorkflow cached.Update, cachedJob cached.Job, err error) {
	// first fetch the job
	cachedJob = goalStateDriver.jobFactory.GetJob(jobID)

	if cachedJob == nil {
		// if job has been untracked, cancel the update and then enqueue into
		// goal state to untrack it.
		log.WithFields(
			log.Fields{
				"job_id":    jobID.GetValue(),
				"update_id": updateID.GetValue(),
			}).Info("job has been deleted, so canceling the update as well")

		cachedJob = goalStateDriver.jobFactory.AddJob(jobID)
		err = cachedJob.AddWorkflow(updateID).Cancel(ctx, nil)
		if err == nil {
			goalStateDriver.EnqueueUpdate(jobID, updateID, time.Now())
		}

		// clean up the job since it is untracked before
		goalStateDriver.EnqueueJob(jobID, time.Now())
		cachedJob = nil
		return
	}

	cachedWorkflow = cachedJob.AddWorkflow(updateID)
	return
}

// handleUnchangedInstancesInUpdate updates the runtime state of the
// instances left unchanged with the given update; essentially, the
// configuration and desired configuration version of all unchanged
// tasks is updated to the newest version.
func handleUnchangedInstancesInUpdate(
	ctx context.Context,
	cachedUpdate cached.Update,
	cachedJob cached.Job,
	jobConfig jobmgrcommon.JobConfig) error {

	runtimes := make(map[uint32]jobmgrcommon.RuntimeDiff)
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
			runtimeDiff := jobmgrcommon.RuntimeDiff{
				jobmgrcommon.ConfigVersionField:        jobConfig.GetChangeLog().GetVersion(),
				jobmgrcommon.DesiredConfigVersionField: jobConfig.GetChangeLog().GetVersion(),
			}
			runtimes[i] = runtimeDiff
		}
	}

	if len(runtimes) > 0 {
		// Just update the runtime of the tasks with the
		// new version and move on.
		_, instancesToBeRetried, err := cachedJob.PatchTasks(ctx, runtimes, false)
		if err != nil {
			return err
		}

		// if some patching of some instances need to be retried, return an
		// error here so that the UpdateStart action is retried.
		if len(instancesToBeRetried) != 0 {
			return _errTasksNotInCache
		}
	}

	return nil
}

// UpdateStart initializes the update. It will move the configuration version
// of the tasks which are not touched by this update to the new version.
// Then it will move the update state to ROLLING_FORWARD and enqueue to
// goal state engine to start the rolling update process.
func UpdateStart(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	// fetch the update and job from the cache
	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		log.WithFields(log.Fields{
			"update_id": updateEnt.id.GetValue(),
		}).WithError(err).Info("unable to start update")
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if cachedWorkflow.GetState().State == update.State_INVALID {
		return UpdateReload(ctx, entity)
	}

	jobID := cachedWorkflow.JobID()
	// fetch the job configuration first
	obj, err := goalStateDriver.jobConfigOps.GetResult(
		ctx,
		jobID,
		cachedWorkflow.GetGoalState().JobVersion)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}
	jobConfig := obj.JobConfig
	configAddOn := obj.ConfigAddOn

	var spec *stateless.JobSpec
	if obj.ApiVersion == common.V1AlphaApi {
		spec = obj.JobSpec
	}

	// lets write the new task configs first
	if err := cachedJob.CreateTaskConfigs(
		ctx,
		jobID,
		jobConfig,
		configAddOn,
		spec); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	if cachedWorkflow.GetWorkflowType() == models.WorkflowType_UPDATE {
		// Populate instancesAdded, instancesUpdated and instancesRemoved
		// by the update. This is not done in the handler because the previous
		// update may be running when this current update was created, and
		// hence the instances in this list may have changed. So do in start
		// to ensure that these list of instances remain the same
		// while the update is non-terminal.

		prevJobConfig, _, err := goalStateDriver.jobConfigOps.Get(
			ctx,
			jobID,
			cachedWorkflow.GetState().JobVersion,
		)
		if err != nil {
			goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
			return err
		}

		instancesAdded, instancesUpdated, instancesRemoved, _, err := cached.GetInstancesToProcessForUpdate(
			ctx,
			cachedJob.ID(),
			prevJobConfig,
			jobConfig,
			goalStateDriver.taskStore,
			goalStateDriver.taskConfigV2Ops,
		)
		if err != nil {
			goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
			return err
		}

		if err := cachedWorkflow.Modify(
			ctx,
			instancesAdded,
			instancesUpdated,
			instancesRemoved,
		); err != nil {
			goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
			return err
		}
	}

	// update the configuration and desired configuration version of
	// all instances which do not need to be updated
	if err = handleUnchangedInstancesInUpdate(
		ctx, cachedWorkflow, cachedJob, jobConfig); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	// update the state of the job update
	if err = cachedJob.WriteWorkflowProgress(
		ctx,
		updateEnt.id,
		update.State_ROLLING_FORWARD,
		[]uint32{},
		[]uint32{},
		[]uint32{},
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateStartFail.Inc(1)
		return err
	}

	log.WithFields(log.Fields{
		"update_id":         updateEnt.id.GetValue(),
		"job_id":            cachedJob.ID().GetValue(),
		"update_type":       cachedWorkflow.GetWorkflowType().String(),
		"instances_added":   len(cachedWorkflow.GetInstancesAdded()),
		"instances_removed": len(cachedWorkflow.GetInstancesRemoved()),
		"instances_updated": len(cachedWorkflow.GetInstancesUpdated()),
	}).Info("update starting")

	goalStateDriver.EnqueueUpdate(jobID, updateEnt.id, time.Now())
	goalStateDriver.mtx.updateMetrics.UpdateStart.Inc(1)
	return nil
}
