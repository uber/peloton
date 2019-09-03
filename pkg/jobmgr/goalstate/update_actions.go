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

	log "github.com/sirupsen/logrus"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"

	"go.uber.org/yarpc/yarpcerrors"
)

// UpdateAbortIfNeeded checks if the update identifier in the goal
// state engine is the same as the one in the job runtime updater (tracking
// the current job update). If not, then it aborts the update in the goal
// state engine and enqueue the current update.
func UpdateAbortIfNeeded(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		return err
	}

	runtime, err := cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	// maybe abort a workflow of a terminated job, reenenque job goal
	// state engine to untrack
	if util.IsPelotonJobStateTerminal(runtime.GetState()) &&
		util.IsPelotonJobStateTerminal(runtime.GetGoalState()) {
		goalStateDriver.EnqueueJob(updateEnt.jobID, time.Now())
	}

	if runtime.GetUpdateID().GetValue() == updateEnt.id.GetValue() {
		// update not been aborted, keep going
		return nil
	}

	if err := cachedWorkflow.Cancel(ctx, nil); err != nil {
		return err
	}

	// return an error to ensure other update actions are not run and to
	// enqueue the same update back to the queue again for untracking
	return yarpcerrors.AbortedErrorf("update aborted")
}

// UpdateReload reloads the update from the DB.
func UpdateReload(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	goalStateDriver.mtx.updateMetrics.UpdateReload.Inc(1)

	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		return err
	}

	if err := cachedWorkflow.Recover(ctx); err != nil {
		if !yarpcerrors.IsNotFound(err) {
			return err
		}
		// update not found in DB, just clean up from cache and goal state
		return UpdateUntrack(ctx, entity)
	}
	goalStateDriver.EnqueueUpdate(updateEnt.jobID, updateEnt.id, time.Now())
	return nil
}

// UpdateComplete indicates that all instances have been updated,
// and the update state should be marked complete.
func UpdateComplete(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		return err
	}

	// TODO: remove after recovery is done when reading state
	if cachedWorkflow.GetState().State == pbupdate.State_INVALID {
		return UpdateReload(ctx, entity)
	}

	completeState := pbupdate.State_SUCCEEDED
	if cachedWorkflow.GetState().State == pbupdate.State_ROLLING_BACKWARD {
		completeState = pbupdate.State_ROLLED_BACK
	}

	if err := cachedJob.WriteWorkflowProgress(
		ctx,
		updateEnt.id,
		completeState,
		cachedWorkflow.GetInstancesDone(),
		cachedWorkflow.GetInstancesFailed(),
		[]uint32{},
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateCompleteFail.Inc(1)
		return err
	}

	log.WithFields(log.Fields{
		"update_id":         updateEnt.id.GetValue(),
		"job_id":            cachedJob.ID().GetValue(),
		"update_type":       cachedWorkflow.GetWorkflowType().String(),
		"instances_failed":  len(cachedWorkflow.GetInstancesFailed()),
		"instances_done":    len(cachedWorkflow.GetInstancesDone()),
		"instances_added":   len(cachedWorkflow.GetInstancesAdded()),
		"instances_removed": len(cachedWorkflow.GetInstancesRemoved()),
		"instances_updated": len(cachedWorkflow.GetInstancesUpdated()),
	}).Info("update completed")

	// enqueue to the goal state engine to untrack the update
	goalStateDriver.EnqueueUpdate(updateEnt.jobID, updateEnt.id, time.Now())
	goalStateDriver.mtx.updateMetrics.UpdateComplete.Inc(1)
	return nil
}

// UpdateUntrack deletes the update from the cache and the goal state engine.
func UpdateUntrack(ctx context.Context, entity goalstate.Entity) error {
	var runtime *pbjob.RuntimeInfo
	var err error

	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	jobID := updateEnt.jobID
	cachedJob := goalStateDriver.jobFactory.AddJob(jobID)
	runtime, err = cachedJob.GetRuntime(ctx)
	if err != nil {
		return err
	}

	// clean up the update from cache and goal state
	goalStateDriver.DeleteUpdate(jobID, updateEnt.id)
	cachedJob.ClearWorkflow(updateEnt.id)
	goalStateDriver.mtx.updateMetrics.UpdateUntrack.Inc(1)

	// check if we have another job update to run
	if len(runtime.GetUpdateID().GetValue()) > 0 &&
		runtime.GetUpdateID().GetValue() != updateEnt.id.GetValue() {
		goalStateDriver.EnqueueUpdate(jobID, runtime.GetUpdateID(), time.Now())
		return nil
	}

	// update can be applied to a terminated job,
	// need to remove job from cache upon completion
	if util.IsPelotonJobStateTerminal(runtime.GetState()) &&
		util.IsPelotonJobStateTerminal(runtime.GetGoalState()) {
		goalStateDriver.EnqueueJob(jobID, time.Now())
	}

	// No more job update to run, so use the time to clean up any old
	// updates if they have not reached a terminal state yet
	updates, err := goalStateDriver.updateStore.GetUpdatesForJob(ctx, jobID.GetValue())
	if err != nil {
		return nil
	}

	for _, prevUpdateID := range updates {
		updateModel, err :=
			goalStateDriver.updateStore.GetUpdateProgress(ctx, prevUpdateID)
		if err != nil {
			continue
		}
		if !cached.IsUpdateStateTerminal(updateModel.GetState()) {
			// just enqueue one and let it untrack first
			goalStateDriver.EnqueueUpdate(jobID, prevUpdateID, time.Now())
			return nil
		}
	}
	return nil
}

// UpdateWriteProgress write the current progress of update
func UpdateWriteProgress(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	cachedWorkflow, cachedJob, err := fetchWorkflowAndJobFromCache(
		ctx, updateEnt.jobID, updateEnt.id, goalStateDriver)
	if err != nil || cachedWorkflow == nil || cachedJob == nil {
		return err
	}

	// TODO: remove after recovery is done when reading state
	if cachedWorkflow.GetState().State == pbupdate.State_INVALID {
		return UpdateReload(ctx, entity)
	}

	// all the instances being updated are finished, nothing new to update
	if len(cachedWorkflow.GetInstancesCurrent()) == 0 {
		goalStateDriver.mtx.updateMetrics.UpdateWriteProgress.Inc(1)
		return nil
	}

	instancesCurrent, instancesDone, instancesFailed, err := cached.GetUpdateProgress(
		ctx,
		cachedJob.ID(),
		cachedWorkflow,
		cachedWorkflow.GetGoalState().JobVersion,
		cachedWorkflow.GetInstancesCurrent(),
		goalStateDriver.taskStore,
	)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateWriteProgressFail.Inc(1)
		return err
	}

	err = cachedJob.WriteWorkflowProgress(
		ctx,
		updateEnt.id,
		cachedWorkflow.GetState().State,
		append(cachedWorkflow.GetInstancesDone(), instancesDone...),
		append(cachedWorkflow.GetInstancesFailed(), instancesFailed...),
		instancesCurrent,
	)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateWriteProgressFail.Inc(1)
		return err
	}

	goalStateDriver.mtx.updateMetrics.UpdateWriteProgress.Inc(1)
	return nil

}
