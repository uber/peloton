package goalstate

import (
	"context"
	"time"

	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/common/goalstate"

	"go.uber.org/yarpc/yarpcerrors"
)

// UpdateReload reloads the update from the DB.
func UpdateReload(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	cachedUpdate := goalStateDriver.updateFactory.AddUpdate(updateEnt.id)
	goalStateDriver.mtx.updateMetrics.UpdateReload.Inc(1)
	if err := cachedUpdate.Recover(ctx); err != nil {
		if !yarpcerrors.IsNotFound(err) {
			return err
		}
		// update not found in DB, just clean up from cache and goal state
		return UpdateUntrack(ctx, entity)
	}
	goalStateDriver.EnqueueUpdate(updateEnt.id, time.Now())
	return nil
}

// UpdateComplete indicates that all instances have been updated,
// and the update state should be marked complete.
func UpdateComplete(ctx context.Context, entity goalstate.Entity) error {
	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	cachedUpdate := goalStateDriver.updateFactory.GetUpdate(updateEnt.id)
	if cachedUpdate == nil {
		goalStateDriver.mtx.updateMetrics.UpdateCompleteFail.Inc(1)
		return nil
	}
	instancesTotal := cachedUpdate.GetGoalState().Instances

	if err := cachedUpdate.WriteProgress(
		ctx,
		pbupdate.State_SUCCEEDED,
		instancesTotal,
		[]uint32{},
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateCompleteFail.Inc(1)
		return err
	}

	// enqueue to the goal state engine to untrack the update
	goalStateDriver.EnqueueUpdate(updateEnt.id, time.Now())
	goalStateDriver.mtx.updateMetrics.UpdateComplete.Inc(1)
	return nil
}

// UpdateUntrack deletes the update from the cache and the goal state engine.
func UpdateUntrack(ctx context.Context, entity goalstate.Entity) error {
	// TODO: first remove the update id from the job runtime
	// if still the same as the update-id

	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver
	goalStateDriver.DeleteUpdate(updateEnt.id)
	goalStateDriver.updateFactory.ClearUpdate(updateEnt.id)
	goalStateDriver.mtx.updateMetrics.UpdateUntrack.Inc(1)
	return nil
}
