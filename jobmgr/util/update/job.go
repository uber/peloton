package update

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/storage"
)

// AbortPreviousJobUpdate is a helper function to abort a given job update.
// It is primarily used to abort previous updates when a new update
// overwrites the previous one.
func AbortPreviousJobUpdate(
	ctx context.Context,
	updateID *peloton.UpdateID,
	updateStore storage.UpdateStore,
	updateFactory cached.UpdateFactory,
	goalStateDriver goalstate.Driver) error {
	// ensure that the previous update is not already terminated
	updateInfo, err := updateStore.GetUpdateProgress(ctx, updateID)
	if err != nil {
		return err
	}

	if cached.IsUpdateStateTerminal(updateInfo.GetState()) {
		return nil
	}

	// abort the previous non-terminal update
	prevUpdate := updateFactory.GetUpdate(updateID)

	if prevUpdate == nil {
		prevUpdate = updateFactory.AddUpdate(updateID)
		if err = prevUpdate.Recover(ctx); err != nil {
			// failed to recover previous update, fail this create request
			return err
		}
	}

	if err = prevUpdate.Cancel(ctx); err != nil {
		// failed to cancel the previous update, since cannot run two
		// updates on the same job, fail this create request
		return err
	}

	// untrack the previous update
	goalStateDriver.EnqueueUpdate(updateID, time.Now())

	return nil
}
