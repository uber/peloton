package update

import (
	"context"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/storage"
)

// AbortJobUpdate is a helper function to abort a given job update.
// It is primarily used to abort previous updates when a new update
// overwrites the previous one or aborting a given update.
func AbortJobUpdate(
	ctx context.Context,
	updateID *peloton.UpdateID,
	updateStore storage.UpdateStore,
	updateFactory cached.UpdateFactory) error {
	// ensure that the previous update is not already terminated
	updateInfo, err := updateStore.GetUpdateProgress(ctx, updateID)
	if err != nil {
		return err
	}

	if cached.IsUpdateStateTerminal(updateInfo.GetState()) {
		return nil
	}

	// abort the previous non-terminal update
	cachedUpdate := updateFactory.GetUpdate(updateID)

	if cachedUpdate == nil {
		cachedUpdate = updateFactory.AddUpdate(updateID)
		if err = cachedUpdate.Recover(ctx); err != nil {
			// failed to recover previous update, fail this create request
			return err
		}
	}

	if err = cachedUpdate.Cancel(ctx); err != nil {
		// failed to cancel the previous update, since cannot run two
		// updates on the same job, fail this create request
		return err
	}

	return nil
}

// HasUpdate returns if a job has any update going
func HasUpdate(jobRuntime *pbjob.RuntimeInfo) bool {
	return jobRuntime.GetUpdateID() != nil &&
		len(jobRuntime.GetUpdateID().GetValue()) > 0
}
