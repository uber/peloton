package goalstate

import (
	"context"
	"time"

	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	log "github.com/sirupsen/logrus"
)

// UpdateRun is responsible to check which instances have been updated,
// start the next set of instances to update and update the state
// of the job update in cache and DB.
func UpdateRun(ctx context.Context, entity goalstate.Entity) error {
	//TODO: implement rolling upgrade
	// Currently all instances are run at the same time, so just go
	// through all instances have reached RUNNING state in the desired
	// configuration version

	var instancesDone []uint32
	var instancesCurrent []uint32

	updateEnt := entity.(*updateEntity)
	goalStateDriver := updateEnt.driver

	log.WithField("update_id", updateEnt.id.GetValue()).
		Info("update running")

	cachedUpdate, cachedJob, err := fetchUpdateAndJobFromCache(
		ctx, updateEnt.id, goalStateDriver)
	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}
	if cachedUpdate == nil || cachedJob == nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return nil
	}

	jobID := cachedUpdate.JobID()

	instancesTotal := cachedUpdate.GetGoalState().Instances
	instancesCurrent, instancesDone, err = cached.GetUpdateProgress(
		ctx,
		cachedJob,
		instancesTotal,
	)

	if err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	state := pbupdate.State_ROLLING_FORWARD

	// update the state of the job update
	if err = cachedUpdate.WriteProgress(
		ctx,
		state,
		instancesDone,
		instancesCurrent,
	); err != nil {
		goalStateDriver.mtx.updateMetrics.UpdateRunFail.Inc(1)
		return err
	}

	if len(instancesTotal) == len(instancesDone) {
		goalStateDriver.EnqueueUpdate(jobID, updateEnt.id, time.Now())
	}
	goalStateDriver.mtx.updateMetrics.UpdateRun.Inc(1)
	return nil
}
