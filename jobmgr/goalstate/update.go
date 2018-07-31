package goalstate

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	log "github.com/sirupsen/logrus"
)

// UpdateAction is a string for job update actions.
type UpdateAction string

const (
	// NoUpdateAction implies do not take any action
	NoUpdateAction UpdateAction = "noop"
	// ReloadUpdateAction will reload the update from DB
	ReloadUpdateAction UpdateAction = "reload"
	// StartUpdateAction will start the update
	StartUpdateAction UpdateAction = "start"
	// RunUpdateAction will continue running the rolling update
	RunUpdateAction UpdateAction = "run"
	// CompleteUpdateAction will complete the update
	CompleteUpdateAction UpdateAction = "complete"
	// ClearUpdateAction clears the update
	ClearUpdateAction UpdateAction = "update_clear"
	// CheckForAbortAction checks if the update needs to be aborted
	CheckForAbortAction UpdateAction = "check_for_abort"
	// WriteProgressUpdateAction writes the latest update progress
	WriteProgressUpdateAction UpdateAction = "write_progress"
)

// _updateActionsMaps maps the UpdateAction string to the Action function.
var (
	_updateActionsMaps = map[UpdateAction]goalstate.ActionExecute{
		NoUpdateAction:            nil,
		ReloadUpdateAction:        UpdateReload,
		StartUpdateAction:         UpdateStart,
		RunUpdateAction:           UpdateRun,
		CompleteUpdateAction:      UpdateComplete,
		ClearUpdateAction:         UpdateUntrack,
		WriteProgressUpdateAction: UpdateWriteProgress,
	}
)

var (
	_isoVersionsUpdateRules = map[update.State]UpdateAction{
		// unknown state, merely reload the update and try again
		update.State_INVALID: ReloadUpdateAction,
		// start running the update
		update.State_INITIALIZED: StartUpdateAction,
		// update is complete, clean it up from the cache and goal state
		update.State_SUCCEEDED: ClearUpdateAction,
		// update is complete, clean it up from the cache and goal state
		update.State_ABORTED: ClearUpdateAction,
		// update is paused, write the upgrade progress if there is
		// any instances under upgrade when pause is called
		update.State_PAUSED: WriteProgressUpdateAction,
	}
)

// NewUpdateEntity implements the goal state Entity interface for job updates.
func NewUpdateEntity(
	id *peloton.UpdateID,
	jobID *peloton.JobID,
	driver *driver) goalstate.Entity {
	return &updateEntity{
		id:     id,
		jobID:  jobID,
		driver: driver,
	}
}

type updateEntity struct {
	jobID  *peloton.JobID    // peloton job identifier
	id     *peloton.UpdateID // peloton update identifier
	driver *driver           // the goal state driver
}

func (u *updateEntity) GetID() string {
	// return job identifier; this ensures that only update for a
	// given job is running at a given time.
	return u.jobID.GetValue()
}

func (u *updateEntity) GetState() interface{} {
	cachedUpdate := u.driver.updateFactory.AddUpdate(u.id)
	return cachedUpdate.GetState()
}

func (u *updateEntity) GetGoalState() interface{} {
	cachedUpdate := u.driver.updateFactory.AddUpdate(u.id)
	return cachedUpdate.GetGoalState()
}

func (u *updateEntity) GetActionList(
	state interface{},
	goalState interface{}) (
	context.Context,
	context.CancelFunc,
	[]goalstate.Action) {
	var actions []goalstate.Action

	updateState := state.(*cached.UpdateStateVector)
	updateGoalState := goalState.(*cached.UpdateStateVector)

	actionStr := u.suggestUpdateAction(updateState, updateGoalState)
	action := _updateActionsMaps[actionStr]

	log.WithFields(
		log.Fields{
			"update_id":       u.id.GetValue(),
			"current_state":   updateState.State.String(),
			"instances_total": len(updateGoalState.Instances),
			"instances_done":  len(updateState.Instances),
			"update_action":   actionStr,
		}).Info("running update action")

	if actionStr != ClearUpdateAction && actionStr != ReloadUpdateAction {
		actions = append(actions, goalstate.Action{
			Name:    string(CheckForAbortAction),
			Execute: UpdateAbortIfNeeded,
		})
	}

	if action != nil {
		actions = append(actions, goalstate.Action{
			Name:    string(actionStr),
			Execute: action,
		})
	}

	return context.Background(), nil, actions
}

func (u *updateEntity) suggestUpdateAction(
	updateState *cached.UpdateStateVector,
	updateGoalState *cached.UpdateStateVector) UpdateAction {

	if updateAction, ok := _isoVersionsUpdateRules[updateState.State]; ok {
		return updateAction
	}

	if updateState.State == update.State_ROLLING_FORWARD {
		// TODO What if instancesTotal has same length as instancesDone, but has
		// different instances in them? It is not clear what to behave other than
		// give up.
		if len(updateState.Instances) == len(updateGoalState.Instances) {
			// update is complete
			return CompleteUpdateAction
		}
		// determine which instances have completed the upgrade,
		// and which need to be upgraded next.
		return RunUpdateAction
	}

	// dont do anything
	return NoUpdateAction
}
