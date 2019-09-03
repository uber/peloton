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

package cached

import (
	"context"
	"sync"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/concurrency"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/storage"
	"github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// number of workers to do has task config changed for
	// get job update diff
	_defaultHasTaskConfigChangeWorkers = 25
)

// Update of a job being stored in the cache.
type Update interface {
	WorkflowStrategy

	// Identifier of the update
	ID() *peloton.UpdateID

	// Job identifier the update belongs to
	JobID() *peloton.JobID

	// Create creates the update in DB and cache
	Create(
		ctx context.Context,
		jobID *peloton.JobID,
		jobConfig jobmgrcommon.JobConfig,
		prevJobConfig *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		instanceAdded []uint32,
		instanceUpdated []uint32,
		instanceRemoved []uint32,
		workflowType models.WorkflowType,
		updateConfig *pbupdate.UpdateConfig,
		opaqueData *peloton.OpaqueData,
	) error

	// Modify modifies the update in DB and cache
	Modify(
		ctx context.Context,
		instancesAdded []uint32,
		instancesUpdated []uint32,
		instancesRemoved []uint32,
	) error

	// Update updates the update in DB and cache
	WriteProgress(ctx context.Context,
		state pbupdate.State,
		instancesDone []uint32,
		instanceFailed []uint32,
		instancesCurrent []uint32) error

	// Pause pauses the current update progress
	Pause(ctx context.Context, opaqueData *peloton.OpaqueData) error

	// Resume resumes a paused update, and update would change
	// to the state before pause
	Resume(ctx context.Context, opaqueData *peloton.OpaqueData) error

	// Recover recovers the update from DB into the cache
	Recover(ctx context.Context) error

	// Cancel is used to cancel the update
	Cancel(ctx context.Context, opaqueData *peloton.OpaqueData) error

	// Rollback is used to rollback the update.
	Rollback(
		ctx context.Context,
		currentConfig *pbjob.JobConfig,
		targetConfig *pbjob.JobConfig,
	) error

	// GetState returns the state of the update
	GetState() *UpdateStateVector

	// GetGoalState returns the goal state of the update
	GetGoalState() *UpdateStateVector

	// GetPrevState returns the previous state of the update
	GetPrevState() pbupdate.State

	// GetInstancesAdded returns the instance to be added with this update
	GetInstancesAdded() []uint32

	// GetInstancesUpdated returns the existing instances to be updated
	// with this update
	GetInstancesUpdated() []uint32

	// GetInstancesRemoved returns the existing instances to be removed
	// with this update
	GetInstancesRemoved() []uint32

	// GetInstancesCurrent returns the current set of instances being updated
	GetInstancesCurrent() []uint32

	// GetInstanceFailed returns the current set of instances marked as failed
	GetInstancesFailed() []uint32

	// GetInstancesDone returns the current set of instances updated
	GetInstancesDone() []uint32

	GetUpdateConfig() *pbupdate.UpdateConfig

	GetWorkflowType() models.WorkflowType

	// GetJobVersion returns job configuration version
	GetJobVersion() uint64

	// GetJobPrevVersion returns previous job configuration version
	GetJobPrevVersion() uint64

	// IsTaskInUpdateProgress returns true if a given task is
	// in progress for the given update, else returns false
	IsTaskInUpdateProgress(instanceID uint32) bool

	// IsTaskInFailed returns true if a given task is in the
	// instancesFailed list for the given update, else returns false
	IsTaskInFailed(instanceID uint32) bool

	// GetLastUpdateTime return the last update time of update object
	GetLastUpdateTime() time.Time
}

// UpdateStateVector is used to the represent the state and goal state
// of an update to the goal state engine.
type UpdateStateVector struct {
	// current update state
	State pbupdate.State
	// for state, it will be the old job config version
	// for goal state, it will be the desired job config version
	JobVersion uint64
	// For state, it will store the instances which have already been updated,
	// and for goal state, it will store all the instances which
	// need to be updated.
	Instances []uint32
}

// newUpdate creates a new cache update object
func newUpdate(
	updateID *peloton.UpdateID,
	jobFactory *jobFactory) *update {
	update := &update{
		id:         updateID,
		jobFactory: jobFactory,
	}

	return update
}

// IsUpdateStateTerminal returns true if the update has reach terminal state
func IsUpdateStateTerminal(state pbupdate.State) bool {
	switch state {
	case pbupdate.State_SUCCEEDED, pbupdate.State_ABORTED,
		pbupdate.State_FAILED, pbupdate.State_ROLLED_BACK:
		return true
	}
	return false
}

// IsUpdateStateActive returns true if the update is in active state
func IsUpdateStateActive(state pbupdate.State) bool {
	switch state {
	case pbupdate.State_ROLLING_FORWARD, pbupdate.State_ROLLING_BACKWARD:
		return true
	}
	return false
}

// update structure holds the information about a given job update in the cache
type update struct {
	// Mutex to acquire before accessing any update information in cache
	sync.RWMutex
	WorkflowStrategy

	jobID *peloton.JobID    // Parent job identifier
	id    *peloton.UpdateID // update identifier

	jobFactory *jobFactory // Pointer to the job factory object

	state     pbupdate.State // current update state
	prevState pbupdate.State // previous update state

	// the update configuration provided in the create request
	updateConfig *pbupdate.UpdateConfig

	// type of the update workflow
	workflowType models.WorkflowType

	// list of instances which will be updated with this update
	instancesTotal []uint32
	// list of instances which have already been updated successfully
	instancesDone []uint32
	// list of instances which have been marked as failed
	instancesFailed []uint32
	// list of instances which are currently being updated
	instancesCurrent []uint32
	// list of instances which have been added
	instancesAdded []uint32
	// list of existing instance which have been updated;
	instancesUpdated []uint32
	// list of existing instances which have been deleted;
	// instancesTotal should be union of instancesAdded,
	// instancesUpdated and instancesRemoved
	instancesRemoved []uint32

	jobVersion     uint64 // job configuration version
	jobPrevVersion uint64 // previous job configuration version

	lastUpdateTime time.Time // last update time of update object
}

func (u *update) ID() *peloton.UpdateID {
	u.RLock()
	defer u.RUnlock()

	return u.id
}

func (u *update) JobID() *peloton.JobID {
	u.RLock()
	defer u.RUnlock()

	return u.jobID
}

func (u *update) Create(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig jobmgrcommon.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	instanceAdded []uint32,
	instanceUpdated []uint32,
	instanceRemoved []uint32,
	workflowType models.WorkflowType,
	updateConfig *pbupdate.UpdateConfig,
	opaqueData *peloton.OpaqueData) error {
	u.Lock()
	defer u.Unlock()

	var state pbupdate.State
	var prevState pbupdate.State

	if updateConfig.GetStartPaused() == true {
		state = pbupdate.State_PAUSED
		prevState = pbupdate.State_INITIALIZED
	} else {
		state = pbupdate.State_INITIALIZED
	}

	updateModel := &models.UpdateModel{
		UpdateID:             u.id,
		JobID:                jobID,
		UpdateConfig:         updateConfig,
		JobConfigVersion:     jobConfig.GetChangeLog().GetVersion(),
		PrevJobConfigVersion: prevJobConfig.GetChangeLog().GetVersion(),
		State:                state,
		PrevState:            prevState,
		InstancesAdded:       instanceAdded,
		InstancesUpdated:     instanceUpdated,
		InstancesRemoved:     instanceRemoved,
		InstancesTotal:       uint32(len(instanceUpdated) + len(instanceAdded) + len(instanceRemoved)),
		Type:                 workflowType,
		OpaqueData:           opaqueData,
		CreationTime:         time.Now().Format(time.RFC3339Nano),
		UpdateTime:           time.Now().Format(time.RFC3339Nano),
	}

	// write initialized workflow state for instances on create update
	instancesInUpdate := append(instanceAdded, instanceUpdated...)
	instancesInUpdate = append(instancesInUpdate, instanceRemoved...)
	if err := u.writeWorkflowEvents(
		ctx,
		instancesInUpdate,
		workflowType,
		state,
	); err != nil {
		return err
	}

	// Store the new update in DB
	if err := u.jobFactory.updateStore.CreateUpdate(ctx, updateModel); err != nil {
		return err
	}

	u.populateCache(updateModel)

	return nil
}

func (u *update) Modify(
	ctx context.Context,
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	now := time.Now()
	updateModel := &models.UpdateModel{
		UpdateID:             u.id,
		JobConfigVersion:     u.jobVersion,
		PrevJobConfigVersion: u.jobPrevVersion,
		State:                u.state,
		PrevState:            u.prevState,
		InstancesAdded:       instancesAdded,
		InstancesUpdated:     instancesUpdated,
		InstancesRemoved:     instancesRemoved,
		InstancesDone:        uint32(len(u.instancesDone)),
		InstancesFailed:      uint32(len(u.instancesFailed)),
		InstancesCurrent:     u.instancesCurrent,
		InstancesTotal:       uint32(len(instancesUpdated) + len(instancesAdded) + len(instancesRemoved)),
		UpdateTime:           now.Format(time.RFC3339Nano),
	}

	// write current workflow state for all instances on modify update
	instancesInUpdate := append(instancesAdded, instancesUpdated...)
	instancesInUpdate = append(instancesInUpdate, instancesRemoved...)
	if err := u.writeWorkflowEvents(
		ctx,
		instancesInUpdate,
		u.workflowType,
		u.state,
	); err != nil {
		u.clearCache()
		return err
	}

	// Store the new update in DB
	if err := u.jobFactory.updateStore.ModifyUpdate(ctx, updateModel); err != nil {
		u.clearCache()
		return err
	}

	// populate in cache
	u.instancesAdded = instancesAdded
	u.instancesUpdated = instancesUpdated
	u.instancesRemoved = instancesRemoved
	u.instancesTotal = append(instancesUpdated, instancesAdded...)
	u.instancesTotal = append(u.instancesTotal, instancesRemoved...)
	u.lastUpdateTime = now

	log.WithField("update_id", u.id.GetValue()).
		WithField("instances_total", len(u.instancesTotal)).
		WithField("instances_added", len(u.instancesAdded)).
		WithField("instance_updated", len(u.instancesUpdated)).
		WithField("instance_removed", len(u.instancesRemoved)).
		Debug("update is modified")
	return nil
}

func (u *update) WriteProgress(
	ctx context.Context,
	state pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	// if state is PAUSED, does not WriteProgress
	// to overwrite the state, because it should be
	// only be overwritten by Resume and Cancel.
	// The branch can be reached when state is changed
	// to PAUSED in handler, and the update is being
	// processed in goal state engine
	if u.state == pbupdate.State_PAUSED {
		state = pbupdate.State_PAUSED
	}

	return u.writeProgress(
		ctx,
		state,
		instancesDone,
		instancesFailed,
		instancesCurrent,
		nil,
	)
}

func (u *update) Pause(ctx context.Context, opaqueData *peloton.OpaqueData) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	// already paused, do nothing
	if u.state == pbupdate.State_PAUSED {
		return nil
	}

	return u.writeProgress(
		ctx,
		pbupdate.State_PAUSED,
		u.instancesDone,
		u.instancesFailed,
		u.instancesCurrent,
		opaqueData,
	)
}

func (u *update) Resume(ctx context.Context, opaqueData *peloton.OpaqueData) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	// already unpaused, do nothing
	if u.state != pbupdate.State_PAUSED {
		return nil
	}

	return u.writeProgress(
		ctx,
		u.prevState,
		u.instancesDone,
		u.instancesFailed,
		u.instancesCurrent,
		opaqueData,
	)
}

// writeProgress write update progress into cache and db,
// it is not concurrency safe and must be called with lock held.
func (u *update) writeProgress(
	ctx context.Context,
	state pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32,
	opaqueData *peloton.OpaqueData) error {
	// once an update is in terminal state, it should
	// not have any more state change
	if IsUpdateStateTerminal(u.state) {
		return nil
	}

	// only update the prevState if it has changed, otherwise
	// we would lose the prevState if WriteProgress is called
	// with the same state for several times
	prevState := u.prevState
	if u.state != state {
		prevState = u.state

		// write job update event on update's state change
		if err := u.jobFactory.jobUpdateEventsOps.Create(
			ctx,
			u.id,
			u.workflowType,
			state); err != nil {
			u.clearCache()
			return err
		}
	}

	now := time.Now()
	// Only change UpdateTime if no state changes.
	// It is necessary because UpdateTime is used to check
	// if a workflow is stale (i.e. not updated for a long period of time).
	// For example, if a workflow is run with bad config that results in
	// instances crash loop, the state would not be changed. However,
	// the workflow should not be considered as stale.
	if !u.updateStateChanged(
		state,
		prevState,
		instancesDone,
		instancesFailed,
		instancesCurrent,
	) {
		// ignore error here, since UpdateTime is informational,
		u.jobFactory.updateStore.WriteUpdateProgress(
			ctx,
			&models.UpdateModel{UpdateID: u.id, UpdateTime: now.Format(time.RFC3339Nano)})
		u.lastUpdateTime = now
		return nil
	}

	updateModel := &models.UpdateModel{
		UpdateID:         u.id,
		PrevState:        prevState,
		State:            state,
		InstancesDone:    uint32(len(instancesDone)),
		InstancesFailed:  uint32(len(instancesFailed)),
		InstancesCurrent: instancesCurrent,
		OpaqueData:       opaqueData,
		UpdateTime:       now.Format(time.RFC3339Nano),
	}

	if IsUpdateStateTerminal(state) {
		updateModel.CompletionTime = now.Format(time.RFC3339Nano)
	}
	if err := u.jobFactory.updateStore.WriteUpdateProgress(ctx, updateModel); err != nil {
		// clear the cache on DB error to avoid cache inconsistency
		u.clearCache()
		return err
	}

	// TODO: Add error handling, if accurate persistence of workflow events
	// is required.
	u.writeWorkflowProgressForInstances(
		ctx,
		u.id,
		instancesCurrent,
		instancesDone,
		instancesFailed,
		u.workflowType,
		state)

	u.prevState = prevState
	u.instancesCurrent = instancesCurrent
	u.instancesFailed = instancesFailed
	u.state = state
	u.instancesDone = instancesDone
	u.lastUpdateTime = now
	return nil
}

// returns if the new state changes compared with the in-memory state.
// it is used to decide if update progress in db need to be updated,
// so when in doubt, it would return true.
func (u *update) updateStateChanged(
	state pbupdate.State,
	prevState pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32,
) bool {
	// cache is invalidated, when in doubt assume the state changed.
	if u.state == pbupdate.State_INVALID {
		return true
	}

	return state != u.state ||
		prevState != u.prevState ||
		!equalInstances(instancesCurrent, u.instancesCurrent) ||
		!equalInstances(instancesDone, u.instancesDone) ||
		!equalInstances(instancesFailed, u.instancesFailed)
}

// returns if the two lists of instances are the same
func equalInstances(instances1 []uint32, instances2 []uint32) bool {
	if len(instances1) != len(instances2) {
		return false
	}

	// if the two instance list are equal, length of intersection
	// should be the same as length of original list
	intersect := util.IntersectSlice(instances1, instances2)
	return len(intersect) == len(instances1)
}

func (u *update) Recover(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

	return u.recover(ctx)
}

func (u *update) recover(ctx context.Context) error {
	// update is already recovered
	if u.state != pbupdate.State_INVALID {
		return nil
	}

	updateModel, err := u.jobFactory.updateStore.GetUpdate(ctx, u.id)
	if err != nil {
		return err
	}

	u.populateCache(updateModel)

	// Skip recovering terminated update for performance
	if IsUpdateStateTerminal(updateModel.State) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
		}).Debug("skip recover update progress for terminated update")
		return nil
	}

	// TODO: optimize the recover path, since it needs to read from task store
	// for each task
	// recover update progress
	u.instancesCurrent, u.instancesDone, u.instancesFailed, err = getUpdateProgress(
		ctx,
		u.jobID,
		u.WorkflowStrategy,
		u.updateConfig.GetMaxInstanceAttempts(),
		u.jobVersion,
		u.instancesTotal,
		u.instancesRemoved,
		u.jobFactory.taskStore,
	)
	if err != nil {
		u.clearCache()
		return err
	}
	return nil
}

func (u *update) Cancel(ctx context.Context, opaqueData *peloton.OpaqueData) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	return u.writeProgress(
		ctx,
		pbupdate.State_ABORTED,
		u.instancesDone,
		u.instancesFailed,
		u.instancesCurrent,
		opaqueData,
	)
}

// Rollback rolls back the current update.
func (u *update) Rollback(
	ctx context.Context,
	currentConfig *pbjob.JobConfig,
	targetConfig *pbjob.JobConfig,
) error {
	u.Lock()
	defer u.Unlock()

	// TODO: do recovery automatically when read state
	if err := u.recover(ctx); err != nil {
		return err
	}

	// Rollback should only happen when an update is in progress.
	// If an update is in terminal state, a new update with
	// the old config should be created.
	if IsUpdateStateTerminal(u.state) {
		return nil
	}

	// update is already rolling back, this can happen due to error retry
	if u.state == pbupdate.State_ROLLING_BACKWARD {
		return nil
	}

	instancesAdded, instancesUpdated, instancesRemoved, _, err := GetInstancesToProcessForUpdate(
		ctx,
		u.jobID,
		currentConfig,
		targetConfig,
		u.jobFactory.taskStore,
		u.jobFactory.taskConfigV2Ops,
	)
	if err != nil {
		return err
	}

	updateModel := &models.UpdateModel{
		UpdateID:             u.id,
		PrevState:            u.state,
		State:                pbupdate.State_ROLLING_BACKWARD,
		InstancesCurrent:     []uint32{},
		InstancesAdded:       instancesAdded,
		InstancesRemoved:     instancesRemoved,
		InstancesUpdated:     instancesUpdated,
		InstancesTotal:       uint32(len(instancesAdded) + len(instancesRemoved) + len(instancesUpdated)),
		JobConfigVersion:     targetConfig.GetChangeLog().GetVersion(),
		PrevJobConfigVersion: currentConfig.GetChangeLog().GetVersion(),
		InstancesDone:        0,
		InstancesFailed:      0,
		UpdateTime:           time.Now().Format(time.RFC3339Nano),
	}

	// writes ROLLING_BACKWARD workflow state for all instances in an update
	// to be rolled back.
	// Event is added for all instances even though the instances
	// have not gone through rollback yet.
	instancesInUpdate := append(instancesAdded, instancesUpdated...)
	instancesInUpdate = append(instancesInUpdate, instancesRemoved...)
	if err := u.writeWorkflowEvents(
		ctx,
		instancesInUpdate,
		u.workflowType,
		pbupdate.State_ROLLING_BACKWARD,
	); err != nil {
		u.clearCache()
		return err
	}

	if err := u.jobFactory.updateStore.ModifyUpdate(ctx, updateModel); err != nil {
		u.clearCache()
		return err
	}

	u.instancesCurrent = []uint32{}
	u.instancesDone = []uint32{}
	u.instancesFailed = []uint32{}
	u.populateCache(updateModel)

	return nil
}

func (u *update) GetState() *UpdateStateVector {
	u.RLock()
	defer u.RUnlock()

	instancesDone := make([]uint32, len(u.instancesDone))
	copy(instancesDone, u.instancesDone)

	instancesFailed := make([]uint32, len(u.instancesFailed))
	copy(instancesFailed, u.instancesFailed)

	return &UpdateStateVector{
		State:      u.state,
		Instances:  append(instancesDone, instancesFailed...),
		JobVersion: u.jobPrevVersion,
	}
}

func (u *update) GetGoalState() *UpdateStateVector {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesTotal))
	copy(instances, u.instancesTotal)

	return &UpdateStateVector{
		Instances:  instances,
		JobVersion: u.jobVersion,
	}
}

func (u *update) GetPrevState() pbupdate.State {
	u.RLock()
	defer u.RUnlock()

	return u.prevState
}

func (u *update) GetInstancesAdded() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesAdded))
	copy(instances, u.instancesAdded)
	return instances
}

func (u *update) GetInstancesUpdated() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesUpdated))
	copy(instances, u.instancesUpdated)
	return instances
}

func (u *update) GetInstancesDone() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesDone))
	copy(instances, u.instancesDone)
	return instances
}

func (u *update) GetInstancesRemoved() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesRemoved))
	copy(instances, u.instancesRemoved)
	return instances
}

func (u *update) GetInstancesCurrent() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesCurrent))
	copy(instances, u.instancesCurrent)
	return instances
}

func (u *update) GetInstancesFailed() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesFailed))
	copy(instances, u.instancesFailed)
	return instances
}

func (u *update) GetUpdateConfig() *pbupdate.UpdateConfig {
	u.RLock()
	defer u.RUnlock()

	if u.updateConfig == nil {
		return nil
	}
	updateConfig := *u.updateConfig
	return &updateConfig
}

func (u *update) GetWorkflowType() models.WorkflowType {
	u.RLock()
	defer u.RUnlock()

	return u.workflowType
}

func (u *update) GetJobVersion() uint64 {
	u.RLock()
	defer u.RUnlock()

	return u.jobVersion
}

func (u *update) GetJobPrevVersion() uint64 {
	u.RLock()
	defer u.RUnlock()

	return u.jobPrevVersion
}

// IsTaskInUpdateProgress returns true if a given task is
// in progress for the given update, else returns false
func (u *update) IsTaskInUpdateProgress(instanceID uint32) bool {
	for _, currentInstance := range u.GetInstancesCurrent() {
		if instanceID == currentInstance {
			return true
		}
	}
	return false
}

func (u *update) IsTaskInFailed(instanceID uint32) bool {
	for _, currentInstance := range u.GetInstancesFailed() {
		if instanceID == currentInstance {
			return true
		}
	}
	return false
}

// populate info in updateModel into update
func (u *update) populateCache(updateModel *models.UpdateModel) {
	if updateModel.GetUpdateConfig() != nil {
		u.updateConfig = updateModel.GetUpdateConfig()
	}
	if updateModel.GetType() != models.WorkflowType_UNKNOWN {
		u.workflowType = updateModel.GetType()
	}
	if updateModel.GetJobID() != nil {
		u.jobID = updateModel.GetJobID()
	}

	u.state = updateModel.GetState()
	u.prevState = updateModel.GetPrevState()
	u.instancesCurrent = updateModel.GetInstancesCurrent()
	u.instancesAdded = updateModel.GetInstancesAdded()
	u.instancesRemoved = updateModel.GetInstancesRemoved()
	u.instancesUpdated = updateModel.GetInstancesUpdated()
	u.jobVersion = updateModel.GetJobConfigVersion()
	u.jobPrevVersion = updateModel.GetPrevJobConfigVersion()
	u.instancesTotal = append(updateModel.GetInstancesUpdated(), updateModel.GetInstancesAdded()...)
	u.instancesTotal = append(u.instancesTotal, updateModel.GetInstancesRemoved()...)
	u.WorkflowStrategy = getWorkflowStrategy(updateModel.GetState(), updateModel.GetType())
	u.lastUpdateTime, _ = time.Parse(time.RFC3339Nano, updateModel.GetUpdateTime())
}

func (u *update) clearCache() {
	u.state = pbupdate.State_INVALID
	u.prevState = pbupdate.State_INVALID
	u.instancesTotal = nil
	u.instancesDone = nil
	u.instancesFailed = nil
	u.instancesCurrent = nil
	u.instancesAdded = nil
	u.instancesUpdated = nil
	u.instancesRemoved = nil
}

// GetUpdateProgress iterates through instancesToCheck and check if they are running and
// their current config version is the same as the desired config version.
// TODO: find the right place to put the func
func GetUpdateProgress(
	ctx context.Context,
	jobID *peloton.JobID,
	cachedUpdate Update,
	desiredConfigVersion uint64,
	instancesToCheck []uint32,
	taskStore storage.TaskStore,
) (instancesCurrent []uint32, instancesDone []uint32, instancesFailed []uint32, err error) {
	// TODO: figure out if cache can be used to read task runtime
	return getUpdateProgress(
		ctx,
		jobID,
		cachedUpdate,
		cachedUpdate.GetUpdateConfig().GetMaxInstanceAttempts(),
		desiredConfigVersion,
		instancesToCheck,
		cachedUpdate.GetInstancesRemoved(),
		taskStore,
	)
}

// getUpdateProgress is the internal version of GetUpdateProgress, which does not depend on cachedUpdate.
// Therefore it can be used inside of cachedUpdate without deadlock risk.
func getUpdateProgress(
	ctx context.Context,
	jobID *peloton.JobID,
	strategy WorkflowStrategy,
	maxInstanceAttempts uint32,
	desiredConfigVersion uint64,
	instancesToCheck []uint32,
	instancesRemoved []uint32,
	taskStore storage.TaskStore,
) (instancesCurrent []uint32, instancesDone []uint32, instancesFailed []uint32, err error) {
	for _, instID := range instancesToCheck {
		runtime, err := taskStore.GetTaskRuntime(ctx, jobID, instID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				if contains(instID, instancesRemoved) {
					instancesDone = append(instancesDone, instID)
				} else {
					log.WithFields(log.Fields{
						"job_id":      jobID.GetValue(),
						"instance_id": instID,
					}).Error(
						"instance in cache but runtime is missing from DB during update")
				}
				continue
			}
			return nil, nil, nil, err
		}

		if strategy.IsInstanceComplete(desiredConfigVersion, runtime) {
			instancesDone = append(instancesDone, instID)
		} else if strategy.IsInstanceFailed(runtime, maxInstanceAttempts) {
			instancesFailed = append(instancesFailed, instID)
		} else if strategy.IsInstanceInProgress(desiredConfigVersion, runtime) {
			// instances set to desired configuration, but has not entered RUNNING state
			instancesCurrent = append(instancesCurrent, instID)
		}
	}

	return instancesCurrent, instancesDone, instancesFailed, nil
}

// hasInstanceConfigChanged is a helper function to determine if the configuration
// of a given task has changed from its current configuration.
func hasInstanceConfigChanged(
	ctx context.Context,
	jobID *peloton.JobID,
	instID uint32,
	configVersion uint64,
	newJobConfig *pbjob.JobConfig,
	taskConfigV2Ops objects.TaskConfigV2Ops,
) (bool, error) {
	// Get the current task configuration. Cannot use prevTaskConfig to do
	// so because the task may be still be on an older configuration
	// version because the previous update may not have succeeded.
	// So, fetch the task configuration of the task from the DB.
	prevTaskConfig, _, err := taskConfigV2Ops.GetTaskConfig(
		ctx, jobID, instID, configVersion)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			//  configuration not found, just update it
			return true, nil
		}
		return false, err
	}

	newTaskConfig := taskconfig.Merge(
		newJobConfig.GetDefaultConfig(),
		newJobConfig.GetInstanceConfig()[instID])
	return taskconfig.HasTaskConfigChanged(prevTaskConfig, newTaskConfig), nil
}

type instanceConfigChange struct {
	instanceID uint32
	isChanged  bool
	err        error
}

// GetInstancesToProcessForUpdate determines the instances which have been updated in a given
// job update. Both the old and the new job configurations are provided as
// inputs, and it returns the instances which have been added and existing
// instances which have been updated.
func GetInstancesToProcessForUpdate(
	ctx context.Context,
	jobID *peloton.JobID,
	prevJobConfig *pbjob.JobConfig,
	newJobConfig *pbjob.JobConfig,
	taskStore storage.TaskStore,
	taskConfigV2Ops objects.TaskConfigV2Ops,
) (
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32,
	instancesUnchanged []uint32,
	err error,
) {

	var lock sync.RWMutex
	// reason to create instancesUpdate and instancesUnchange because if
	// one of the go-routine fails in mapper then it does early exit which
	// leads to assign nil for instancesUpdated and instancesUnchanged
	var instancesToCheck []uint32
	var instancesToUpdate []uint32
	var instancesNotChanged []uint32
	var taskRuntimes map[uint32]*pbtask.RuntimeInfo
	taskRuntimes, err = taskStore.GetTaskRuntimesForJobByRange(
		ctx,
		jobID,
		nil,
	)
	if err != nil {
		return
	}

	for instID := uint32(0); instID < newJobConfig.GetInstanceCount(); instID++ {
		if _, ok := taskRuntimes[instID]; !ok {
			// new instance added
			instancesAdded = append(instancesAdded, instID)
		} else {
			instancesToCheck = append(instancesToCheck, instID)
		}
	}

	f := func(ctx context.Context, instID interface{}) (interface{}, error) {
		lock.RLock()
		instanceID := instID.(uint32)
		runtime := taskRuntimes[instanceID]
		lock.RUnlock()

		changed, err := hasInstanceConfigChanged(
			ctx,
			jobID,
			instanceID,
			runtime.GetConfigVersion(),
			newJobConfig,
			taskConfigV2Ops,
		)
		if err != nil {
			return nil, err
		}

		lock.Lock()
		defer lock.Unlock()
		if changed || runtime.GetConfigVersion() != runtime.GetDesiredConfigVersion() {
			// Update if configuration has changed.
			// Also, if the configuration version is not the same as the desired
			// configuration version, then lets treat this instance as one
			// which needs to be updated irrespective of whether the current
			// config it has is the same as provided in the new configuration.
			// In some cases it may result in an unneeded restart of a
			// few instances, but this ensures correctness.
			instancesToUpdate = append(instancesToUpdate, instanceID)
		} else {
			instancesNotChanged = append(instancesNotChanged, instanceID)
		}

		// TODO what happens if the update does not change the instance
		// configuration, but it was being updates as part of the
		// previous aborted update.
		delete(taskRuntimes, instanceID)

		return nil, nil
	}

	var inputs []interface{}
	for _, i := range instancesToCheck {
		inputs = append(inputs, i)
	}

	_, err = concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		_defaultHasTaskConfigChangeWorkers)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	for instID := range taskRuntimes {
		// instance has been removed
		instancesRemoved = append(instancesRemoved, instID)
	}

	instancesUpdated = instancesToUpdate
	instancesUnchanged = instancesNotChanged

	return
}

// writeWorkflowEvents writes update state event for job and impacted instances
// Persistenting job update event is required on any workflow action, and
// if error occurs in this function, caller must retry an update
// such that update events for instance and job are guaranteed to be persisted
// Currently, this function is called on CREATE, MODIFY and ROLLING_BACKWARD
// a workflow.
func (u *update) writeWorkflowEvents(
	ctx context.Context,
	instances []uint32,
	workflowType models.WorkflowType,
	state pbupdate.State,
) error {

	if err := u.addWorkflowEventForInstances(
		ctx,
		u.id,
		workflowType,
		state,
		instances); err != nil {
		return err
	}

	// only need to add job update events if new state is different from existing state
	if state != u.state {
		if err := u.jobFactory.jobUpdateEventsOps.Create(
			ctx,
			u.id,
			workflowType,
			state); err != nil {
			return err
		}
	}

	return nil
}

func (u *update) GetLastUpdateTime() time.Time {
	u.RLock()
	defer u.RUnlock()

	return u.lastUpdateTime
}

// writeWorkflowProgressForInstances writes workflow progress for instances,
// in process of updating or are already updated (success/failure).
// - Add instances that succeeded
// - Add instances that failed
// - Add instances that are in process
// Workflow events are persisted for debugging a job update progress,
// and failure to persist workflow events will not retry an update
// for following scenarios, since retry cost for a scenario is high
// - Progress on ROLLING_FORWARD/ROLLING_BACKWARD
// - Write state change (PAUSED/RESUME/ABORT) for current instances only,
//   unprocessed instances won't have update state change event.
func (u *update) writeWorkflowProgressForInstances(
	ctx context.Context,
	updateID *peloton.UpdateID,
	instancesCurrent []uint32,
	instancesDone []uint32,
	instancesFailed []uint32,
	workflowType models.WorkflowType,
	state pbupdate.State) error {
	// instances updated successfully
	succeededInstances := util.IntersectSlice(u.instancesCurrent, instancesDone)
	if err := u.addWorkflowEventForInstances(
		ctx,
		updateID,
		workflowType,
		pbupdate.State_SUCCEEDED,
		succeededInstances); err != nil {
		return err
	}

	// instances update failed
	failedInstances := util.IntersectSlice(u.instancesCurrent, instancesFailed)
	if err := u.addWorkflowEventForInstances(
		ctx,
		updateID,
		workflowType,
		pbupdate.State_FAILED,
		failedInstances); err != nil {
		return err
	}

	// new instances to update
	if err := u.addWorkflowEventForInstances(
		ctx,
		updateID,
		workflowType,
		state,
		instancesCurrent); err != nil {
		return err
	}

	return nil
}

// addWorkflowEventForInstances writes the workflow state & type for
// instances that are part of the update.
func (u *update) addWorkflowEventForInstances(
	ctx context.Context,
	updateID *peloton.UpdateID,
	workflowType models.WorkflowType,
	workflowState pbupdate.State,
	instances []uint32) error {
	// addWorkflowEvent persists the workflow event for an instance at storage
	addWorkflowEvent := func(id uint32) error {
		return u.jobFactory.updateStore.AddWorkflowEvent(
			ctx,
			updateID,
			id,
			workflowType,
			workflowState)
	}

	// add workflow events for provided instances in parallel batches.
	return util.RunInParallel(u.id.GetValue(), instances, addWorkflowEvent)
}

// contains is a helper function to check if an element is present in the list
func contains(element uint32, slice []uint32) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}
