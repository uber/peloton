package cached

import (
	"context"
	"sync"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common/taskconfig"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/storage"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
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
		jobConfig *pbjob.JobConfig,
		prevJobConfig *pbjob.JobConfig,
		configAddOn *models.ConfigAddOn,
		instanceAdded []uint32,
		instanceUpdated []uint32,
		instanceRemoved []uint32,
		workflowType models.WorkflowType,
		updateConfig *pbupdate.UpdateConfig,
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
	Pause(ctx context.Context) error

	// Resume resumes a paused update, and update would change
	// to the state before pause
	Resume(ctx context.Context) error

	// Recover recovers the update from DB into the cache
	Recover(ctx context.Context) error

	// Cancel is used to cancel the update
	Cancel(ctx context.Context) error

	// Rollback is used to rollback the update.
	// It makes a copy of the previous config. The job points to the config copy
	// and update to the config copy.
	Rollback(ctx context.Context) error

	// GetState returns the state of the update
	GetState() *UpdateStateVector

	// GetGoalState returns the goal state of the update
	GetGoalState() *UpdateStateVector

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

	// IsTaskInUpdateProgress returns true if a given task is
	// in progress for the given update, else returns false
	IsTaskInUpdateProgress(instanceID uint32) bool

	// IsTaskInFailed returns true if a given task is in the
	// instancesFailed list for the given update, else returns false
	IsTaskInFailed(instanceID uint32) bool
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

// newUpdatecreates a new cache update object
func newUpdate(
	updateID *peloton.UpdateID,
	jobFactory JobFactory,
	updateFactory *updateFactory) *update {
	update := &update{
		id:            updateID,
		jobFactory:    jobFactory,
		updateFactory: updateFactory,
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

// update structure holds the information about a given job update in the cache
type update struct {
	// Mutex to acquire before accessing any update information in cache
	sync.RWMutex
	WorkflowStrategy

	jobID *peloton.JobID    // Parent job identifier
	id    *peloton.UpdateID // update identifier

	jobFactory    JobFactory     // Pointer to the job factory object
	updateFactory *updateFactory // Pointer to the parent update factory object

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
	jobConfig *pbjob.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	configAddOn *models.ConfigAddOn,
	instanceAdded []uint32,
	instanceUpdated []uint32,
	instanceRemoved []uint32,
	workflowType models.WorkflowType,
	updateConfig *pbupdate.UpdateConfig) error {

	if err := validateInput(
		jobConfig,
		prevJobConfig,
		instanceAdded,
		instanceUpdated,
		instanceRemoved,
	); err != nil {
		return err
	}

	u.Lock()
	defer u.Unlock()

	state := pbupdate.State_INITIALIZED

	// Store the new job configuration
	cachedJob := u.jobFactory.AddJob(jobID)
	newConfig, err := cachedJob.CompareAndSetConfig(ctx, jobConfig, configAddOn)
	if err != nil {
		return err
	}

	updateModel := &models.UpdateModel{
		UpdateID:             u.id,
		JobID:                jobID,
		UpdateConfig:         updateConfig,
		JobConfigVersion:     newConfig.GetChangeLog().GetVersion(),
		PrevJobConfigVersion: prevJobConfig.GetChangeLog().GetVersion(),
		State:                state,
		InstancesAdded:       instanceAdded,
		InstancesUpdated:     instanceUpdated,
		InstancesRemoved:     instanceRemoved,
		InstancesTotal:       uint32(len(instanceUpdated) + len(instanceAdded) + len(instanceRemoved)),
		Type:                 workflowType,
	}
	// Store the new update in DB
	if err := u.updateFactory.updateStore.CreateUpdate(ctx, updateModel); err != nil {
		return err
	}

	u.populateCache(updateModel)

	if err := u.updateJobConfigAndUpdateID(
		ctx,
		cachedJob,
		workflowType,
		newConfig.GetChangeLog().GetVersion()); err != nil {
		return err
	}

	log.WithField("update_id", u.id.GetValue()).
		WithField("job_id", jobID.GetValue()).
		WithField("instances_total", len(u.instancesTotal)).
		WithField("instances_added", len(u.instancesAdded)).
		WithField("instance_updated", len(u.instancesUpdated)).
		WithField("instance_removed", len(u.instancesRemoved)).
		WithField("update_state", u.state.String()).
		WithField("update_type", u.workflowType.String()).
		Debug("update is created")

	return nil
}

func (u *update) Modify(
	ctx context.Context,
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32) error {
	u.Lock()
	defer u.Unlock()

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
	}
	// Store the new update in DB
	if err := u.updateFactory.updateStore.ModifyUpdate(ctx, updateModel); err != nil {
		u.clearCache()
		return err
	}

	// populate in cache

	u.instancesAdded = instancesAdded
	u.instancesUpdated = instancesUpdated
	u.instancesRemoved = instancesRemoved
	u.instancesTotal = append(instancesUpdated, instancesAdded...)
	u.instancesTotal = append(u.instancesTotal, instancesRemoved...)

	log.WithField("update_id", u.id.GetValue()).
		WithField("instances_total", len(u.instancesTotal)).
		WithField("instances_added", len(u.instancesAdded)).
		WithField("instance_updated", len(u.instancesUpdated)).
		WithField("instance_removed", len(u.instancesRemoved)).
		Debug("update is modified")
	return nil
}

// updateJobConfigAndUpdateID updates job runtime with newConfigVersion and
// set the runtime updateID to u.id.
// It validates if the workflowType update is valid and retries if update fails due to
// concurrency error
func (u *update) updateJobConfigAndUpdateID(
	ctx context.Context,
	cachedJob Job,
	workflowType models.WorkflowType,
	newConfigVersion uint64,
) error {
	// 1. read job runtime
	// 2. decide if the current workflow of the job can be overwritten
	// 3. overwrite the workflow if step2 succeeds
	// 4. go to step 1, if step 3 fails due to concurrency error
	for i := 0; i < jobmgrcommon.MaxConcurrencyErrorRetry; i++ {
		jobRuntime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			return err
		}

		if err := u.validateWorkflowOverwrite(
			ctx,
			jobRuntime,
			workflowType,
		); err != nil {
			return err
		}

		jobRuntime.UpdateID = &peloton.UpdateID{
			Value: u.id.GetValue(),
		}
		jobRuntime.ConfigurationVersion = newConfigVersion
		_, err = cachedJob.CompareAndSetRuntime(
			ctx,
			jobRuntime,
		)

		// the error is due to another goroutine updates the runtime, which
		// results in a concurrency error. Redo the process again.
		if err == jobmgrcommon.UnexpectedVersionError {
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}

	return yarpcerrors.AbortedErrorf(
		"job update failed too many times due to concurrency error")
}

// validateWorkflowOverwrite validates if the new workflow type can override
// existing workflow in job runtime. It returns an error if the validation
// fails.
func (u *update) validateWorkflowOverwrite(
	ctx context.Context,
	jobRuntime *pbjob.RuntimeInfo,
	workflowType models.WorkflowType,
) error {

	updateID := jobRuntime.GetUpdateID()
	// no update ongoing, workflow update always succeeds
	if len(updateID.GetValue()) == 0 {
		return nil
	}

	updateModel, err := u.updateFactory.updateStore.GetUpdate(ctx, updateID)
	if err != nil {
		return err
	}

	// workflow update should succeed if previous update is terminal
	if IsUpdateStateTerminal(updateModel.GetState()) {
		return nil
	}

	// an overwrite is only valid if both current and new workflow
	// type is update
	if updateModel.GetType() == models.WorkflowType_UPDATE &&
		workflowType == models.WorkflowType_UPDATE {
		return nil
	}

	return yarpcerrors.InvalidArgumentErrorf(
		"workflow %s cannot overwrite workflow %s",
		workflowType.String(), updateModel.GetType().String())
}

func (u *update) WriteProgress(
	ctx context.Context,
	state pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32) error {
	u.Lock()
	defer u.Unlock()

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
	)
}

func (u *update) Pause(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

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
	)
}

func (u *update) Resume(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

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
	)
}

// writeProgress write update progress into cache and db,
// it is not concurrency safe and must be called with lock held.
func (u *update) writeProgress(
	ctx context.Context,
	state pbupdate.State,
	instancesDone []uint32,
	instancesFailed []uint32,
	instancesCurrent []uint32) error {
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
	}

	if err := u.updateFactory.updateStore.WriteUpdateProgress(
		ctx,
		&models.UpdateModel{
			UpdateID:         u.id,
			PrevState:        prevState,
			State:            state,
			InstancesDone:    uint32(len(instancesDone)),
			InstancesFailed:  uint32(len(instancesFailed)),
			InstancesCurrent: instancesCurrent,
		}); err != nil {
		// clear the cache on DB error to avoid cache inconsistency
		u.clearCache()
		return err
	}

	u.prevState = prevState
	u.instancesCurrent = instancesCurrent
	u.instancesFailed = instancesFailed
	u.state = state
	u.instancesDone = instancesDone
	return nil
}

func (u *update) Recover(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

	// update is already recovered
	if u.state != pbupdate.State_INVALID {
		return nil
	}

	updateModel, err := u.updateFactory.updateStore.GetUpdate(ctx, u.id)
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

	// recover update progress
	cachedJob := u.jobFactory.AddJob(u.jobID)
	u.instancesCurrent, u.instancesDone, u.instancesFailed, err = getUpdateProgress(
		ctx,
		cachedJob,
		u.WorkflowStrategy,
		u.updateConfig.GetMaxInstanceAttempts(),
		u.jobVersion,
		u.instancesTotal,
		u.instancesRemoved,
	)
	if err != nil {
		u.clearCache()
		return err
	}
	return nil
}

func (u *update) Cancel(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

	return u.writeProgress(
		ctx,
		pbupdate.State_ABORTED,
		u.instancesDone,
		u.instancesFailed,
		u.instancesCurrent,
	)
}

// Rollback rolls back the current update. It creates the copy of job and task
// config of the version to rollback to, and set the desired job version
// to the copy version. It also updates job to point to the config copy.
func (u *update) Rollback(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

	// Rollback should only happen when an update is in progress.
	// If an update is in terminal state, a new update with
	// the old config should be created.
	if IsUpdateStateTerminal(u.state) {
		return nil
	}

	cachedJob := u.jobFactory.AddJob(u.jobID)

	// update is already rolling back, this can happen due to error retry
	if u.state == pbupdate.State_ROLLING_BACKWARD {
		runtime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			return err
		}
		// runtime configuration version fails to be updated in the last call,
		// only need to update job
		if u.jobVersion != runtime.GetConfigurationVersion() {
			return u.updateJobConfigAndUpdateID(
				ctx,
				cachedJob,
				u.workflowType,
				u.jobVersion,
			)
		}
		// update and job are in expected state, no action is needed
		return nil
	}

	jobConfigCopy, err := u.copyJobAndTaskConfig(
		ctx,
		cachedJob,
		u.jobPrevVersion,
	)
	if err != nil {
		return err
	}

	currentJobConfig, _, err := u.updateFactory.jobStore.GetJobConfigWithVersion(
		ctx,
		u.jobID,
		u.jobVersion,
	)
	if err != nil {
		return err
	}

	instancesAdded, instancesUpdated, instancesRemoved, err := GetInstancesToProcessForUpdate(
		ctx,
		cachedJob,
		currentJobConfig,
		jobConfigCopy,
		u.updateFactory.taskStore,
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
		JobConfigVersion:     jobConfigCopy.GetChangeLog().GetVersion(),
		PrevJobConfigVersion: u.jobVersion,
		InstancesDone:        0,
		InstancesFailed:      0,
	}

	if err := u.updateFactory.updateStore.ModifyUpdate(ctx, updateModel); err != nil {
		u.clearCache()
		return err
	}

	u.instancesCurrent = []uint32{}
	u.instancesDone = []uint32{}
	u.instancesFailed = []uint32{}
	u.populateCache(updateModel)

	return u.updateJobConfigAndUpdateID(
		ctx,
		cachedJob,
		u.workflowType,
		u.jobVersion)
}

// copyJobAndTaskConfig copies the job config and task config of the version provided
// with new version number of value (max version of all configs + 1)
// It returns the copied config with changeLog updated.
func (u *update) copyJobAndTaskConfig(
	ctx context.Context,
	cachedJob Job,
	version uint64,
) (*pbjob.JobConfig, error) {
	jobConfig, configAddOn, err := u.updateFactory.jobStore.
		GetJobConfigWithVersion(ctx, u.jobID, version)
	if err != nil {
		log.WithError(err).
			WithField("job_id", u.jobID).
			WithField("update_id", u.id).
			Info("failed to get job config to copy for update rolling back")
		return nil, err
	}

	currentConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", u.jobID).
			WithField("update_id", u.id).
			Info("failed to get current job config for update rolling back")
		return nil, err
	}
	// set config changeLog version to that of current config for
	// concurrency control
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: currentConfig.GetChangeLog().GetVersion(),
	}
	// copy job config
	configCopy, err := cachedJob.CompareAndSetConfig(ctx, jobConfig, configAddOn)
	if err != nil {
		log.WithError(err).
			WithField("job_id", u.jobID).
			WithField("update_id", u.id).
			Info("failed to set job config for update rolling back")
		return nil, err
	}

	// change the job config version to that of config copy,
	// so task config would have the right version
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: configCopy.GetChangeLog().GetVersion(),
	}
	// copy task configs
	if err = u.updateFactory.taskStore.CreateTaskConfigs(ctx, u.jobID, jobConfig, configAddOn); err != nil {
		log.WithError(err).
			WithField("job_id", u.jobID).
			WithField("update_id", u.id).
			Error("failed to create task configs for update rolling back")
		return nil, err
	}

	return jobConfig, nil
}

func (u *update) GetState() *UpdateStateVector {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesDone",
		}).Debug("accessing fields in terminated update which can be stale")
	}

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

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesTotal",
		}).Debug("accessing fields in terminated update which can be stale")
	}

	instances := make([]uint32, len(u.instancesTotal))
	copy(instances, u.instancesTotal)

	return &UpdateStateVector{
		Instances:  instances,
		JobVersion: u.jobVersion,
	}
}

func (u *update) GetInstancesAdded() []uint32 {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesAdded",
		}).Warn("accessing fields in terminated update which can be stale")
	}

	instances := make([]uint32, len(u.instancesAdded))
	copy(instances, u.instancesAdded)
	return instances
}

func (u *update) GetInstancesUpdated() []uint32 {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesUpdated",
		}).Warn("accessing fields in terminated update which can be stale")
	}

	instances := make([]uint32, len(u.instancesUpdated))
	copy(instances, u.instancesUpdated)
	return instances
}

func (u *update) GetInstancesDone() []uint32 {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesDone",
		}).Warn("accessing fields in terminated update which can be stale")
	}

	instances := make([]uint32, len(u.instancesDone))
	copy(instances, u.instancesDone)
	return instances
}

func (u *update) GetInstancesRemoved() []uint32 {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state.String(),
			"field":     "instancesRemoved",
		}).Warn("accessing fields in terminated update which can be stale")
	}

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
}

func (u *update) clearCache() {
	u.state = pbupdate.State_INVALID
	u.prevState = pbupdate.State_INVALID
	u.instancesTotal = nil
	u.instancesDone = nil
	u.instancesCurrent = nil
	u.instancesAdded = nil
	u.instancesUpdated = nil
	u.instancesRemoved = nil
	u.workflowType = models.WorkflowType_UNKNOWN
}

func validateInput(
	jobConfig *pbjob.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	instanceAdded []uint32,
	instanceUpdated []uint32,
	instanceRemoved []uint32,
) error {
	// validate all instances in instanceUpdated is within old
	// instance config range
	for _, instanceID := range instanceUpdated {
		if instanceID >= prevJobConfig.GetInstanceCount() {
			return yarpcerrors.InvalidArgumentErrorf(
				"instance %d is out side of range for update", instanceID)
		}
	}

	// validate all instances in instanceAdded is within new
	// instance config range
	for _, instanceID := range instanceAdded {
		if instanceID >= jobConfig.GetInstanceCount() {
			return yarpcerrors.InvalidArgumentErrorf(
				"instance %d is out side of range for add", instanceID)
		}
	}

	// validate all removed instances is within old instance config range
	for _, instanceID := range instanceRemoved {
		if instanceID >= prevJobConfig.GetInstanceCount() {
			return yarpcerrors.InvalidArgumentErrorf(
				"instance %d is out side of range for update", instanceID)
		}
	}

	return nil
}

// GetUpdateProgress iterates through instancesToCheck and check if they are running and
// their current config version is the same as the desired config version.
// TODO: find the right place to put the func
func GetUpdateProgress(
	ctx context.Context,
	cachedJob Job,
	cachedUpdate Update,
	desiredConfigVersion uint64,
	instancesToCheck []uint32,
) (instancesCurrent []uint32, instancesDone []uint32, instancesFailed []uint32, err error) {
	return getUpdateProgress(
		ctx,
		cachedJob,
		cachedUpdate,
		cachedUpdate.GetUpdateConfig().GetMaxInstanceAttempts(),
		desiredConfigVersion,
		instancesToCheck,
		cachedUpdate.GetInstancesRemoved(),
	)
}

// getUpdateProgress is the internal version of GetUpdateProgress, which does not depend on cachedUpdate.
// Therefore it can be used inside of cachedUpdate without deadlock risk.
func getUpdateProgress(
	ctx context.Context,
	cachedJob Job,
	strategy WorkflowStrategy,
	maxInstanceAttempts uint32,
	desiredConfigVersion uint64,
	instancesToCheck []uint32,
	instancesRemoved []uint32,
) (instancesCurrent []uint32, instancesDone []uint32, instancesFailed []uint32, err error) {
	for _, instID := range instancesToCheck {
		cachedTask, err := cachedJob.AddTask(ctx, instID)
		// task is not created, this can happen when an update
		// adds more instances or instance is being removed
		if yarpcerrors.IsNotFound(err) ||
			err == InstanceIDExceedsInstanceCountError {
			if contains(instID, instancesRemoved) {
				instancesDone = append(instancesDone, instID)
			}
			continue
		}

		if err != nil {
			return nil, nil, nil, err
		}

		runtime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				// should never happen, so dump a sentry error
				log.WithFields(log.Fields{
					"job_id":      cachedJob.ID().GetValue(),
					"instance_id": instID,
				}).Error(
					"instance in cache but runtime is missing from DB during update")
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
	cachedJob Job,
	instID uint32,
	configVersion uint64,
	newJobConfig *pbjob.JobConfig,
	taskStore storage.TaskStore,
	labelsChanged bool,
) (bool, error) {
	if labelsChanged {
		// labels have changed, task needs to restarted
		return true, nil
	}

	// Get the current task configuration. Cannot use prevTaskConfig to do
	// so because the task may be still be on an older configuration
	// version because the previous update may not have succeeded.
	// So, fetch the task configuration of the task from the DB.
	prevTaskConfig, _, err := taskStore.GetTaskConfig(
		ctx, cachedJob.ID(), instID, configVersion)
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
	return taskConfigChange(prevTaskConfig, newTaskConfig), nil
}

// GetInstancesToProcessForUpdate determines the instances which have been updated in a given
// job update. Both the old and the new job configurations are provided as
// inputs, and it returns the instances which have been added and existing
// instances which have been updated.
func GetInstancesToProcessForUpdate(
	ctx context.Context,
	cachedJob Job,
	prevJobConfig *pbjob.JobConfig,
	newJobConfig *pbjob.JobConfig,
	taskStore storage.TaskStore,
) (
	instancesAdded []uint32,
	instancesUpdated []uint32,
	instancesRemoved []uint32,
	err error,
) {
	var taskRuntimes map[uint32]*pbtask.RuntimeInfo

	taskRuntimes, err = taskStore.GetTaskRuntimesForJobByRange(
		ctx,
		cachedJob.ID(),
		nil,
	)
	if err != nil {
		return
	}

	labelsChanged := labelsChangeCheck(
		prevJobConfig.GetLabels(),
		newJobConfig.GetLabels(),
	)

	for instID := uint32(0); instID < newJobConfig.GetInstanceCount(); instID++ {
		if runtime, ok := taskRuntimes[instID]; !ok {
			// new instance added
			instancesAdded = append(instancesAdded, instID)
		} else {
			var changed bool

			changed, err = hasInstanceConfigChanged(
				ctx,
				cachedJob,
				instID,
				runtime.GetConfigVersion(),
				newJobConfig,
				taskStore,
				labelsChanged,
			)
			if err != nil {
				return
			}

			if changed {
				// instance needs to be updated
				instancesUpdated = append(instancesUpdated, instID)
			}
			delete(taskRuntimes, instID)
		}
	}

	for instID := range taskRuntimes {
		// instance has been removed
		instancesRemoved = append(instancesRemoved, instID)
	}

	return
}

// labelsChangeCheck returns true if the labels have changed
func labelsChangeCheck(
	prevLabels []*peloton.Label,
	newLabels []*peloton.Label) bool {
	if len(prevLabels) != len(newLabels) {
		return true
	}

	for _, label := range newLabels {
		found := false
		for _, prevLabel := range prevLabels {
			if label.GetKey() == prevLabel.GetKey() &&
				label.GetValue() == prevLabel.GetValue() {
				found = true
				break
			}
		}

		// label not found
		if found == false {
			return true
		}
	}

	// all old labels found in new config as well
	return false
}

// taskConfigChange returns true if the task config (other than the name)
// has changed.
func taskConfigChange(
	prevTaskConfig *pbtask.TaskConfig,
	newTaskConfig *pbtask.TaskConfig) bool {
	if prevTaskConfig == nil || newTaskConfig == nil {
		return true
	}

	oldName := prevTaskConfig.GetName()
	newName := newTaskConfig.GetName()
	prevTaskConfig.Name = ""
	newTaskConfig.Name = ""

	changed := !proto.Equal(prevTaskConfig, newTaskConfig)

	prevTaskConfig.Name = oldName
	newTaskConfig.Name = newName
	return changed
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
