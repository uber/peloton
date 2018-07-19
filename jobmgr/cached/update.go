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
	"code.uber.internal/infra/peloton/util"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// Update of a job being stored in the cache.
type Update interface {
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
		updateConfig *pbupdate.UpdateConfig,
	) error

	// Update updates the update in DB and cache
	WriteProgress(ctx context.Context, state pbupdate.State,
		instancesDone []uint32, instancesCurrent []uint32) error

	// Recover recovers the update from DB into the cache
	Recover(ctx context.Context) error

	// Cancel is used to cancel the update
	Cancel(ctx context.Context) error

	// GetState returns the state of the update
	GetState() *UpdateStateVector
	// GetGoalState returns the goal state of the update
	GetGoalState() *UpdateStateVector

	// GetInstancesAdded returns the instance to be added with this update
	GetInstancesAdded() []uint32

	// GetInstancesUpdated returns the existing instances to be updated
	// with this update
	GetInstancesUpdated() []uint32

	// GetInstancesCurrent returns the current set of instances being updated
	GetInstancesCurrent() []uint32
}

// UpdateStateVector is used to the represent the state and goal state
// of an update to the goal state engine.
type UpdateStateVector struct {
	State pbupdate.State // current update state
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
	case pbupdate.State_SUCCEEDED, pbupdate.State_ABORTED:
		return true
	}
	return false
}

// update structure holds the information about a given job update in the cache
type update struct {
	// Mutex to acquire before accessing any update information in cache
	sync.RWMutex

	jobID *peloton.JobID    // Parent job identifier
	id    *peloton.UpdateID // update identifier

	jobFactory    JobFactory     // Pointer to the job factory object
	updateFactory *updateFactory // Pointer to the parent update factory object

	state pbupdate.State // current update state
	// the update configuration provided in the create request
	updateConfig *pbupdate.UpdateConfig

	// list of instances which will be updated with this update
	instancesTotal []uint32
	// list of instances which have already been updated
	instancesDone []uint32
	// list of instances which are currently being updated
	instancesCurrent []uint32
	// list of instances which have been added
	instancesAdded []uint32
	// list of existing instance which have been updated;
	// instancesTotal should be union of instancesAdded and instancesUpdated
	instancesUpdated []uint32

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

// labelsChangeCheck returns true if the labels have changed
func (u *update) labelsChangeCheck(
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
func (u *update) taskConfigChange(
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

// diffConfig determines the instances which have been updated in a given
// job update. Both the old and the new job configurations are provided as
// inputs, and it returns the instances which have been added, existing
// instances which have been updated, and all instances touched by this
// update (which is a union of the added and updated instances).
func (u *update) diffConfig(
	ctx context.Context,
	prevJobConfig *pbjob.JobConfig,
	newJobConfig *pbjob.JobConfig,
) (
	instancesTotal []uint32,
	instancesAdded []uint32,
	instancesUpdated []uint32,
	err error,
) {

	if newJobConfig.GetInstanceCount() > prevJobConfig.GetInstanceCount() {
		// New instances have been added
		for instID := uint32(prevJobConfig.GetInstanceCount()); instID < uint32(newJobConfig.GetInstanceCount()); instID++ {
			instancesTotal = append(instancesTotal, instID)
			instancesAdded = append(instancesAdded, instID)
		}
	}

	if u.labelsChangeCheck(
		prevJobConfig.GetLabels(),
		newJobConfig.GetLabels()) {
		// changing labels implies that all instances need to updated
		// so that new labels get updated in mesos
		for i := uint32(0); i < prevJobConfig.GetInstanceCount(); i++ {
			instancesTotal = append(instancesTotal, i)
			instancesUpdated = append(instancesUpdated, i)
		}
		return
	}

	j := u.jobFactory.AddJob(u.jobID)
	for i := uint32(0); i < prevJobConfig.GetInstanceCount(); i++ {
		// Get the current task configuration. Cannot use prevTaskConfig to do
		// so because the task may be still be on an older configurarion
		// version because the previous update may not have succeeded.
		// So, fetch the task configuration of the task from the DB.
		var runtime *pbtask.RuntimeInfo
		var prevTaskConfig *pbtask.TaskConfig

		t := j.AddTask(i)
		runtime, err = t.GetRunTime(ctx)
		if err != nil {
			return
		}

		prevTaskConfig, err = u.updateFactory.taskStore.GetTaskConfig(
			ctx, u.jobID, i, runtime.GetConfigVersion())
		if err != nil {
			return
		}

		newTaskConfig := taskconfig.Merge(
			newJobConfig.GetDefaultConfig(),
			newJobConfig.GetInstanceConfig()[i])

		if u.taskConfigChange(prevTaskConfig, newTaskConfig) {
			// instance needs to be updated
			instancesTotal = append(instancesTotal, i)
			instancesUpdated = append(instancesUpdated, i)
		}
	}

	return
}

func (u *update) Create(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *pbjob.JobConfig,
	prevJobConfig *pbjob.JobConfig,
	updateConfig *pbupdate.UpdateConfig) error {
	var err error

	u.Lock()
	defer u.Unlock()

	u.state = pbupdate.State_INITIALIZED
	u.jobID = jobID
	u.updateConfig = updateConfig

	// determine the instances being updated with this update
	if u.instancesTotal, u.instancesAdded, u.instancesUpdated, err =
		u.diffConfig(ctx, prevJobConfig, jobConfig); err != nil {
		return err
	}

	// Store the new job configuration
	cachedJob := u.jobFactory.AddJob(jobID)
	if err = cachedJob.Update(
		ctx,
		&pbjob.JobInfo{
			Config: jobConfig,
		},
		UpdateCacheAndDB,
	); err != nil {
		return err
	}

	newConfig, err := cachedJob.GetConfig(ctx)
	if err != nil {
		return err
	}

	// store the previous and current job configuration versions
	u.jobVersion = newConfig.GetChangeLog().GetVersion()
	u.jobPrevVersion = prevJobConfig.GetChangeLog().GetVersion()

	// Store the new update in DB
	if err = u.updateFactory.updateStore.CreateUpdate(
		ctx,
		&models.UpdateModel{
			UpdateID:             u.id,
			JobID:                u.jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     newConfig.GetChangeLog().GetVersion(),
			PrevJobConfigVersion: prevJobConfig.GetChangeLog().GetVersion(),
			State:                u.state,
			InstancesTotal:       uint32(len(u.instancesTotal)),
		}); err != nil {
		return err
	}

	// store the update identifier and new configuration version in the job runtime
	if err = cachedJob.Update(
		ctx,
		&pbjob.JobInfo{
			Runtime: &pbjob.RuntimeInfo{
				UpdateID:             u.id,
				ConfigurationVersion: newConfig.GetChangeLog().GetVersion(),
			},
		},
		UpdateCacheAndDB,
	); err != nil {
		return err
	}

	// TODO Make this log debug
	log.WithField("update_id", u.id.GetValue()).
		WithField("job_id", jobID.GetValue()).
		WithField("instances_total", len(u.instancesTotal)).
		WithField("instances_added", len(u.instancesAdded)).
		WithField("instance_updated", len(u.instancesUpdated)).
		WithField("update_state", u.state.String()).
		Info("update is created")

	return nil
}

func (u *update) WriteProgress(
	ctx context.Context,
	state pbupdate.State,
	instancesDone []uint32,
	instancesCurrent []uint32) error {
	u.Lock()
	defer u.Unlock()

	if err := u.updateFactory.updateStore.WriteUpdateProgress(
		ctx,
		&models.UpdateModel{
			UpdateID:         u.id,
			State:            state,
			InstancesDone:    uint32(len(instancesDone)),
			InstancesCurrent: instancesCurrent,
		}); err != nil {
		return err
	}

	u.instancesCurrent = instancesCurrent
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
			"state":     u.state,
		}).Debug("skip recover upgrade progress for terminated upgrade")
		return nil
	}

	err = u.recoverUpdateProgress(ctx)
	if err != nil {
		u.clearCache()
		return err
	}
	return nil
}

func (u *update) Cancel(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()

	// ignore canceling terminated updates
	if IsUpdateStateTerminal(u.state) {
		return nil
	}

	err := u.updateFactory.updateStore.WriteUpdateProgress(
		ctx,
		&models.UpdateModel{
			UpdateID:         u.id,
			State:            pbupdate.State_ABORTED,
			InstancesDone:    uint32(len(u.instancesDone)),
			InstancesCurrent: u.instancesCurrent,
		})

	if err == nil {
		u.state = pbupdate.State_ABORTED
	}

	return err
}

func (u *update) GetState() *UpdateStateVector {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state,
			"field":     "instancesDone",
		}).Debug("accessing fields in terminated upgrade which can be stale")
	}

	instances := make([]uint32, len(u.instancesDone))
	copy(instances, u.instancesDone)

	return &UpdateStateVector{
		State:     u.state,
		Instances: instances,
	}
}

func (u *update) GetGoalState() *UpdateStateVector {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state,
			"field":     "instancesTotal",
		}).Debug("accessing fields in terminated upgrade which can be stale")
	}

	instances := make([]uint32, len(u.instancesTotal))
	copy(instances, u.instancesTotal)

	return &UpdateStateVector{
		Instances: instances,
	}
}

func (u *update) GetInstancesAdded() []uint32 {
	u.RLock()
	defer u.RUnlock()

	if IsUpdateStateTerminal(u.state) {
		log.WithFields(log.Fields{
			"update_id": u.id.GetValue(),
			"job_id":    u.jobID.GetValue(),
			"state":     u.state,
			"field":     "instancesAdded",
		}).Warn("accessing fields in terminated upgrade which can be stale")
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
			"state":     u.state,
			"field":     "instancesUpdated",
		}).Warn("accessing fields in terminated upgrade which can be stale")
	}

	instances := make([]uint32, len(u.instancesUpdated))
	copy(instances, u.instancesUpdated)
	return instances
}

func (u *update) GetInstancesCurrent() []uint32 {
	u.RLock()
	defer u.RUnlock()

	instances := make([]uint32, len(u.instancesCurrent))
	copy(instances, u.instancesCurrent)
	return instances
}

// populate info in updateModel into update
func (u *update) populateCache(updateModel *models.UpdateModel) {
	u.updateConfig = updateModel.GetUpdateConfig()
	u.state = updateModel.GetState()
	u.jobID = updateModel.GetJobID()
	u.instancesCurrent = updateModel.GetInstancesCurrent()
	u.jobVersion = updateModel.GetJobConfigVersion()
	u.jobPrevVersion = updateModel.GetPrevJobConfigVersion()
}

func (u *update) recoverUpdateProgress(ctx context.Context) error {
	jobConfig, err := u.updateFactory.jobStore.GetJobConfigWithVersion(
		ctx, u.jobID, u.jobVersion)
	if err != nil {
		return err
	}

	prevJobConfig, err := u.updateFactory.jobStore.GetJobConfigWithVersion(
		ctx, u.jobID, u.jobPrevVersion)
	if err != nil {
		return err
	}

	// determine the instances being updated with this update
	if u.instancesTotal, u.instancesAdded, u.instancesUpdated, err =
		u.diffConfig(ctx, prevJobConfig, jobConfig); err != nil {
		return err
	}

	cachedJob := u.jobFactory.AddJob(u.jobID)
	u.instancesCurrent, u.instancesDone, err =
		GetUpdateProgress(ctx, cachedJob, u.instancesTotal)
	return err
}

func (u *update) clearCache() {
	u.state = pbupdate.State_INVALID
	u.instancesTotal = nil
	u.instancesDone = nil
	u.instancesCurrent = nil
	u.instancesAdded = nil
	u.instancesUpdated = nil
}

// GetUpdateProgress iterates through all tasks and check if they are running and
// their current config version is the same as the desired config version.
// TODO: find the right place to put the func
// TODO: fix instancesCurrent for rolling upgrade
func GetUpdateProgress(
	ctx context.Context,
	cachedJob Job,
	instancesTotal []uint32,
) (instancesCurrent []uint32, instancesDone []uint32, err error) {
	for _, instID := range instancesTotal {
		cachedTask := cachedJob.AddTask(instID)
		runtime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			return nil, nil, err
		}

		if (runtime.GetState() == pbtask.TaskState_RUNNING &&
			runtime.GetConfigVersion() == runtime.GetDesiredConfigVersion()) ||
			util.IsPelotonStateTerminal(runtime.GetGoalState()) {
			// Either instance has been updated, or instance is in terminal
			// goal state, so it will not be updated anyways => add to
			// instancesDone.
			instancesDone = append(instancesDone, instID)
		} else {
			// instance not updated yet, copy to instancesCurrent
			instancesCurrent = append(instancesCurrent, instID)
		}
	}
	return instancesCurrent, instancesDone, err
}
