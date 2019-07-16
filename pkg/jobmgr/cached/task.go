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

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/util"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

var uuidLength = len(uuid.New())

// IsResMgrOwnedState returns true if the task state indicates that the task
// is either waiting for admission or being placed or being preempted.
func IsResMgrOwnedState(state pbtask.TaskState) bool {
	_, ok := resMgrOwnedTaskStates[state]
	if ok {
		return true
	}
	return false
}

// IsMesosOwnedState returns true if the task state indicates that the task
// is present in mesos.
func IsMesosOwnedState(state pbtask.TaskState) bool {
	_, ok := mesosOwnedTaskStates[state]
	if ok {
		return true
	}
	return false
}

// Task in the cache.
type Task interface {
	// Identifier of the task.
	ID() uint32

	// Job identifier the task belongs to.
	JobID() *peloton.JobID

	// GetRuntime returns the task run time
	GetRuntime(ctx context.Context) (*pbtask.RuntimeInfo, error)

	// GetCacheRuntime returns the task run time stored in the cache.
	// It returns nil if the is no runtime in the cache.
	GetCacheRuntime() *pbtask.RuntimeInfo

	// GetLabels returns the task labels
	GetLabels(ctx context.Context) ([]*peloton.Label, error)

	// CurrentState of the task.
	CurrentState() TaskStateVector

	// GoalState of the task.
	GoalState() TaskStateVector

	// StateSummary of the task.
	StateSummary() TaskStateSummary

	// TerminationStatus of the task.
	TerminationStatus() *pbtask.TerminationStatus
}

// TaskStateVector defines the state of a task.
// This encapsulates both the actual state and the goal state.
type TaskStateVector struct {
	State         pbtask.TaskState
	ConfigVersion uint64
	MesosTaskID   *mesos.TaskID
}

type TaskStateSummary struct {
	CurrentState pbtask.TaskState
	GoalState    pbtask.TaskState
	HealthState  pbtask.HealthState
}

// newTask creates a new cache task object
func newTask(jobID *peloton.JobID, id uint32, jobFactory *jobFactory, jobType pbjob.JobType) *task {
	task := &task{
		jobID:      jobID,
		id:         id,
		jobType:    jobType,
		jobFactory: jobFactory,
	}

	return task
}

// taskConfigCache is the structure which defines the
// subset of task configuration to be stored in the cache
type taskConfigCache struct {
	configVersion uint64           // the current configuration version
	labels        []*peloton.Label // task labels
	revocable     bool             // whether task uses revocable resources
}

// task structure holds the information about a given task in the cache.
type task struct {
	sync.RWMutex // Mutex to acquire before accessing any task information in cache

	jobID   *peloton.JobID // Parent job identifier
	id      uint32         // instance identifier
	jobType pbjob.JobType  // Parent job type

	jobFactory *jobFactory // Pointer to the parent job factory object

	runtime *pbtask.RuntimeInfo // task runtime information

	config *taskConfigCache // task configuration information

	initializedAt time.Time // Task intialization timestamp
}

func (t *task) ID() uint32 {
	return t.id
}

func (t *task) JobID() *peloton.JobID {
	return t.jobID
}

// validateMesosTaskID validates whether newRunID is greater than current runID,
// since each restart/update for a task's runID is monotonically incremented.
func validateMesosTaskID(mesosTaskID, prevMesosTaskID string) bool {
	// TODO: remove this check, post mesostaskID migration.
	if len(mesosTaskID) > 2*uuidLength && len(prevMesosTaskID) > 2*uuidLength {
		return true
	}

	var newRunID, currentRunID uint64
	var err error

	if newRunID, err = util.ParseRunID(mesosTaskID); err != nil {
		return false
	}
	// TODO: remove prevMesosTaskID len check post mesostaskID migration
	if currentRunID, err = util.ParseRunID(prevMesosTaskID); err != nil {
		return len(prevMesosTaskID) > 2*uuidLength || false
	}
	return newRunID >= currentRunID
}

// validateState returns true if the state transition from the previous
// task runtime to the current one is valid.
func (t *task) validateState(newRuntime *pbtask.RuntimeInfo) bool {
	currentRuntime := t.runtime

	if newRuntime == nil {
		// no runtime is invalid
		return false
	}

	// if current goal state is deleted, it cannot be overwritten
	// till the desired configuration version also changes
	if currentRuntime.GetGoalState() == pbtask.TaskState_DELETED &&
		newRuntime.GetGoalState() != currentRuntime.GetGoalState() {
		if currentRuntime.GetDesiredConfigVersion() == newRuntime.GetDesiredConfigVersion() {
			return false
		}
	}

	if newRuntime.GetMesosTaskId() != nil {
		if currentRuntime.GetMesosTaskId().GetValue() !=
			newRuntime.GetMesosTaskId().GetValue() {
			// Validate post migration, new runid is greater than previous one
			if !validateMesosTaskID(newRuntime.GetMesosTaskId().GetValue(),
				currentRuntime.GetMesosTaskId().GetValue()) {
				return false
			}

			// mesos task id has changed
			if newRuntime.GetState() == pbtask.TaskState_INITIALIZED {
				return true
			}
			return false
		}
	}

	// desired mesos task id should not have runID decrease at
	// any time
	if newRuntime.GetDesiredMesosTaskId() != nil &&
		!validateMesosTaskID(newRuntime.GetDesiredMesosTaskId().GetValue(),
			currentRuntime.GetDesiredMesosTaskId().GetValue()) {
		return false
	}

	// if state update is not requested, then return true
	if newRuntime.GetState() == currentRuntime.GetState() {
		return true
	}

	//TBD replace if's with more structured checks

	if util.IsPelotonStateTerminal(currentRuntime.GetState()) {
		// cannot overwrite terminal state without changing the mesos task id
		return false
	}

	if IsMesosOwnedState(newRuntime.GetState()) {
		// update from mesos eventstream is ok from mesos states, resource manager states
		// and from INITIALIZED and LAUNCHED states.
		if IsMesosOwnedState(currentRuntime.GetState()) || IsResMgrOwnedState(currentRuntime.GetState()) {
			return true
		}

		if currentRuntime.GetState() == pbtask.TaskState_INITIALIZED || currentRuntime.GetState() == pbtask.TaskState_LAUNCHED {
			return true
		}

		// Update from KILLING state to only terminal states is allowed
		if util.IsPelotonStateTerminal(newRuntime.GetState()) && currentRuntime.GetState() == pbtask.TaskState_KILLING {
			return true
		}
	}

	if IsResMgrOwnedState(newRuntime.GetState()) {
		// update from resource manager evenstream is ok from resource manager states or INITIALIZED state
		if IsResMgrOwnedState(currentRuntime.GetState()) {
			return true
		}

		if currentRuntime.GetState() == pbtask.TaskState_INITIALIZED {
			return true
		}
	}

	if newRuntime.GetState() == pbtask.TaskState_LAUNCHED {
		// update to LAUNCHED state from resource manager states and INITIALIZED state is ok
		if IsResMgrOwnedState(currentRuntime.GetState()) {
			return true
		}
		if currentRuntime.GetState() == pbtask.TaskState_INITIALIZED {
			return true
		}
	}

	if newRuntime.GetState() == pbtask.TaskState_KILLING {
		// update to KILLING state from any non-terminal state is ok
		return true
	}

	// any other state transition is invalid
	return false
}

// updateConfig is used to update the config in the task cache.
// It should be called with the write task lock held.
func (t *task) updateConfig(ctx context.Context, configVersion uint64) error {
	if t.config != nil && t.config.configVersion == configVersion {
		// no change, do nothing
		return nil
	}

	taskConfig, _, err := t.jobFactory.taskConfigV2Ops.GetTaskConfig(
		ctx, t.jobID, t.id, configVersion)
	if err != nil {
		return err
	}

	t.config = &taskConfigCache{
		configVersion: configVersion,
		labels:        taskConfig.GetLabels(),
		revocable:     taskConfig.GetRevocable(),
	}
	return nil
}

// cleanTaskCache cleans the task runtime and labels in the task cache.
// It should be called with the write task lock held.
func (t *task) cleanTaskCache() {
	t.runtime = nil
	t.config = nil
}

// createTask creates the task runtime in DB and cache
func (t *task) createTask(ctx context.Context, runtime *pbtask.RuntimeInfo, owner string) error {
	var runtimeCopy *pbtask.RuntimeInfo
	var labelsCopy []*peloton.Label

	// notify listeners after dropping the lock
	defer func() {
		t.jobFactory.notifyTaskRuntimeChanged(
			t.JobID(),
			t.ID(),
			t.jobType,
			runtimeCopy,
			labelsCopy,
		)
	}()
	t.Lock()
	defer t.Unlock()

	// First create the runtime in DB and then store in the cache if DB create is successful
	err := t.jobFactory.taskStore.CreateTaskRuntime(
		ctx,
		t.jobID,
		t.id,
		runtime,
		owner,
		t.jobType)
	if err != nil {
		t.cleanTaskCache()
		return err
	}

	// Update the config in cache only after creating the runtime in DB
	// so that cache is not populated if runtime write fails.
	err = t.updateConfig(ctx, runtime.GetConfigVersion())
	if err != nil {
		t.cleanTaskCache()
		return err
	}
	t.logStateTransitionMetrics(runtime)

	t.runtime = runtime
	runtimeCopy = proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	labelsCopy = t.copyLabelsInCache()
	return nil
}

// patchTask patches diff to the existing runtime cache
// in task and persists to DB.
func (t *task) patchTask(ctx context.Context, diff jobmgrcommon.RuntimeDiff) error {
	if diff == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"unexpected nil diff")
	}

	if _, ok := diff[jobmgrcommon.RevisionField]; ok {
		return yarpcerrors.InvalidArgumentErrorf(
			"unexpected Revision field in diff")
	}

	var runtimeCopy *pbtask.RuntimeInfo
	var labelsCopy []*peloton.Label

	// notify listeners after dropping the lock
	defer func() {
		t.jobFactory.notifyTaskRuntimeChanged(
			t.JobID(),
			t.ID(),
			t.jobType,
			runtimeCopy,
			labelsCopy,
		)
	}()
	t.Lock()
	defer t.Unlock()

	// reload cache if there is none
	if t.runtime == nil {
		// fetch runtime from db if not present in cache
		err := t.updateRuntimeFromDB(ctx)
		if err != nil {
			return err
		}
	}

	// make a copy of runtime since patch() would update runtime in place
	newRuntime := *t.runtime
	newRuntimePtr := &newRuntime
	if err := patch(newRuntimePtr, diff); err != nil {
		return err
	}

	// validate if the patched runtime is valid,
	// if not ignore the diff, since the runtime has already been updated by
	// other threads and the change in diff is no longer valid
	if !t.validateState(newRuntimePtr) {
		return nil
	}

	t.updateRevision(newRuntimePtr)

	err := t.jobFactory.taskStore.UpdateTaskRuntime(
		ctx,
		t.jobID,
		t.id,
		newRuntimePtr,
		t.jobType)
	if err != nil {
		// clean the runtime in cache on DB write failure
		t.cleanTaskCache()
		return err
	}

	err = t.updateConfig(ctx, newRuntimePtr.GetConfigVersion())
	if err != nil {
		t.cleanTaskCache()
		return err
	}
	t.logStateTransitionMetrics(newRuntimePtr)

	// Store the new runtime in cache
	t.runtime = newRuntimePtr
	runtimeCopy = proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	labelsCopy = t.copyLabelsInCache()
	return nil
}

// compareAndSetTask replaces the existing task runtime in DB and cache.
// It uses RuntimeInfo.Revision.Version for concurrency control, and it would
// update RuntimeInfo.Revision.Version automatically upon success.
// Caller should not manually modify the value of RuntimeInfo.Revision.Version.
func (t *task) compareAndSetTask(
	ctx context.Context,
	runtime *pbtask.RuntimeInfo,
	jobType pbjob.JobType,
) (*pbtask.RuntimeInfo, error) {
	if runtime == nil {
		return nil, yarpcerrors.InvalidArgumentErrorf(
			"unexpected nil runtime")
	}

	var runtimeCopy *pbtask.RuntimeInfo
	var labelsCopy []*peloton.Label

	// notify listeners after dropping the lock
	defer func() {
		t.jobFactory.notifyTaskRuntimeChanged(
			t.JobID(),
			t.ID(),
			jobType,
			runtimeCopy,
			labelsCopy,
		)
	}()

	t.Lock()
	defer t.Unlock()

	// reload cache if there is none
	if t.runtime == nil {
		// fetch runtime from db if not present in cache
		err := t.updateRuntimeFromDB(ctx)
		if err != nil {
			return nil, err
		}
	}

	// validate that the input version is the same as the version in cache
	if runtime.GetRevision().GetVersion() !=
		t.runtime.GetRevision().GetVersion() {
		t.cleanTaskCache()
		return nil, jobmgrcommon.UnexpectedVersionError
	}

	// validate if the patched runtime is valid,
	// if not ignore the diff, since the runtime has already been updated by
	// other threads and the change in diff is no longer valid
	if !t.validateState(runtime) {
		return nil, nil
	}

	// bump up the changelog version
	t.updateRevision(runtime)

	// update the DB
	err := t.jobFactory.taskStore.UpdateTaskRuntime(
		ctx,
		t.jobID,
		t.id,
		runtime,
		jobType)
	if err != nil {
		// clean the runtime in cache on DB write failure
		t.cleanTaskCache()
		return nil, err
	}

	err = t.updateConfig(ctx, runtime.GetConfigVersion())
	if err != nil {
		t.cleanTaskCache()
		return nil, err
	}
	t.logStateTransitionMetrics(runtime)

	// Store the new runtime in cache
	t.runtime = runtime
	runtimeCopy = proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	labelsCopy = t.copyLabelsInCache()
	return runtimeCopy, nil
}

// deleteTask deletes any state, if any, stored by the task and let the
// listeners know that the task is being deleted.
func (t *task) deleteTask() {
	// There is no state to clean up as of now.
	// If the goal state was set to DELETED, then let the
	// listeners know that the task has been deleted.

	var runtimeCopy *pbtask.RuntimeInfo
	var labelsCopy []*peloton.Label

	// notify listeners after dropping the lock
	defer func() {
		if runtimeCopy != nil {
			t.jobFactory.notifyTaskRuntimeChanged(
				t.jobID,
				t.id,
				t.jobType,
				runtimeCopy,
				labelsCopy,
			)
		}
	}()

	t.RLock()
	defer t.RUnlock()

	if t.runtime == nil {
		return
	}

	if t.runtime.GetGoalState() != pbtask.TaskState_DELETED {
		return
	}

	runtimeCopy = proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	runtimeCopy.State = pbtask.TaskState_DELETED
	labelsCopy = t.copyLabelsInCache()
}

// replaceTask replaces runtime and config in cache with runtime input.
// forceReplace would decide whether to check version when replacing the runtime and config,
// it should only be used in Refresh for debugging purpose
func (t *task) replaceTask(
	runtime *pbtask.RuntimeInfo,
	taskConfig *pbtask.TaskConfig,
	forceReplace bool) error {
	if runtime == nil || runtime.GetRevision() == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"replaceTask expects a non-nil runtime with non-nil Revision")
	}

	var runtimeCopy *pbtask.RuntimeInfo
	var labelsCopy []*peloton.Label

	// notify listeners after dropping the lock
	defer func() {
		t.jobFactory.notifyTaskRuntimeChanged(
			t.JobID(),
			t.ID(),
			t.jobType,
			runtimeCopy,
			labelsCopy,
		)
	}()

	t.Lock()
	defer t.Unlock()

	// update the cache if,
	// 1. it is a force replace, or
	// 2. there is no existing runtime cache,
	// 3. new runtime has a higher version number than the existing
	if forceReplace ||
		t.runtime == nil ||
		runtime.GetRevision().GetVersion() > t.runtime.GetRevision().GetVersion() {
		// Update task config and runtime
		t.config = &taskConfigCache{
			configVersion: runtime.GetConfigVersion(),
			labels:        taskConfig.GetLabels(),
		}
		t.runtime = runtime
		runtimeCopy = proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
		labelsCopy = t.copyLabelsInCache()
	}

	return nil
}

func (t *task) updateRevision(runtime *pbtask.RuntimeInfo) {
	if runtime.Revision == nil {
		// should never enter here
		log.WithField("job_id", t.jobID).
			WithField("instance_id", t.id).
			Error("runtime revision is nil in update tasks")
		runtime.Revision = &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
		}
	}

	// bump up the runtime version
	runtime.Revision.Version++
	runtime.Revision.UpdatedAt = uint64(time.Now().UnixNano())
}

// updateRuntimeFromDB is a helper function to load the runtime from the DB
// into the cache. This has to be called with the write lock held.
func (t *task) updateRuntimeFromDB(ctx context.Context) error {
	if t.runtime != nil {
		return nil
	}

	runtime, err := t.jobFactory.taskStore.GetTaskRuntime(ctx, t.jobID, t.id)
	if err != nil {
		return err
	}
	t.runtime = runtime
	return nil
}

func (t *task) GetRuntime(ctx context.Context) (*pbtask.RuntimeInfo, error) {
	t.Lock()
	defer t.Unlock()

	if t.runtime == nil {
		// If runtime is not present in the cache, then fetch from the DB
		err := t.updateRuntimeFromDB(ctx)
		if err != nil {
			return nil, err
		}
	}

	runtime := proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	return runtime, nil
}

func (t *task) GetCacheRuntime() *pbtask.RuntimeInfo {
	t.RLock()
	defer t.RUnlock()

	if t.runtime == nil {
		return nil
	}

	runtime := proto.Clone(t.runtime).(*pbtask.RuntimeInfo)
	return runtime
}

// copyLabelsInCache returns a copy of the labels in the cache.
// At least a read lock should be held before invoking the API.
func (t *task) copyLabelsInCache() []*peloton.Label {
	var labelsCopy []*peloton.Label

	if t.config == nil {
		return labelsCopy
	}

	for _, label := range t.config.labels {
		var l peloton.Label

		l = *label
		labelsCopy = append(labelsCopy, &l)
	}
	return labelsCopy
}

func (t *task) GetLabels(ctx context.Context) ([]*peloton.Label, error) {
	t.Lock()
	defer t.Unlock()

	if t.runtime == nil {
		err := t.updateRuntimeFromDB(ctx)
		if err != nil {
			return nil, err
		}
	}

	if t.config == nil {
		err := t.updateConfig(ctx, t.runtime.GetConfigVersion())
		if err != nil {
			return nil, err
		}
	}

	return t.copyLabelsInCache(), nil
}

func (t *task) CurrentState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
		MesosTaskID:   t.runtime.GetMesosTaskId(),
	}
}

func (t *task) GoalState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
		MesosTaskID:   t.runtime.GetDesiredMesosTaskId(),
	}
}

func (t *task) StateSummary() TaskStateSummary {
	t.RLock()
	defer t.RUnlock()

	return TaskStateSummary{
		CurrentState: t.runtime.GetState(),
		GoalState:    t.runtime.GetGoalState(),
		HealthState:  t.runtime.GetHealthy(),
	}
}

func (t *task) TerminationStatus() *pbtask.TerminationStatus {
	t.RLock()
	defer t.RUnlock()

	return t.runtime.GetTerminationStatus()
}

func (t *task) logStateTransitionMetrics(runtime *pbtask.RuntimeInfo) {
	if runtime.GetState() == pbtask.TaskState_INITIALIZED {
		t.initializedAt = time.Now()
		return
	}

	if t.runtime != nil && t.runtime.GetState() == runtime.GetState() {
		// Ignore if there is no change in state
		return
	}
	if t.initializedAt.IsZero() {
		// If initialization time is not set then we cannot calculate
		// time-to-assign or time-to-run
		return
	}
	revocable := false
	if t.config != nil {
		revocable = t.config.revocable
	}

	tt := time.Since(t.initializedAt)
	switch runtime.GetState() {
	case pbtask.TaskState_LAUNCHED:
		if revocable {
			t.jobFactory.taskMetrics.TimeToAssignRevocable.Record(tt)
		} else {
			t.jobFactory.taskMetrics.TimeToAssignNonRevocable.Record(tt)
		}
	case pbtask.TaskState_RUNNING:
		if revocable {
			t.jobFactory.taskMetrics.TimeToRunRevocable.Record(tt)
		} else {
			t.jobFactory.taskMetrics.TimeToRunNonRevocable.Record(tt)
		}
	}
}

// GetResourceManagerProcessingStates returns the active task states in Resource Manager
func GetResourceManagerProcessingStates() []string {
	states := make([]string, len(resMgrOwnedTaskStates))
	i := 0
	for k := range resMgrOwnedTaskStates {
		states[i] = k.String()
		i++
	}
	return states
}
