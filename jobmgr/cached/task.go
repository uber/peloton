package cached

import (
	"context"
	"reflect"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/util"
	stringsutil "code.uber.internal/infra/peloton/util/strings"

	log "github.com/sirupsen/logrus"
)

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

	// CreateRuntime creates the task runtime in DB and cache
	CreateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, owner string) error

	// UpdateRuntime updates the task run time in DB and cache
	UpdateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, req UpdateRequest) error

	// GetRunTime returns the task run time
	GetRunTime(ctx context.Context) (*pbtask.RuntimeInfo, error)

	// GetLastRuntimeUpdateTime returns the last time the task runtime was updated.
	GetLastRuntimeUpdateTime() time.Time

	// CurrentState of the task.
	CurrentState() TaskStateVector

	// GoalState of the task.
	GoalState() TaskStateVector
}

// TaskStateVector defines the state of a task.
// This encapsulates both the actual state and the goal state.
type TaskStateVector struct {
	State         pbtask.TaskState
	ConfigVersion uint64
}

// newTask creates a new cache task object
func newTask(jobID *peloton.JobID, id uint32, jobFactory *jobFactory) *task {
	task := &task{
		jobID:      jobID,
		id:         id,
		jobFactory: jobFactory,
	}

	return task
}

// task structure holds the information about a given task in the cache.
type task struct {
	sync.RWMutex // Mutex to acquire before accessing any task information in cache

	jobID *peloton.JobID // Parent job identifier
	id    uint32         // instance identifier

	jobFactory *jobFactory // Pointer to the parent job factory object

	runtime *pbtask.RuntimeInfo // task runtime information

	lastRuntimeUpdateTime time.Time // last time at which the task runtime information was updated
}

func (t *task) ID() uint32 {
	return t.id
}

func (t *task) JobID() *peloton.JobID {
	return t.jobID
}

// validateStateUpdate returns true if the state transition from the previous
// task runtime to the current one is valid.
func (t *task) validateStateUpdate(newRuntime *pbtask.RuntimeInfo) bool {
	currentRuntime := t.runtime

	if newRuntime == nil {
		// no runtime is invalid
		return false
	}

	if newRuntime.GetRevision() != nil {
		// Make sure that not overwriting with old or same version
		if newRuntime.GetRevision().GetVersion() <= currentRuntime.GetRevision().GetVersion() {
			return false
		}
	}

	if newRuntime.GetMesosTaskId() != nil {
		if currentRuntime.GetMesosTaskId().GetValue() != newRuntime.GetMesosTaskId().GetValue() {
			// mesos task id has changed
			if newRuntime.GetState() == pbtask.TaskState_INITIALIZED {
				return true
			}
			return false
		}
	}

	// mesos task id remains the same as before

	// if state update is not requested, then return true
	if newRuntime.GetState() == pbtask.TaskState_UNKNOWN {
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

// mergeRuntime merges the current runtime and the new runtime and returns the merged
// runtime back. The runtime provided as input only contains the fields which
// the caller intends to change and the remaining are kept invalid/nil.
func (t *task) mergeRuntime(newRuntime *pbtask.RuntimeInfo) *pbtask.RuntimeInfo {
	currentRuntime := t.runtime
	runtime := *currentRuntime

	if newRuntime.GetState() != pbtask.TaskState_UNKNOWN {
		runtime.State = newRuntime.GetState()
	}

	if newRuntime.GetMesosTaskId() != nil {
		runtime.MesosTaskId = newRuntime.GetMesosTaskId()
	}

	if stringsutil.ValidateString(newRuntime.GetStartTime()) {
		runtime.StartTime = newRuntime.GetStartTime()
	}

	if stringsutil.ValidateString(newRuntime.GetCompletionTime()) {
		runtime.CompletionTime = newRuntime.GetCompletionTime()
	}

	if stringsutil.ValidateString(newRuntime.GetHost()) {
		runtime.Host = newRuntime.GetHost()
	}

	if len(newRuntime.GetPorts()) > 0 {
		runtime.Ports = newRuntime.GetPorts()
	}

	if newRuntime.GetGoalState() != pbtask.TaskState_UNKNOWN {
		runtime.GoalState = newRuntime.GetGoalState()
	}

	if stringsutil.ValidateString(newRuntime.GetMessage()) {
		runtime.Message = newRuntime.GetMessage()
	}

	if stringsutil.ValidateString(newRuntime.GetReason()) {
		runtime.Reason = newRuntime.GetReason()
	}

	if newRuntime.GetFailureCount() > 0 {
		runtime.FailureCount = newRuntime.GetFailureCount()
	}

	if newRuntime.GetVolumeID() != nil {
		runtime.VolumeID = newRuntime.GetVolumeID()
	}

	if newRuntime.GetConfigVersion() > 0 {
		runtime.ConfigVersion = newRuntime.GetConfigVersion()
	}

	if newRuntime.GetDesiredConfigVersion() > 0 {
		runtime.DesiredConfigVersion = newRuntime.GetDesiredConfigVersion()
	}

	if newRuntime.GetAgentID() != nil {
		runtime.AgentID = newRuntime.GetAgentID()
	}

	if newRuntime.GetPrevMesosTaskId() != nil {
		runtime.PrevMesosTaskId = newRuntime.GetPrevMesosTaskId()
	}

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
	return &runtime
}

func (t *task) CreateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, owner string) error {
	t.Lock()
	defer t.Unlock()

	// First create the runtime in DB and then store in the cache if DB create is successful
	err := t.jobFactory.taskStore.CreateTaskRuntime(ctx, t.jobID, t.id, runtime, owner)
	if err != nil {
		t.runtime = nil
		return err
	}

	t.runtime = runtime
	t.lastRuntimeUpdateTime = time.Now()
	return nil
}

func (t *task) validateAndMergeRuntime(runtime *pbtask.RuntimeInfo) *pbtask.RuntimeInfo {
	// First validate the update
	if !t.validateStateUpdate(runtime) {
		log.WithField("current_revision", t.runtime.GetRevision().GetVersion()).
			WithField("new_revision", runtime.GetRevision().GetVersion()).
			WithField("new_state", runtime.GetState().String()).
			WithField("old_state", t.runtime.GetState().String()).
			WithField("new_goal_state", runtime.GetGoalState().String()).
			WithField("old_goal_state", t.runtime.GetGoalState().String()).
			WithField("job_id", t.jobID.Value).
			WithField("instance_id", t.id).
			Info("failed task state validation")
		return nil
	}

	newRuntime := t.mergeRuntime(runtime)

	// No change in the runtime, ignore the update
	if reflect.DeepEqual(t.runtime, newRuntime) {
		return nil
	}

	return newRuntime
}

// The runtime being passed should only set the fields which the caller intends to change,
// the remaining fields should be left unfilled.
func (t *task) UpdateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, req UpdateRequest) error {
	t.Lock()
	defer t.Unlock()

	newRuntime := runtime

	if req == UpdateCacheAndDB {
		if t.runtime == nil {
			// fetch runtime from db if not present in cache
			runtime, err := t.jobFactory.taskStore.GetTaskRuntime(ctx, t.jobID, t.id)
			if err != nil {
				return err
			}
			t.runtime = runtime
		}
	}

	if t.runtime != nil {
		newRuntime = t.validateAndMergeRuntime(runtime)
		if newRuntime == nil {
			return nil
		}
	}

	if req == UpdateCacheAndDB {
		// First update the runtime in DB
		err := t.jobFactory.taskStore.UpdateTaskRuntime(ctx, t.jobID, t.id, newRuntime)
		if err != nil {
			// clean the runtime in cache on DB write failure
			t.runtime = nil
			return err
		}
	}

	// Store the new runtime in cache
	t.runtime = newRuntime
	t.lastRuntimeUpdateTime = time.Now()
	return nil
}

func (t *task) GetRunTime(ctx context.Context) (*pbtask.RuntimeInfo, error) {
	t.Lock()
	defer t.Unlock()

	if t.runtime == nil {
		// If runtime is not present in the cache, then fetch from the DB
		runtime, err := t.jobFactory.taskStore.GetTaskRuntime(ctx, t.jobID, t.id)
		if err != nil {
			return nil, err
		}
		t.runtime = runtime
	}

	return t.runtime, nil
}

func (t *task) GetLastRuntimeUpdateTime() time.Time {
	t.RLock()
	defer t.RUnlock()
	return t.lastRuntimeUpdateTime
}

func (t *task) CurrentState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetState(),
		ConfigVersion: t.runtime.GetConfigVersion(),
	}
}

func (t *task) GoalState() TaskStateVector {
	t.RLock()
	defer t.RUnlock()

	return TaskStateVector{
		State:         t.runtime.GetGoalState(),
		ConfigVersion: t.runtime.GetDesiredConfigVersion(),
	}
}
