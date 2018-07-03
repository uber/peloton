package cached

import (
	"context"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/util"
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

	// CreateRuntime creates the task runtime in DB and cache
	CreateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, owner string) error

	// PatchRuntime patches diff to the existing runtime cache
	// in task and persists to DB.
	PatchRuntime(ctx context.Context, diff RuntimeDiff) error

	// ReplaceRuntime replaces cache with runtime
	// forceReplace would decide whether to check version when replacing the runtime
	// forceReplace is used for Refresh, which is for debugging only
	ReplaceRuntime(runtime *pbtask.RuntimeInfo, forceReplace bool) error

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

// validateRunID validates whether newRunID is greater than current runID,
// since each restart/upgrade for a task's runID is monotonically incremented.
func validateRunID(mesosTaskID, prevMesosTaskID string) bool {
	// TODO: remove this check, post mesostaskID migration.
	if len(mesosTaskID) > 2*uuidLength && len(prevMesosTaskID) > 2*uuidLength {
		return true
	}

	var newRunID, currentRunID int
	var err error

	if newRunID, err = util.ParseRunID(mesosTaskID); err != nil {
		return false
	}
	// TODO: remove prevMesosTaskID len check post mesostaskID migration
	if currentRunID, err = util.ParseRunID(prevMesosTaskID); err != nil {
		return len(prevMesosTaskID) > 2*uuidLength || false
	}
	return newRunID > currentRunID
}

// validateState returns true if the state transition from the previous
// task runtime to the current one is valid.
func (t *task) validateState(newRuntime *pbtask.RuntimeInfo) bool {
	currentRuntime := t.runtime

	if newRuntime == nil {
		// no runtime is invalid
		return false
	}

	if newRuntime.GetMesosTaskId() != nil {
		if currentRuntime.GetMesosTaskId().GetValue() !=
			newRuntime.GetMesosTaskId().GetValue() {
			// Validate post migration, new runid is greater than previous one
			if !validateRunID(newRuntime.GetMesosTaskId().GetValue(),
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

	// mesos task id remains the same as before

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

func (t *task) CreateRuntime(ctx context.Context, runtime *pbtask.RuntimeInfo, owner string) error {
	t.Lock()
	defer t.Unlock()

	// fetch job configuration to get job type
	jobConfig, err := t.jobFactory.GetJob(t.JobID()).GetConfig(ctx)
	if err != nil {
		return err
	}

	// First create the runtime in DB and then store in the cache if DB create is successful
	err = t.jobFactory.taskStore.CreateTaskRuntime(
		ctx,
		t.jobID,
		t.id,
		runtime,
		owner,
		jobConfig.GetType())
	if err != nil {
		t.runtime = nil
		return err
	}

	t.runtime = runtime
	t.lastRuntimeUpdateTime = time.Now()
	return nil
}

// PatchRuntime patches diff to the existing runtime cache
// in task and persists to DB.
func (t *task) PatchRuntime(ctx context.Context, diff RuntimeDiff) error {
	if diff == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"unexpected nil diff")
	}

	if _, ok := diff[RevisionField]; ok {
		return yarpcerrors.InvalidArgumentErrorf(
			"unexpected Revision field in diff")
	}

	t.Lock()
	defer t.Unlock()

	// reload cache if there is none
	if t.runtime == nil {
		// fetch runtime from db if not present in cache
		runtime, err := t.jobFactory.taskStore.GetTaskRuntime(ctx, t.jobID, t.id)
		if err != nil {
			return err
		}
		t.runtime = runtime
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

	t.updateRevision()

	jobConfig, err := t.jobFactory.GetJob(t.JobID()).GetConfig(ctx)
	if err != nil {
		return err
	}

	err = t.jobFactory.taskStore.UpdateTaskRuntime(
		ctx,
		t.jobID,
		t.id,
		newRuntimePtr,
		jobConfig.GetType())
	if err != nil {
		// clean the runtime in cache on DB write failure
		t.runtime = nil
		return err
	}
	// Store the new runtime in cache
	t.runtime = newRuntimePtr
	t.lastRuntimeUpdateTime = time.Now()
	return nil
}

// ReplaceRuntime replaces runtime in cache with runtime input.
// forceReplace would decide whether to check version when replacing the runtime,
// it should only be used in Refresh for debugging purpose
func (t *task) ReplaceRuntime(runtime *pbtask.RuntimeInfo, forceReplace bool) error {
	if runtime == nil || runtime.GetRevision() == nil {
		return yarpcerrors.InvalidArgumentErrorf(
			"ReplaceRuntime expects a non-nil runtime with non-nil Revision")
	}

	t.Lock()
	defer t.Unlock()

	// update the cache if,
	// 1. it is a force replace, or
	// 2. there is no existing runtime cache,
	// 3. new runtime has a higher version number than the existing
	if forceReplace ||
		t.runtime == nil ||
		runtime.GetRevision().GetVersion() > t.runtime.GetRevision().GetVersion() {
		t.runtime = runtime
		return nil
	}

	return nil
}

func (t *task) updateRevision() {
	if t.runtime.Revision == nil {
		// should never enter here
		log.WithField("job_id", t.jobID).
			WithField("instance_id", t.id).
			Error("runtime revision is nil in update tasks")
		t.runtime.Revision = &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
		}
	}

	// bump up the runtime version
	t.runtime.Revision.Version++
	t.runtime.Revision.UpdatedAt = uint64(time.Now().UnixNano())
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
