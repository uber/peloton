package tracked

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

// JobAction that can be given to the RunJobAction method.
type JobAction string

// Actions available to be performed on the job.
const (
	JobNoAction      JobAction = "noop"
	JobCreateTasks   JobAction = "create_tasks"
	JobKill          JobAction = "job_kill"
	JobUntrackAction JobAction = "untrack"
)

// Job tracked by the system, serving as a best effort view of what's stored
// in the database.
type Job interface {
	// ID of the job.
	ID() *peloton.JobID

	// GetTask from the task id.
	GetTask(id uint32) Task

	// GetAllTasks returns all tasks for the job
	GetAllTasks() map[uint32]Task

	// GetTasksNum returns total number of tasks in cache for the job
	GetTasksNum() uint32

	// RunAction on the job. Returns a bool representing if the job should
	// be rescheduled by the goalstate engine and an error representing the
	// result of the action.
	RunAction(ctx context.Context, action JobAction) (bool, error)

	// GetConfig fetches the job config stored in the cache
	GetConfig() (*JobConfig, error)

	// GetJobRuntime returns the runtime of the job
	GetJobRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error)

	// ClearJobRuntime sets the cached job runtime to nil
	ClearJobRuntime()

	// SetLastDelay sets the last delay value
	SetLastDelay(delay time.Duration)

	// GetLastDelay gets the last delay value
	GetLastDelay() time.Duration

	// Set the update times for the job
	SetTaskUpdateTime(t *float64)

	// TODO JobRuntimeUpdater needs to be moved away from cache into
	// a job specific structure.
	// JobRuntimeUpdater updates the runtime of the job based on the task states
	JobRuntimeUpdater(ctx context.Context) (bool, error)
}

func newJob(id *peloton.JobID, m *manager) *job {
	return &job{
		queueItemMixin:   newQueueItemMixing(),
		id:               id,
		m:                m,
		tasks:            map[uint32]*task{},
		initializedTasks: map[uint32]*task{},
		lastDelay:        0,
	}
}

// JobConfig of a job. This encapsulate a subset of the job config, needed
// for processing actions.
// TODO this structure needs to go away and pb_job.JobConfig should be
// used to store the job configuration in the cache as well.
type JobConfig struct {
	instanceCount uint32
	respoolID     *peloton.ResourcePoolID
	sla           *pb_job.SlaConfig
	jobType       pb_job.JobType
}

type job struct {
	sync.RWMutex
	queueItemMixin

	id        *peloton.JobID
	config    *JobConfig
	m         *manager
	runtime   *pb_job.RuntimeInfo
	lastDelay time.Duration

	// map of tasks in initialized state
	initializedTasks map[uint32]*task

	tasks map[uint32]*task

	firstTaskUpdateTime float64
	lastTaskUpdateTime  float64
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) ClearJobRuntime() {
	j.Lock()
	defer j.Unlock()
	j.runtime = nil
}

func (j *job) SetLastDelay(delay time.Duration) {
	j.Lock()
	defer j.Unlock()
	j.lastDelay = delay
}

func (j *job) GetLastDelay() time.Duration {
	j.RLock()
	defer j.RUnlock()
	return j.lastDelay
}

func (j *job) RunAction(ctx context.Context, action JobAction) (bool, error) {
	defer j.m.mtx.scope.Tagged(map[string]string{"action": string(action)}).Timer("run_duration").Start().Stop()

	switch action {
	case JobNoAction:
		return false, nil
	case JobCreateTasks:
		return j.createTasks(ctx)
	case JobKill:
		return j.killJob(ctx)
	case JobUntrackAction:
		j.m.clearJob(j)
		return false, nil
	default:
		return false, fmt.Errorf("unsupported job action %v", action)
	}
}

func (j *job) GetTask(id uint32) Task {
	j.RLock()
	defer j.RUnlock()

	if t, ok := j.tasks[id]; ok {
		return t
	}

	return nil
}

func (j *job) GetAllTasks() map[uint32]Task {
	j.RLock()
	defer j.RUnlock()
	taskMap := make(map[uint32]Task)
	for k, v := range j.tasks {
		taskMap[k] = v
	}
	return taskMap
}

func (j *job) GetTasksNum() uint32 {
	j.RLock()
	defer j.RUnlock()

	return uint32(len(j.tasks))
}

func (j *job) setTask(id uint32, runtime *pb_task.RuntimeInfo) *task {
	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTask(j, id)
		j.tasks[id] = t
	}

	t.UpdateRuntime(runtime)
	return t
}

// setTasks creates the task provided in the input map (if not already present),
// updates the runtime of each task, and returns a map of the tasks.
func (j *job) setTasks(runtimes map[uint32]*pb_task.RuntimeInfo) map[uint32]*task {
	j.Lock()
	defer j.Unlock()

	taskInJobMap := make(map[uint32]*task)
	for id, runtime := range runtimes {
		t, ok := j.tasks[id]
		if !ok {
			t = newTask(j, id)
			j.tasks[id] = t
		}
		t.UpdateRuntime(runtime)
		taskInJobMap[id] = t
	}
	return taskInJobMap
}

func (j *job) GetJobRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error) {
	j.Lock()
	defer j.Unlock()

	if j.runtime != nil {
		return j.runtime, nil
	}
	var err error
	j.runtime, err = j.m.jobStore.GetJobRuntime(ctx, j.ID())
	return j.runtime, err
}

func (j *job) updateRuntime(jobInfo *pb_job.JobInfo) {
	j.Lock()
	defer j.Unlock()

	j.runtime = jobInfo.GetRuntime()
	if jobInfo.GetConfig() != nil {
		j.config = &JobConfig{
			instanceCount: jobInfo.Config.InstanceCount,
			sla:           jobInfo.Config.Sla,
			respoolID:     jobInfo.Config.RespoolID,
			jobType:       jobInfo.Config.GetType(),
		}
	}
}

func (j *job) GetConfig() (*JobConfig, error) {
	j.RLock()
	defer j.RUnlock()

	var jobConfig JobConfig
	if j.config == nil {
		return nil, errors.New("missing job config in goal state")
	}

	jobConfig = *j.config
	if j.config.sla != nil {
		*jobConfig.sla = *j.config.sla
	}
	if j.config.respoolID != nil {
		*jobConfig.respoolID = *j.config.respoolID
	}
	return &jobConfig, nil
}

func (j *job) clearInitializedTaskMap() {
	j.Lock()
	defer j.Unlock()

	j.initializedTasks = map[uint32]*task{}
}

func (j *job) addTaskToInitializedTaskMap(t *task) {
	j.Lock()
	defer j.Unlock()

	j.initializedTasks[t.ID()] = t
}

func (j *job) SetTaskUpdateTime(t *float64) {
	j.Lock()
	defer j.Unlock()

	if j.firstTaskUpdateTime == 0 {
		j.firstTaskUpdateTime = *t
	}

	j.lastTaskUpdateTime = *t
}

func (j *job) getFirstTaskUpdateTime() float64 {
	j.RLock()
	j.RUnlock()

	return j.firstTaskUpdateTime
}

func (j *job) getLastTaskUpdateTime() float64 {
	j.RLock()
	j.RUnlock()

	return j.lastTaskUpdateTime
}

// killJob will stop all tasks in the job.
// If the action needs to be rescheduled, the function returns true.
// In general, if an error is encountered while stopping all tasks,
// the action needs to be rescheduled.
func (j *job) killJob(ctx context.Context) (bool, error) {
	jobConfig, err := j.m.jobStore.GetJobConfig(ctx, j.id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to fetch job config to kill a job")
		return true, err
	}

	instanceCount := jobConfig.GetInstanceCount()
	instRange := &pb_task.InstanceRange{
		From: 0,
		To:   instanceCount,
	}
	runtimes, err := j.m.taskStore.GetTaskRuntimesForJobByRange(ctx, j.id, instRange)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to fetch task runtimes to kill a job")
		return true, err
	}

	// Update task runtimes to kill task
	updatedRuntimes := make(map[uint32]*pb_task.RuntimeInfo)
	for instanceID, runtime := range runtimes {
		if runtime.GetGoalState() == pb_task.TaskState_KILLED || util.IsPelotonStateTerminal(runtime.GetState()) {
			continue
		}
		runtime.GoalState = pb_task.TaskState_KILLED
		runtime.Message = "Task stop API request"
		runtime.Reason = ""
		updatedRuntimes[instanceID] = runtime
	}

	err = j.m.UpdateTaskRuntimes(ctx, j.id, updatedRuntimes, UpdateAndSchedule)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to update task runtimes to kill a job")
		return true, err
	}

	// Get job runtime and update job state to killing
	jobRuntime, err := j.m.jobStore.GetJobRuntime(ctx, j.id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to get job runtime during job kill")
		return true, err
	}
	jobState := pb_job.JobState_KILLING

	// If all instances have not been created, and all created instances are already killed,
	// then directly update the job state to KILLED.
	if len(updatedRuntimes) == 0 && jobRuntime.GetState() == pb_job.JobState_INITIALIZED && j.GetTasksNum() < instanceCount {
		jobState = pb_job.JobState_KILLED
		for _, runtime := range runtimes {
			if !util.IsPelotonStateTerminal(runtime.GetState()) {
				jobState = pb_job.JobState_KILLING
				break
			}
		}
	}
	jobRuntime.State = jobState

	err = j.m.jobStore.UpdateJobRuntime(ctx, j.id, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to update job runtime during job kill")
		return true, err
	}

	log.WithField("job_id", j.id.GetValue()).
		Info("initiated kill of all tasks in the job")
	return false, nil
}
