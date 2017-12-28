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
)

// JobAction that can be given to the RunJobAction method.
type JobAction string

// Actions available to be performed on the job.
const (
	JobNoAction    JobAction = "no_action"
	JobCreateTasks JobAction = "create_tasks"
)

// Job tracked by the system, serving as a best effort view of what's stored
// in the database.
type Job interface {
	// ID of the job.
	ID() *peloton.JobID

	// GetTask from the task id.
	GetTask(id uint32) Task

	// Get All Tasks returns all tasks for the job
	GetAllTasks() map[uint32]Task

	// RunAction on the job. Returns a bool representing if the job should
	// be rescheduled by the goalstate engine and an error representing the
	// result of the action.
	RunAction(ctx context.Context, action JobAction) (bool, error)

	// GetJobRuntime returns the runtime of the job
	GetJobRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error)

	// ClearJobRuntime sets the cached job runtime to nil
	ClearJobRuntime()

	// SetLastDelay sets the last delay value
	SetLastDelay(delay time.Duration)

	// GetLastDelay gets the last delay value
	GetLastDelay() time.Duration
}

func newJob(id *peloton.JobID, m *manager) *job {
	return &job{
		queueItemMixin: newQueueItemMixing(),
		id:             id,
		m:              m,
		tasks:          map[uint32]*task{},
		lastDelay:      0,
	}
}

// config of a job. This encapsulate a subset of the job config, needed
// for processing actions.
type jobConfig struct {
	instanceCount uint32
	respoolID     *peloton.ResourcePoolID
	sla           *pb_job.SlaConfig
}

type job struct {
	sync.RWMutex
	queueItemMixin

	id        *peloton.JobID
	config    *jobConfig
	m         *manager
	runtime   *pb_job.RuntimeInfo
	lastDelay time.Duration

	// currentScheduledTasks is used to track number of tasks scheduled.
	// It is used to ensure that number of tasks scheduled does not
	// exceed MaximumRunningInstances in the job SLA configuration
	currentScheduledTasks uint32

	// TODO: Use list as we expect to always track tasks 0..n-1.
	tasks map[uint32]*task
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
		j.config = &jobConfig{
			instanceCount: jobInfo.Config.InstanceCount,
			sla:           jobInfo.Config.Sla,
			respoolID:     jobInfo.Config.RespoolID,
		}
	}
}

func (j *job) getConfig() (*jobConfig, error) {
	j.RLock()
	defer j.RUnlock()

	if j.config == nil {
		return nil, errors.New("missing job config in goal state")
	}
	return j.config, nil
}
