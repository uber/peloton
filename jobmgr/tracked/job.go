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

	"code.uber.internal/infra/peloton/common/taskconfig"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"

	log "github.com/sirupsen/logrus"
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

	// TODO: Use list as we expect to always track tasks 0..n-1.
	tasks map[uint32]*task
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) ClearJobRuntime() {
	j.runtime = nil
}

func (j *job) SetLastDelay(delay time.Duration) {
	j.lastDelay = delay
}

func (j *job) GetLastDelay() time.Duration {
	return j.lastDelay
}

func (j *job) RunAction(ctx context.Context, action JobAction) (bool, error) {
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

	t.updateRuntime(runtime)
	return t
}

func (j *job) GetJobRuntime(ctx context.Context) (*pb_job.RuntimeInfo, error) {
	j.RLock()
	defer j.RUnlock()

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

func (j *job) createTasks(ctx context.Context) (bool, error) {
	var err error
	var jobConfig *pb_job.JobConfig
	var taskInfos map[uint32]*pb_task.TaskInfo
	var jobRuntime *pb_job.RuntimeInfo

	startAddTaskTime := time.Now()

	jobConfig, err = j.m.jobStore.GetJobConfig(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to get job config")
		return true, err
	}

	instances := jobConfig.InstanceCount

	// First create task configs
	if err = j.m.taskStore.CreateTaskConfigs(ctx, j.id, jobConfig); err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to create task configs")
		return true, err
	}

	// Get task runtimes.
	taskInfos, err = j.m.taskStore.GetTasksForJob(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id).
			Error("failed to get tasks for job")
		return true, err
	}

	if len(taskInfos) == 0 {
		// New job being created
		err = j.createAndEnqueueTasks(ctx, jobConfig)
	} else {
		// Recover error in previous creation of job
		err = j.recoverTasks(ctx, jobConfig, taskInfos)
	}

	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		return true, err
	}

	// Get job runtime and update job state to pending
	jobRuntime, err = j.m.jobStore.GetJobRuntime(ctx, j.id)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to get job runtime")
		return true, err
	}

	jobRuntime.State = pb_job.JobState_PENDING
	err = j.m.jobStore.UpdateJobRuntime(ctx, j.id, jobRuntime)
	if err != nil {
		j.m.mtx.jobMetrics.JobCreateFailed.Inc(1)
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to update job runtime")
		return true, err
	}

	j.m.mtx.jobMetrics.JobCreate.Inc(1)
	log.WithField("job_id", j.id.GetValue()).
		WithField("instance_count", instances).
		WithField("time_spent", time.Since(startAddTaskTime)).
		Info("all tasks created for job")

	return false, nil
}

func (j *job) recoverTasks(ctx context.Context, jobConfig *pb_job.JobConfig, taskInfos map[uint32]*pb_task.TaskInfo) error {
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		if _, ok := taskInfos[i]; ok {
			if taskInfos[i].GetRuntime().GetState() == pb_task.TaskState_INITIALIZED {
				// Task exists, just send to resource manager
				j.m.SetTask(j.id, i, taskInfos[i].GetRuntime())
			}
			continue
		}

		// Task does not exist in taskStore, create runtime and then send to resource manager
		log.WithField("job_id", j.id.GetValue()).
			WithField("task_instance", i).
			Info("Creating missing task")

		runtime := jobmgr_task.CreateInitializingTask(j.id, i, jobConfig)
		if err := j.m.taskStore.CreateTaskRuntime(ctx, j.id, i, runtime, jobConfig.OwningTeam); err != nil {
			j.m.mtx.taskMetrics.TaskCreateFail.Inc(1)
			log.WithError(err).
				WithField("job_id", j.id.GetValue()).
				WithField("id", i).
				Error("failed to create task")
			return err
		}
		j.m.mtx.taskMetrics.TaskCreate.Inc(1)
		j.m.SetTask(j.id, i, runtime)
	}

	return nil
}

func (j *job) createAndEnqueueTasks(ctx context.Context, jobConfig *pb_job.JobConfig) error {
	instances := jobConfig.InstanceCount

	// Create task runtimes
	tasks := make([]*pb_task.TaskInfo, instances)
	runtimes := make([]*pb_task.RuntimeInfo, instances)
	for i := uint32(0); i < instances; i++ {
		runtime := jobmgr_task.CreateInitializingTask(j.id, i, jobConfig)
		runtimes[i] = runtime
		tasks[i] = &pb_task.TaskInfo{
			JobId:      j.id,
			InstanceId: i,
			Runtime:    runtime,
			Config:     taskconfig.Merge(jobConfig.GetDefaultConfig(), jobConfig.GetInstanceConfig()[i]),
		}
	}

	err := j.m.taskStore.CreateTaskRuntimes(ctx, j.id, runtimes, jobConfig.OwningTeam)
	nTasks := int64(len(tasks))
	if err != nil {
		log.Errorf("Failed to create tasks (%d) for job %v: %v",
			nTasks, j.id.GetValue(), err)
		j.m.mtx.taskMetrics.TaskCreateFail.Inc(nTasks)
		return err
	}
	j.m.mtx.taskMetrics.TaskCreate.Inc(nTasks)

	// Add task to tracked manager in-memory DB
	for i := uint32(0); i < instances; i++ {
		j.setTask(i, runtimes[i])
	}

	// Send tasks to resource manager
	err = jobmgr_task.EnqueueGangs(ctx, tasks, jobConfig, j.m.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", j.id.GetValue()).
			Error("failed to enqueue tasks to RM")
		return err
	}

	return nil
}
