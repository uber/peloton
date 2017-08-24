package tracked

import (
	"context"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// Job tracked by the system, serving as a best effort view of what's stored
// in the database.
type Job interface {
	sync.Locker

	// ID of the job.
	ID() *peloton.JobID

	// GetTask from the task id.
	GetTask(id uint32) Task

	// SetTask to the new runtime info. This will also schedule the task for
	// immediate evaluation.
	SetTask(id uint32, runtime *pb_task.RuntimeInfo)

	// UpdateTask with the new runtime info, by first attempting to persit it.
	// If it fail in persisting the change due to a data race, an AlreadyExists
	// error is returned.
	// If succesfull, this will also schedule the task for immediate evaluation.
	// TODO: Should only take the task runtime, not info.
	UpdateTask(ctx context.Context, id uint32, info *pb_task.TaskInfo) error
}

func newJob(id *peloton.JobID, m *manager) *job {
	return &job{
		id:    id,
		m:     m,
		tasks: map[uint32]*task{},
	}
}

type job struct {
	sync.RWMutex

	id *peloton.JobID
	m  *manager

	// TODO: Use list as we expect to always track tasks 0..n-1.
	tasks map[uint32]*task
}

func (j *job) ID() *peloton.JobID {
	return j.id
}

func (j *job) GetTask(id uint32) Task {
	j.RLock()
	defer j.RUnlock()

	if t, ok := j.tasks[id]; ok {
		return t
	}

	return nil
}

func (j *job) SetTask(id uint32, runtime *pb_task.RuntimeInfo) {
	j.Lock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTask(j, id)
		j.tasks[id] = t
	}

	t.updateRuntime(runtime)
	j.Unlock()

	j.m.ScheduleTask(t, time.Now())
}

func (j *job) UpdateTask(ctx context.Context, id uint32, info *pb_task.TaskInfo) error {
	if err := j.m.taskStore.UpdateTask(ctx, info); err != nil {
		return err
	}

	j.SetTask(id, info.GetRuntime())

	return nil
}
