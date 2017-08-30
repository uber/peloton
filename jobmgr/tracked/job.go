package tracked

import (
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// Job tracked by the system, serving as a best effort view of what's stored
// in the database.
type Job interface {
	// ID of the job.
	ID() *peloton.JobID

	// GetTask from the task id.
	GetTask(id uint32) Task
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
