package tracked

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	log "github.com/sirupsen/logrus"
)

// Job tracked by the system, serving as a best effort view of what's stored
// in the database.
type Job interface {
	sync.Locker

	// ID of the job.
	ID() *peloton.JobID

	// GetTask from the task id.
	GetTask(id uint32) Task

	// UpdateTask with the new runtime info. This will also schedule the task for
	// immediate evaluation.
	UpdateTask(id uint32, runtime *pb_task.RuntimeInfo)

	// UpdateTask with the new state. This will also schedule the task for
	// immediate evaluation.
	// TODO: Ideally, we should only have UpdateTask, as the state should be
	// persisted before we evalute it.
	UpdateTaskState(id uint32, state pb_task.TaskState)
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

func (j *job) UpdateTask(id uint32, runtime *pb_task.RuntimeInfo) {
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

func (j *job) UpdateTaskState(id uint32, state pb_task.TaskState) {
	j.Lock()

	t, ok := j.tasks[id]
	if !ok {
		log.
			WithField("job", j.id.GetValue()).
			WithField("id", id).
			Warnf("failed updating state of untracked task")
		j.Unlock()
		return
	}

	t.updateState(state)
	j.Unlock()

	j.m.ScheduleTask(t, time.Now())
}
