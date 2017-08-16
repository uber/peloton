package goalstate

import (
	"context"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

const (
	_indefDelay time.Duration = math.MaxInt64
)

func newTrackedJob(id *peloton.JobID, e *engine) *trackedJob {
	return &trackedJob{
		id:           id,
		e:            e,
		tasks:        map[uint32]*trackedTask{},
		queue:        newTimerQueue(),
		queueChanged: make(chan struct{}, 1),
	}
}

type trackedJob struct {
	sync.Mutex

	id      *peloton.JobID
	runtime *job.RuntimeInfo
	e       *engine
	// TODO: Use list as we expect to always track tasks 0..n-1.
	tasks map[uint32]*trackedTask

	stopChan <-chan struct{}

	queue        *timeoutQueue
	queueChanged chan struct{}
}

// TODO: this will spawn a goroutine per job. We should instead use an
// async.Pool.
func (j *trackedJob) start(stopChan <-chan struct{}) {
	j.Lock()
	defer j.Unlock()

	if j.stopChan != stopChan {
		j.stopChan = stopChan
		go j.run(stopChan)
	}
}

func (j *trackedJob) run(stopChan <-chan struct{}) {
	for {
		j.Lock()
		deadline := j.queue.nextDeadline()
		j.Unlock()

		var timer *time.Timer
		var timerChan <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerChan = timer.C
		}

		select {
		case <-timerChan:
			j.processNextTask()

		case <-j.queueChanged:

		case <-stopChan:
			return
		}

		if timer != nil {
			timer.Stop()
		}
	}
}

func (j *trackedJob) processNextTask() {
	j.Lock()
	defer j.Unlock()

	qi := j.queue.popIfReady()
	if qi == nil {
		return
	}

	t := qi.(*trackedTask)

	currentState := t.currentState()
	a := suggestAction(currentState, t.goalState())

	go j.runTaskAction(a, t)
}

func (j *trackedJob) runTaskAction(a action, t *trackedTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := t.applyAction(ctx, j.e.taskOperator, a)
	if err != nil {
		log.
			WithField("job", j.id.GetValue()).
			WithField("task", t.id).
			WithError(err).
			Error("failed to execute goalstate action")
	}
	cancel()

	// Update and reschedule the task, based on the result.
	j.Lock()
	defer j.Unlock()

	delay := _indefDelay
	switch {
	case a == _noAction:
		// No need to reschedule.

	case a != t.lastAction:
		// First time we see this, trigger default timeout.
		if err != nil {
			delay = j.e.cfg.FailureRetryDelay
		} else {
			delay = j.e.cfg.SuccessRetryDelay
		}

	case a == t.lastAction:
		// Not the first time we see this, apply backoff.
		delay = time.Since(t.lastActionTime) * 2
	}

	t.updateLastAction(a, t.currentState())

	var deadline time.Time
	if delay != _indefDelay {
		// Cap delay to max.
		if delay > j.e.cfg.MaxRetryDelay {
			delay = j.e.cfg.MaxRetryDelay
		}
		deadline = time.Now().Add(delay)
	}
	j.scheduleTask(t, deadline)
}

func (j *trackedJob) updateTask(id uint32, runtime *task.RuntimeInfo) {
	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		t = newTrackedTask(j, id)
		j.tasks[id] = t
	}

	t.updateRuntime(runtime)
	j.scheduleTask(t, time.Now())
}

func (j *trackedJob) updateTaskState(id uint32, state task.TaskState) {
	j.Lock()
	defer j.Unlock()

	t, ok := j.tasks[id]
	if !ok {
		log.
			WithField("job", j.id.GetValue()).
			WithField("id", id).
			Warnf("failed updating state of untracked task")
		return
	}

	t.updateState(state)
	j.scheduleTask(t, time.Now())
}

func (j *trackedJob) scheduleTask(t *trackedTask, deadline time.Time) {
	t.setDeadline(deadline)
	j.queue.update(t)
	select {
	case j.queueChanged <- struct{}{}:
	default:
	}
}
