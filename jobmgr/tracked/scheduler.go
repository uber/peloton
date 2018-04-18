package tracked

import (
	"sync"
	"time"
)

// Scheduler is used to schedule a work item in a deadline queue.
// It is used for scheduling job and task goal state engine.
type scheduler struct {
	sync.RWMutex

	queue        *deadlineQueue
	queueChanged chan struct{}
}

func newScheduler(mtx *QueueMetrics) *scheduler {
	return &scheduler{
		queue:        newDeadlineQueue(mtx),
		queueChanged: make(chan struct{}, 1),
	}
}

// schedule item to be run at deadline.
func (s *scheduler) schedule(qi queueItem, deadline time.Time) {
	s.Lock()
	defer s.Unlock()

	// Override if newer.
	if qi.deadline().IsZero() || deadline.Before(qi.deadline()) {
		qi.setDeadline(deadline)
		s.queue.update(qi)
		select {
		case s.queueChanged <- struct{}{}:
		default:
		}
	}
}

func (s *scheduler) waitForReady(stopChan <-chan struct{}) queueItem {
	for {
		s.RLock()
		deadline := s.queue.nextDeadline()
		s.RUnlock()

		var timer *time.Timer
		var timerChan <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerChan = timer.C
		}

		select {
		case <-timerChan:
			s.Lock()
			r := s.queue.popIfReady()
			s.Unlock()

			if r != nil {
				return r
			}

		case <-s.queueChanged:
			// Wake up to process the next item in the queue

		case <-stopChan:
			return nil
		}

		if timer != nil {
			timer.Stop()
		}
	}
}
