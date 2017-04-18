package async

import (
	"container/list"
	"sync"
)

// Queue structure that works similar to an unlimited channel, where Jobs can be
// added using Enqueue and drained by reading from the DequeueChannel.
// TODO: This Queue may be changed dramatically going forward, as the main
// purpose right now is to facilitate the Pool.
type Queue struct {
	lock sync.Mutex
	// TODO: Consider using circular buffer, if memory overhead can be lowered.
	list *list.List

	// enqueueSignal is added to after a successful enqueue. By having a buffer
	// size of 1, it's guaranteed that the job is processed.
	enqueueSignal  chan struct{}
	dequeueChannel chan Job
}

// NewQueue for enqueing Jobs.
func NewQueue() *Queue {
	q := &Queue{
		list:           list.New(),
		enqueueSignal:  make(chan struct{}, 1),
		dequeueChannel: make(chan Job),
	}
	go q.run()
	return q
}

// Enqueue the Job. This method will return immediately.
func (q *Queue) Enqueue(job Job) {
	q.lock.Lock()
	q.list.PushBack(job)
	q.lock.Unlock()

	// Try signal a new items is available.
	select {
	case q.enqueueSignal <- struct{}{}:
	default:
	}
}

// DequeueChannel for reading in order from the front of the queue.
func (q *Queue) DequeueChannel() <-chan Job {
	return q.dequeueChannel
}

func (q *Queue) run() {
	for {
		q.lock.Lock()

		f := q.list.Front()
		if f == nil {
			q.lock.Unlock()

			// Wait for jobs to be enqueued before continuing.
			<-q.enqueueSignal
			continue
		}

		q.list.Remove(f)
		q.lock.Unlock()

		q.dequeueChannel <- f.Value.(Job)
	}
}
