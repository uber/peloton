package async

import (
	"container/list"
	"sync"
)

// Queue defines the interface of a queue used by the async pool
// to enqueue jobs and then dequeue the job when a worker becomes available
type Queue interface {
	// Enqueue is used to enqueue a job
	Enqueue(job Job)
	// Dequeue is used to fetch an enqueued job when a worker is available
	Dequeue(stopChan <-chan struct{}) Job
}

// queue structure that works similar to an unlimited channel, where Jobs can be
// added using Enqueue and drained by reading from the DequeueChannel.
// TODO: This queue may be changed dramatically going forward, as the main
// purpose right now is to facilitate the Pool.
type queue struct {
	sync.Mutex
	// TODO: Consider using circular buffer, if memory overhead can be lowered.
	list *list.List

	// enqueueSignal is added to after a successful enqueue. By having a buffer
	// size of 1, it's guaranteed that the job is processed.
	enqueueSignal  chan struct{}
	dequeueChannel chan Job
}

// newQueue for enqueing Jobs.
func newQueue() *queue {
	q := &queue{
		list:           list.New(),
		enqueueSignal:  make(chan struct{}, 1),
		dequeueChannel: make(chan Job),
	}
	go q.run()
	return q
}

// Enqueue the Job. This method will return immediately.
func (q *queue) Enqueue(job Job) {
	q.Lock()
	q.list.PushBack(job)
	q.Unlock()

	// Try signal a new items is available.
	select {
	case q.enqueueSignal <- struct{}{}:
	default:
	}
}

// Dequeue the Job.
func (q *queue) Dequeue(stopChan <-chan struct{}) Job {
	select {
	case <-stopChan:
		return nil
	case job := <-q.dequeueChannel:
		return job
	}
}

func (q *queue) run() {
	for {
		q.Lock()

		f := q.list.Front()
		if f == nil {
			q.Unlock()

			// Wait for jobs to be enqueued before continuing.
			<-q.enqueueSignal
			continue
		}

		q.list.Remove(f)
		q.Unlock()

		q.dequeueChannel <- f.Value.(Job)
	}
}
