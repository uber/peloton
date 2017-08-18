package tracked

import (
	"container/heap"
	"time"
)

const (
	_defaultDelay = time.Second
)

type queueItem interface {
	deadline() time.Time

	setIndex(index int)
	index() int
}

type queueItemMixin struct {
	queueIndex    int
	queueDeadline time.Time
}

func (i *queueItemMixin) index() int                     { return i.queueIndex }
func (i *queueItemMixin) setIndex(index int)             { i.queueIndex = index }
func (i *queueItemMixin) deadline() time.Time            { return i.queueDeadline }
func (i *queueItemMixin) setDeadline(deadline time.Time) { i.queueDeadline = deadline }

func newQueueItemMixing() queueItemMixin {
	return queueItemMixin{queueIndex: -1}
}

func newDeadlineQueue() *deadlineQueue {
	q := &deadlineQueue{
		pq: &priorityQueue{},
	}

	heap.Init(q.pq)

	return q
}

type deadlineQueue struct {
	pq *priorityQueue
}

func (q *deadlineQueue) nextDeadline() time.Time {
	if q.pq.Len() == 0 {
		return time.Time{}
	}

	return (*q.pq)[0].deadline()
}

func (q *deadlineQueue) popIfReady() queueItem {
	if q.pq.Len() == 0 {
		return nil
	}

	return heap.Pop(q.pq).(queueItem)
}

func (q *deadlineQueue) update(item queueItem) {
	// Check if it's not in the queue.
	if item.index() == -1 {
		if item.deadline().IsZero() {
			// Should not be scheduled.
			return
		}

		heap.Push(q.pq, item)
		return
	}

	// It's in the queue. Remove if it should not be scheduled.
	if item.deadline().IsZero() {
		heap.Remove(q.pq, item.index())
		return
	}

	heap.Fix(q.pq, item.index())
}

// priorityQueue is the backing heap implementation, implementing the
// `continer/heap.Interface` interface. The priorityQueue must only
// be called indirectly through the `container/heap` functions.
type priorityQueue []queueItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].deadline().Before(pq[j].deadline())
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].setIndex(i)
	pq[j].setIndex(j)
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(queueItem)
	item.setIndex(n)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	// Clear index.
	item.setIndex(-1)
	*pq = old[0 : n-1]
	// TODO: Down-size if len(pq) < cap(pq) / 2.
	return item
}
