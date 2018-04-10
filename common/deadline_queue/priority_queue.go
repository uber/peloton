package deadlinequeue

import (
	"time"
)

// priorityQueue is the backing heap implementation, implementing the
// `continer/heap.Interface` interface. The priorityQueue must only
// be called indirectly through the `container/heap` functions.
type priorityQueue []QueueItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Deadline().Before(pq[j].Deadline())
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].SetIndex(i)
	pq[j].SetIndex(j)
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(QueueItem)
	item.SetIndex(n)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	// Clear index and deadline.
	item.SetIndex(-1)
	item.SetDeadline(time.Time{})
	*pq = old[0 : n-1]
	// TODO: Down-size if len(pq) < cap(pq) / 2.
	return item
}

func (pq *priorityQueue) NextDeadline() time.Time {
	return (*pq)[0].Deadline()
}
