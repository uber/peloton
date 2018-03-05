package deadlinequeue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	i1 := &testQueueItem{1, 1}
	i2 := &testQueueItem{2, 2}
	i3 := &testQueueItem{3, 3}
	i4 := &testQueueItem{4, 4}

	pq := priorityQueue{}

	pq.Push(i1)
	pq.Push(i2)
	pq.Push(i3)
	pq.Push(i4)

	assert.Equal(t, 4, pq.Len())
	assert.Equal(t, 1, pq[0].(*testQueueItem).value)

	pq.Swap(0, 3)
	assert.Equal(t, 4, pq[0].(*testQueueItem).value)
	assert.True(t, pq.Less(1, 0))

	pq.Pop()
	assert.Equal(t, 3, pq.Len())
}
