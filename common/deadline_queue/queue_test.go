package deadlinequeue

import (
	"container/heap"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type testQueueItem struct {
	value int
	i     int
}

func (i *testQueueItem) Index() int                     { return i.i }
func (i *testQueueItem) SetIndex(v int)                 { i.i = v }
func (i *testQueueItem) SetDeadline(deadline time.Time) {}
func (i *testQueueItem) Deadline() time.Time {
	if i.value == 0 {
		return time.Time{}
	}
	return time.Unix(int64(i.value), 0)
}
func (i *testQueueItem) IsScheduled() bool { return !i.Deadline().IsZero() }

func TestTimeoutQueueOrdering(t *testing.T) {
	mtx := NewQueueMetrics(tally.NoopScope)
	q := &deadlineQueue{
		pq:           &priorityQueue{},
		queueChanged: make(chan struct{}, 1),
		mtx:          mtx,
	}
	heap.Init(q.pq)

	i1 := &testQueueItem{1, -1}
	i2 := &testQueueItem{2, -1}
	i3 := &testQueueItem{3, -1}
	i4 := &testQueueItem{4, -1}

	expectOrder := func(expect []*testQueueItem, input []*testQueueItem, updates ...func()) {
		i1.value = 1
		i2.value = 2
		i3.value = 3
		i4.value = 4

		for _, i := range input {
			q.update(i)
		}

		for _, f := range updates {
			f()
		}

		for _, v := range expect {
			p := q.popIfReady().(*testQueueItem)
			assert.Equal(t, v.value, p.value)
		}
	}

	expectOrder([]*testQueueItem{i1, i2, i3, i4}, []*testQueueItem{i1, i2, i3, i4})
	expectOrder([]*testQueueItem{i1, i2, i3, i4}, []*testQueueItem{i4, i3, i2, i1})
	expectOrder([]*testQueueItem{i1, i3, i4, i2}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i2.value = 5
			q.update(i2)
		})
	expectOrder([]*testQueueItem{i2, i3, i1, i4}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i1.value = 3
			q.update(i1)
		})
	expectOrder([]*testQueueItem{i1, i3, i4}, []*testQueueItem{i1, i2, i3, i4},
		func() {
			i2.value = 0
			q.update(i2)
			assert.Equal(t, -1, i2.Index())
		})
	expectOrder([]*testQueueItem{}, []*testQueueItem{},
		func() {
			i2.value = 0
			q.update(i2)
		})
}

func TestEnqueueAndDequeueTasks(t *testing.T) {
	q := NewDeadlineQueue(NewQueueMetrics(tally.NoopScope))

	c := 100
	var wg sync.WaitGroup
	wg.Add(c)

	for i := 0; i < c; i++ {
		go func() {
			tt := q.Dequeue(nil)
			assert.NotNil(t, tt)
			wg.Done()
		}()
	}

	go func() {
		for i := 0; i < c; i++ {
			tt := NewItem(strconv.Itoa(i))
			q.Enqueue(tt, time.Now())
		}
	}()

	wg.Wait()
}
