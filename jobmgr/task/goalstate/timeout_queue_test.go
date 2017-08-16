package goalstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testQueueItem struct {
	value int
	i     int
}

func (i *testQueueItem) index() int     { return i.i }
func (i *testQueueItem) setIndex(v int) { i.i = v }
func (i *testQueueItem) deadline() time.Time {
	if i.value == 0 {
		return time.Time{}
	}
	return time.Unix(int64(i.value), 0)
}

func TestTimeoutQueueOrdering(t *testing.T) {

	q := newTimerQueue()

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
			assert.Equal(t, v, q.popIfReady())
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
			assert.Equal(t, -1, i2.index())
		})
	expectOrder([]*testQueueItem{}, []*testQueueItem{},
		func() {
			i2.value = 0
			q.update(i2)
		})
}
