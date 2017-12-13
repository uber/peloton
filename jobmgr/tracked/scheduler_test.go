package tracked

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestSchedulerScheduleAndDequeueTasks(t *testing.T) {
	s := newScheduler(NewQueueMetrics(tally.NoopScope))

	c := 100
	var wg sync.WaitGroup
	wg.Add(c)

	for i := 0; i < c; i++ {
		go func() {
			tt := s.waitForReady(nil)
			assert.NotNil(t, tt)
			wg.Done()
		}()
	}

	go func() {
		for i := 0; i < c; i++ {
			tt := newTask(nil, uint32(i))
			s.schedule(tt, time.Now())
		}
	}()

	wg.Wait()
}
