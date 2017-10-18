package async

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Counter struct {
	number  int64
	running bool
	lock    sync.Mutex
}

func (c *Counter) Run(ctx context.Context) error {
	atomic.AddInt64(&c.number, 1)
	return nil
}

func (c *Counter) larger(expected int64) int64 {
	repeat := true
	var value int64
	for repeat {
		value = atomic.LoadInt64(&c.number)
		repeat = value < expected
	}
	return value
}

type Waiter struct {
	gotEvent bool
	running  bool
	lock     sync.Mutex
}

func (w *Waiter) Running() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.running
}

func (w *Waiter) setRunning(state bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.running = state
}

func (w *Waiter) Run(ctx context.Context) error {
	w.setRunning(true)
	defer w.setRunning(false)
	select {
	case <-ctx.Done():
		w.gotEvent = true
	}
	return nil
}

func setupWaiter() (Daemon, *Waiter) {
	waiter := &Waiter{}
	return NewDaemon("waiter", waiter), waiter
}

func setupCounter() (Daemon, *Counter) {
	counter := &Counter{}
	return NewDaemon("counter", counter), counter
}

func TestDaemonStart(t *testing.T) {
	daemon, counter := setupCounter()
	daemon.Start()
	value1 := counter.larger(1)
	assert.True(t, value1 > 0)
	value2 := counter.larger(value1)
	assert.True(t, value2 >= value1)
	daemon.Stop()
}

func TestDaemonStopEvent(t *testing.T) {
	daemon, waiter := setupWaiter()
	daemon.Start()
	for !waiter.Running() {
		continue
	}
	daemon.Stop()
	for waiter.Running() {
		continue
	}
	assert.True(t, waiter.gotEvent)
}

func TestDaemonStop(t *testing.T) {
	daemon, counter := setupCounter()
	daemon.Start()
	value1 := counter.larger(1)
	daemon.Stop()
	assert.True(t, value1 > 0)
	value2 := counter.larger(value1)
	value3 := counter.larger(value1)
	assert.True(t, value3 == value2)
}
