// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package async

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Waiter struct {
	gotEvent bool
	running  bool
	lock     sync.Mutex
}

func (w *Waiter) GotEvent() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.gotEvent
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

func (w *Waiter) setGotEvent(state bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.gotEvent = state
}

func (w *Waiter) Run(ctx context.Context) error {
	w.setRunning(true)
	defer w.setRunning(false)
	select {
	case <-ctx.Done():
		w.setGotEvent(true)
		return ctx.Err()
	}
}

type Running struct {
	running  int64
	multiple int64
}

func (r *Running) Run(ctx context.Context) error {
	atomic.AddInt64(&r.running, 1)
	if running := atomic.LoadInt64(&r.running); running != 1 {
		atomic.StoreInt64(&r.multiple, 1)
	}
	select {
	case <-time.After(2 * time.Millisecond):
	case <-ctx.Done():
	}
	atomic.AddInt64(&r.running, -1)
	return nil
}

func setupWaiter() (Daemon, *Waiter) {
	waiter := &Waiter{}
	return NewDaemon("waiter", waiter), waiter
}

func setupRunning() (Daemon, *Running) {
	running := &Running{}
	return NewDaemon("running", running), running
}

func TestDaemonMultipleStartAndStopsStartsOnlyOneConcurrentRunnable(t *testing.T) {
	daemon, running := setupRunning()
	allStarted := &sync.WaitGroup{}
	concurrency := 100
	allStarted.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer allStarted.Done()
			daemon.Start()
		}()
	}
	allStarted.Wait()
	// Multiple stops will not block
	daemon.Stop()
	daemon.Stop()
	assert.Equal(t, int64(0), atomic.LoadInt64(&running.multiple))
}

func TestDaemonStart(t *testing.T) {
	daemon, waiter := setupWaiter()
	daemon.Start()
	for !waiter.Running() {
		continue
	}
	assert.True(t, waiter.Running())
	daemon.Stop()
}

func TestDaemonStop(t *testing.T) {
	daemon, waiter := setupWaiter()
	daemon.Start()
	for !waiter.Running() {
		continue
	}
	daemon.Stop()
	assert.True(t, waiter.GotEvent())
}
