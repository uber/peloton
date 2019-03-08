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

package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Closer closes the runtime metrics collection
type Closer func()

// _numGCThreshold comes from the PauseNs buffer size https://golang.org/pkg/runtime/#MemStats
var _numGCThreshold = uint32(256)

// StartCollectingRuntimeMetrics starts generating runtime metrics under given metrics scope with
// the collectInterval
func StartCollectingRuntimeMetrics(
	scope tally.Scope,
	enabled bool,
	collectInterval time.Duration,
) Closer {
	runtimeCollector := NewRuntimeCollector(scope, collectInterval)
	if enabled {
		runtimeCollector.Start()
	}
	return runtimeCollector.close
}

type runtimeMetrics struct {
	numGoRoutines   tally.Gauge
	goMaxProcs      tally.Gauge
	memoryAllocated tally.Gauge
	memoryHeap      tally.Gauge
	memoryHeapIdle  tally.Gauge
	memoryHeapInuse tally.Gauge
	memoryStack     tally.Gauge
	numGC           tally.Counter
	gcPauseMs       tally.Timer
	lastNumGC       uint32
}

// RuntimeCollector is a struct containing the state of the runtimeMetrics.
type RuntimeCollector struct {
	scope           tally.Scope
	collectInterval time.Duration
	metrics         runtimeMetrics
	startedMutex    sync.RWMutex
	started         bool // protected by startedMutex
	quit            chan struct{}
}

// NewRuntimeCollector creates a new RuntimeCollector.
func NewRuntimeCollector(scope tally.Scope, collectInterval time.Duration) *RuntimeCollector {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	return &RuntimeCollector{
		scope:           scope,
		collectInterval: collectInterval,
		metrics: runtimeMetrics{
			numGoRoutines:   scope.Gauge("num_goroutines"),
			goMaxProcs:      scope.Gauge("gomaxprocs"),
			memoryAllocated: scope.Gauge("memory_allocated"),
			memoryHeap:      scope.Gauge("memory_heap"),
			memoryHeapIdle:  scope.Gauge("memory_heapidle"),
			memoryHeapInuse: scope.Gauge("memory_heapinuse"),
			memoryStack:     scope.Gauge("memory_stack"),
			numGC:           scope.Counter("memory_num_gc"),
			gcPauseMs:       scope.Timer("memory_gc_pause_ms"),
			lastNumGC:       memstats.NumGC,
		},
		started: false,
		quit:    make(chan struct{}),
	}
}

// IsRunning returns true if the collector has been started and false if not.
func (r *RuntimeCollector) IsRunning() bool {
	r.startedMutex.RLock()
	defer r.startedMutex.RUnlock()
	return r.started
}

// Start starts the collector thread that periodically emits metrics.
func (r *RuntimeCollector) Start() {
	log.Info("rtm: starting")
	r.startedMutex.RLock()
	if r.started {
		r.startedMutex.RUnlock()
		return
	}
	r.startedMutex.RUnlock()
	go func() {
		ticker := time.NewTicker(r.collectInterval)
		for {
			select {
			case <-ticker.C:
				r.generate()
			case <-r.quit:
				ticker.Stop()
				return
			}
		}
	}()
	r.startedMutex.Lock()
	r.started = true
	r.startedMutex.Unlock()
}

// generate sends runtime metrics to the local metrics collector.
func (r *RuntimeCollector) generate() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	r.metrics.numGoRoutines.Update(float64(runtime.NumGoroutine()))
	r.metrics.goMaxProcs.Update(float64(runtime.GOMAXPROCS(0)))
	r.metrics.memoryAllocated.Update(float64(memStats.Alloc))
	r.metrics.memoryHeap.Update(float64(memStats.HeapAlloc))
	r.metrics.memoryHeapIdle.Update(float64(memStats.HeapIdle))
	r.metrics.memoryHeapInuse.Update(float64(memStats.HeapInuse))
	r.metrics.memoryStack.Update(float64(memStats.StackInuse))

	// Now we calculate the total time paused for GC.

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at
	// 2^32).
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.metrics.lastNumGC, num) // reset for the next iteration

	// (num - lastNum) tells us the number of gc cycles performed since the late
	// generate was called.
	if delta := num - lastNum; delta > 0 {
		r.metrics.numGC.Inc(int64(delta))
		// _numGCThreshold holds the max value beyond which we can't get the gc
		// pause stats from runtime.MemStats.
		if delta >= _numGCThreshold {
			lastNum = num - _numGCThreshold
		}
		// for each gc cycle record the pause duration.
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.metrics.gcPauseMs.Record(time.Duration(pause))
		}
	}
}

// close stops collecting runtime metrics. It cannot be started again after it's
// been stopped.
func (r *RuntimeCollector) close() {
	close(r.quit)
}
