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
)

const (
	// DefaultMaxWorkers of a Pool. See Pool.SetMaxWorkers for more info.
	DefaultMaxWorkers = 4
)

// PoolOptions for constructing a new Pool.
type PoolOptions struct {
	MaxWorkers int
}

// Pool structure for running up to a maximum number of jobs concurrently.
// The pool as an internal queue, such that all jobs added will be accepted
// but not run until it reached the front of the queue and a worker is free.
type Pool struct {
	sync.Mutex
	options    PoolOptions
	queue      Queue
	numWorkers int
	jobs       sync.WaitGroup
	stopChan   chan struct{}
}

// NewPool returns a new pool, provided the PoolOptions and the queue.
func NewPool(o PoolOptions, queue Queue) *Pool {
	if o.MaxWorkers <= 0 {
		o.MaxWorkers = DefaultMaxWorkers
	}

	if queue == nil {
		queue = newQueue()
	}

	p := &Pool{
		options:    o,
		queue:      queue,
		numWorkers: o.MaxWorkers,
	}

	return p
}

// SetMaxWorkers to the number provided. If smaller than the current value, it
// will lazily close existing workers. If greater, new workers will be created.
// If 0 or less is given, DefaultMaxWorkers will be used instead.
func (p *Pool) SetMaxWorkers(num int) {
	if num <= 0 {
		num = DefaultMaxWorkers
	}

	p.Lock()
	p.options.MaxWorkers = num
	if p.numWorkers > p.options.MaxWorkers {
		go p.stopWorkers()
	} else if p.numWorkers < p.options.MaxWorkers {
		go p.addWorkers()
	}
	p.Unlock()
}

// Enqueue a job in the pool.
// TODO: Take an context argument that will be associated to the job. That way
// deadlines can easily be propagated.
func (p *Pool) Enqueue(job Job) {
	p.jobs.Add(1)
	p.queue.Enqueue(job)
}

// WaitUntilProcessed will block until both the queue is empty and all workers
// are idle. This is useful for per-request Pools and in testing.
func (p *Pool) WaitUntilProcessed() {
	p.jobs.Wait()
}

// Start the worker pool by initializing the stop channel
// and starting all the workers
func (p *Pool) Start() {
	p.Lock()
	if p.stopChan != nil {
		p.Unlock()
		return
	}

	p.stopChan = make(chan struct{})
	p.Unlock()

	// Spawn initial workers.
	for i := 0; i < p.options.MaxWorkers; i++ {
		go p.runWorker()
	}
}

// Stop sets the assigned workers (goal state) to zero,
// and then stopWorkers terminates running workers (actual state) to 0 value
// amd finally cleans up the stop channel
func (p *Pool) Stop() {
	p.Lock()
	if p.stopChan == nil {
		p.Unlock()
		return
	}

	p.options.MaxWorkers = 0
	p.Unlock()
	p.stopWorkers()

	p.Lock()
	defer p.Unlock()
	close(p.stopChan)
	p.stopChan = nil
}

// addWorkers add more workers in the pool to achieve goal state of Max Workers in the pool.
func (p *Pool) addWorkers() {
	for {
		p.Lock()
		// Validate Running workers >= Assigned Workers.
		if p.numWorkers >= p.options.MaxWorkers {
			p.Unlock()
			break
		} else {
			p.numWorkers++
			go p.runWorker()
		}
		p.Unlock()
	}
}

// stopWorkers stops running workers to achieve goal state of Max Workers in the pool.
func (p *Pool) stopWorkers() {
	for {
		p.Lock()
		// Validate Running workers <= Assigned Workers.
		if p.numWorkers <= p.options.MaxWorkers {
			p.Unlock()
			break
		} else {
			// Send best effort stopChan to terminate worker,
			// if received then a running worker is terminated.
			select {
			case p.stopChan <- struct{}{}:
				p.numWorkers--
			default:
			}
		}
		p.Unlock()
	}
}

// runWorker starts a worker go routine to process jobs from FIFO queue.
func (p *Pool) runWorker() {
	for {
		job := p.queue.Dequeue(p.stopChan)
		if job == nil {
			return
		}

		job.Run(context.TODO())
		p.jobs.Done()
	}
}
