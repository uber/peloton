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

	"github.com/stretchr/testify/assert"
)

func TestEmptyPool(t *testing.T) {
	p := NewPool(PoolOptions{
		MaxWorkers: 1,
	}, nil)

	p.WaitUntilProcessed()
}

func TestPoolEnqueueAndRunMany(t *testing.T) {
	p := NewPool(PoolOptions{}, nil)
	c := 100
	p.Start()

	var r int64

	for i := 0; i < c; i++ {
		p.Enqueue(JobFunc(func(ctx context.Context) {
			atomic.AddInt64(&r, 1)
		}))
	}

	p.WaitUntilProcessed()

	assert.Equal(t, int64(c), r)
}

func TestPoolEnqueueConcurrentAndRunMany(t *testing.T) {
	p := NewPool(PoolOptions{}, nil)
	c := 100
	p.Start()

	var r int64
	var wg sync.WaitGroup
	wg.Add(c)

	for i := 0; i < c; i++ {
		go func() {
			p.Enqueue(JobFunc(func(ctx context.Context) {
				atomic.AddInt64(&r, 1)
			}))
			wg.Done()
		}()
	}

	wg.Wait()

	p.WaitUntilProcessed()

	assert.Equal(t, int64(c), r)
}

func TestPoolSetMaxWorkers(t *testing.T) {
	p := NewPool(PoolOptions{}, nil)
	c := 100
	iter := 10
	p.Start()

	var r int64
	var wg sync.WaitGroup
	wg.Add(c * iter)

	test := func() {
		for i := 0; i < c; i++ {
			go func() {
				p.Enqueue(JobFunc(func(ctx context.Context) {
					atomic.AddInt64(&r, 1)
				}))
				wg.Done()
			}()
		}
	}

	for i := 0; i < iter; i++ {
		test()
		if i == 0 {
			p.SetMaxWorkers(-1)
		} else if i == 3 {
			p.SetMaxWorkers(2)
		} else if i == 6 {
			p.SetMaxWorkers(6)
		} else if i == 9 {
			p.SetMaxWorkers(1)
		}
	}

	wg.Wait()

	p.WaitUntilProcessed()

	assert.Equal(t, int64(c*iter), r)
}

func TestPoolStop(t *testing.T) {
	p := NewPool(PoolOptions{}, nil)
	c := 100
	iter := 10
	p.Start()

	var r int64

	test := func() {
		for i := 0; i < c; i++ {
			go func() {
				p.Enqueue(JobFunc(func(ctx context.Context) {
					atomic.AddInt64(&r, 1)
				}))
			}()
		}
	}

	p.SetMaxWorkers(2)
	for i := 0; i < iter; i++ {
		go func() {
			test()
		}()
	}
	p.SetMaxWorkers(6)

	p.Stop()

	// Since pool is stopped, all the workers are terminated
	assert.Equal(t, p.numWorkers, 0)
}
