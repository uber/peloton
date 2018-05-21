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
	})

	p.WaitUntilProcessed()
}

func TestPoolEnqueueAndRunMany(t *testing.T) {
	p := NewPool(PoolOptions{})
	c := 100

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
	p := NewPool(PoolOptions{})
	c := 100

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
	p := NewPool(PoolOptions{})
	c := 100
	iter := 10

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
		}
	}

	wg.Wait()

	p.WaitUntilProcessed()

	assert.Equal(t, int64(c*iter), r)
}

func TestPoolStop(t *testing.T) {
	p := NewPool(PoolOptions{})
	c := 100
	iter := 10

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

	for i := 0; i < iter; i++ {
		test()
	}

	p.Stop()

	// Since jobs were terminated, all jobs were not processed
	assert.NotEqual(t, int64(c*iter), r)
}
