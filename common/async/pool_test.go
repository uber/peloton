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
