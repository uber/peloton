package async

import (
	"context"
	"testing"
)

func TestEnqueueManyAreAllRead(t *testing.T) {
	q := newQueue()
	c := 100

	test := func() {
		for i := 0; i < c; i++ {
			q.Enqueue(JobFunc(func(ctx context.Context) {}))
		}

		for i := 0; i < c; i++ {
			<-q.dequeueChannel
		}
	}

	for i := 0; i < 10; i++ {
		test()
	}
}

func TestEnqueueManyConcurrentlyAreAllRead(t *testing.T) {
	q := newQueue()
	c := 100

	test := func() {
		for i := 0; i < c; i++ {
			go func() {
				q.Enqueue(JobFunc(func(ctx context.Context) {}))
			}()
		}

		for i := 0; i < c; i++ {
			<-q.dequeueChannel
		}
	}

	for i := 0; i < 10; i++ {
		test()
	}
}
