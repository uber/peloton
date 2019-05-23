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
	"testing"
)

func TestEnqueueManyAreAllRead(t *testing.T) {
	q := newQueue()
	c := 100

	q.Run(make(chan struct{}))

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

	q.Run(make(chan struct{}))

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
