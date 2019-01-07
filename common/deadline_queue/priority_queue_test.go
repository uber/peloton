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

package deadlinequeue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	i1 := &testQueueItem{1, 1}
	i2 := &testQueueItem{2, 2}
	i3 := &testQueueItem{3, 3}
	i4 := &testQueueItem{4, 4}

	pq := priorityQueue{}

	pq.Push(i1)
	pq.Push(i2)
	pq.Push(i3)
	pq.Push(i4)

	assert.Equal(t, 4, pq.Len())
	assert.Equal(t, 1, pq[0].(*testQueueItem).value)

	pq.Swap(0, 3)
	assert.Equal(t, 4, pq[0].(*testQueueItem).value)
	assert.True(t, pq.Less(1, 0))

	pq.Pop()
	assert.Equal(t, 3, pq.Len())
}
