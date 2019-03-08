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
	"time"
)

// priorityQueue is the backing heap implementation, implementing the
// `continer/heap.Interface` interface. The priorityQueue must only
// be called indirectly through the `container/heap` functions.
type priorityQueue []QueueItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Deadline().Before(pq[j].Deadline())
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].SetIndex(i)
	pq[j].SetIndex(j)
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(QueueItem)
	item.SetIndex(n)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	// Clear index and deadline.
	item.SetIndex(-1)
	*pq = old[0 : n-1]
	// TODO: Down-size if len(pq) < cap(pq) / 2.
	return item
}

func (pq *priorityQueue) NextDeadline() time.Time {
	return (*pq)[0].Deadline()
}
