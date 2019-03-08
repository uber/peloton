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

/*
Package deadlinequeue implements a deadline queue.
It provides a QueueItem interface which can be used to define an item
wnich can enqueued to and dequeued from the deadline queue.
It also provides an Item structure which implements a sample queue item.
The deadline queue provides an Enqueue call which can be used to enqueue
a queue item with a deadline indicating whien the item should be dequeued,
and a Dequeue call which is a blocking call which returns the first item
in the queue when its deadline expires.
*/
package deadlinequeue
