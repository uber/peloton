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
