package deadlinequeue

import (
	"github.com/uber-go/tally"
)

// QueueMetrics contains all counters to track queue metrics
type QueueMetrics struct {
	queueLength   tally.Gauge // length of the queue
	queuePopDelay tally.Timer // delay in dequeuing the queue item after its deadline has expired
}

// NewQueueMetrics returns a new QueueMetrics struct.
func NewQueueMetrics(scope tally.Scope) *QueueMetrics {
	queueScope := scope.SubScope("queue")
	return &QueueMetrics{
		queueLength:   queueScope.Gauge("length"),
		queuePopDelay: queueScope.Timer("pop_delay"),
	}
}
