package queue

import (
	"reflect"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/common/queue"

	log "github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

// maxMaintenanceQueueSize is the max size of the maintenance queue.
const (
	maintenanceQueueName    = "maintenance-queue"
	maxMaintenanceQueueSize = 10000
	defaultDequeueTimeout   = 100 * time.Millisecond
)

type maintenanceQueue struct {
	lock  sync.RWMutex
	queue queue.Queue
}

// MaintenanceQueue is the interface for maintenance queue.
type MaintenanceQueue interface {
	// Enqueue enqueues a list of hostnames into the maintenance queue
	Enqueue(hostnames []string) error
	// Dequeue dequeues a hostname from the maintenance queue
	Dequeue(maxWaitTime time.Duration) (string, error)
	// Length returns the length of maintenance queue at any time
	Length() int
	// Clear contents of maintenance queue
	Clear()
}

// NewMaintenanceQueue returns an instance of the maintenance queue
func NewMaintenanceQueue() MaintenanceQueue {
	return &maintenanceQueue{
		queue: queue.NewQueue(
			maintenanceQueueName,
			reflect.TypeOf(""),
			maxMaintenanceQueueSize),
	}
}

// Enqueue enqueues a list of hostnames into the maintenance queue
func (mq *maintenanceQueue) Enqueue(hostnames []string) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	var errs error
	for _, host := range hostnames {
		err := mq.queue.Enqueue(host)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

// Dequeue dequeues a hostname from the maintenance queue
func (mq *maintenanceQueue) Dequeue(maxWaitTime time.Duration) (string, error) {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	item, err := mq.queue.Dequeue(maxWaitTime)
	if err != nil {
		if _, isTimeout := err.(queue.DequeueTimeOutError); !isTimeout {
			// error is not due to timeout so we log the error
			log.WithError(err).
				Error("unable to dequeue task from maintenance queue")
		}
		return "", err
	}

	host := item.(string)
	return host, nil
}

// Length returns the length of maintenance queue at any time
func (mq *maintenanceQueue) Length() int {
	mq.lock.RLock()
	defer mq.lock.RUnlock()

	return mq.queue.Length()
}

// Clear clears the contents of the maintenance queue
func (mq *maintenanceQueue) Clear() {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	for len := mq.queue.Length(); len > 0; len-- {
		_, err := mq.queue.Dequeue(defaultDequeueTimeout)
		if err != nil {
			log.WithError(err).
				Error("unable to dequeue task from maintenance queue")
		}
	}
	log.Info("Maintenance queue cleared")
}
