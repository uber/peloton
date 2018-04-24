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
)

var (
	mq *maintenanceQueue
)

type maintenanceQueue struct {
	sync.Mutex
	queue queue.Queue
}

// MaintenanceQueue is the interface for maintenance queue.
type MaintenanceQueue interface {
	// Enqueue enqueues a list of hostnames into the maintenance queue
	Enqueue(hostnames []string) error
	// Dequeue dequeues a hostname from the maintenance queue
	Dequeue(maxWaitTime time.Duration) (string, error)
}

// NewMaintenanceQueue returns an instance of the maintenance queue
func NewMaintenanceQueue() MaintenanceQueue {
	mq = &maintenanceQueue{
		queue: queue.NewQueue(
			maintenanceQueueName,
			reflect.TypeOf(""),
			maxMaintenanceQueueSize),
	}
	return mq
}

// Enqueue enqueues a list of hostnames into the maintenance queue
func (mq *maintenanceQueue) Enqueue(hostnames []string) error {
	mq.Lock()
	defer mq.Unlock()

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
	mq.Lock()
	defer mq.Unlock()

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
