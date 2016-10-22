package util

import (
	"code.uber.internal/go-common.git/x/log"
	mesos_v1 "mesos/v1"
	"peloton/task"
	"time"
)

const (
	defaultChannelBufferSize = 1000000
	memLocalOfferQueue       = "MemLocalOfferQueue"
	memLocalTaskQueue        = "MemLocalTaskQueue"
)

// RecQueue is the common functions for queues
type RecQueue interface {
	GetQueueName() string
	GetQueueType() string
}

// TaskQueue stores the tasks to be launched
type TaskQueue interface {
	RecQueue
	GetTask(d time.Duration) *task.TaskInfo
	PutTask(offer *task.TaskInfo)
}

// OfferQueue stores offers
type OfferQueue interface {
	RecQueue
	GetOffer(d time.Duration) *mesos_v1.Offer
	PutOffer(offer *mesos_v1.Offer)
}

type memLocalQueue struct {
	channel chan interface{}
	name    string
}

// GetRec gets a record
func (m *memLocalQueue) GetRec(d time.Duration) interface{} {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(d)
		timeout <- true
	}()
	select {
	case rec := <-m.channel:
		return rec
	case <-timeout:
		log.Debugf("Queue %v GetRec timeout after %v", m.GetQueueName(), d)
		return nil
	}
	return nil
}

// PutRec puts a record
func (m *memLocalQueue) PutRec(rec interface{}) {
	m.channel <- rec
}

//GetQueueName returns queue name
func (m *memLocalQueue) GetQueueName() string {
	return m.name
}

// MemLocalOfferQueue is the Offer queue backed by a go channel
type MemLocalOfferQueue struct {
	memLocalQueue
}

// NewMemLocalOfferQueue creates a MemLocalOfferQueue
func NewMemLocalOfferQueue(qName string) OfferQueue {
	q := MemLocalOfferQueue{
		memLocalQueue{channel: make(chan interface{}, defaultChannelBufferSize),
			name: qName,
		},
	}
	return &q
}

// PutOffer puts an offer
func (m *MemLocalOfferQueue) PutOffer(offer *mesos_v1.Offer) {
	m.PutRec(offer)
}

// GetOffer gets an offer
func (m *MemLocalOfferQueue) GetOffer(d time.Duration) *mesos_v1.Offer {
	rec := m.GetRec(d)
	if rec == nil {
		return nil
	}
	offer, ok := rec.(*mesos_v1.Offer)
	if ok {
		return offer
	}
	log.Errorf("Found non-offer rec %v in the queue %v", rec, m.GetQueueName())
	return nil
}

// GetQueueType returns the queue type
func (m *MemLocalOfferQueue) GetQueueType() string {
	return memLocalOfferQueue
}

// MemLocalTaskQueue is the task queue backed by a go channel
type MemLocalTaskQueue struct {
	memLocalQueue
}

//NewMemLocalTaskQueue creates a MemLocalTaskQueue
func NewMemLocalTaskQueue(qName string) TaskQueue {
	q := MemLocalTaskQueue{
		memLocalQueue{channel: make(chan interface{}, defaultChannelBufferSize),
			name: qName,
		},
	}
	return &q
}

// PutTask puts a task
func (m *MemLocalTaskQueue) PutTask(task *task.TaskInfo) {
	m.PutRec(task)
}

// GetTask gets a task
func (m *MemLocalTaskQueue) GetTask(d time.Duration) *task.TaskInfo {
	rec := m.GetRec(d)
	if rec == nil {
		return nil
	}
	offer, ok := rec.(*task.TaskInfo)
	if ok {
		return offer
	}
	log.Errorf("Found non-task rec %v in the queue %v", rec, m.GetQueueName())
	return nil
}

// GetQueueType returns the queue type
func (m *MemLocalTaskQueue) GetQueueType() string {
	return memLocalTaskQueue
}
