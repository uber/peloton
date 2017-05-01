package task

import (
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"math"
	mesos "mesos/v1"
	pb_eventstream "peloton/private/eventstream"
	"sync"
	"sync/atomic"
)

// StatusProcessor is the interface to process a task status update
type StatusProcessor interface {
	ProcessStatusUpdate(taskStatus *mesos.TaskStatus) error
}

// asyncEventProcessor maps events to a list of buckets; and each bucket would be consumed by a single go routine
// in which the task updates are processed. This would allow quick response to mesos for those status updates; while
// for each individual task, the events are processed in order
type asyncEventProcessor struct {
	sync.RWMutex
	eventBuckets []*eventBucket
}

// taskUpdateBucket is a bucket of task updates. All updates for one task would end up in one bucket in order; a bucket
// can hold status updates for multiple tasks.
type eventBucket struct {
	eventChannel    chan *pb_eventstream.Event
	shutdownChannel chan struct{}
	// index is used to identify the bucket in eventBuckets
	index           int
	processedCount  *int32
	processedOffset *uint64
}

func newEventBucket(size int, index int) *eventBucket {
	updates := make(chan *pb_eventstream.Event, size)
	var processedCount int32
	var processedOffset uint64
	return &eventBucket{
		eventChannel:    updates,
		shutdownChannel: make(chan struct{}, 10),
		index:           index,
		processedCount:  &processedCount,
		processedOffset: &processedOffset,
	}
}

func (t *eventBucket) shutdown() {
	log.WithField("bucket_index", t.index).Info("Shutting down bucket")
	t.shutdownChannel <- struct{}{}
}

func (t *eventBucket) getProcessedCount() int32 {
	return atomic.LoadInt32(t.processedCount)
}

func newBucketEventProcessor(t StatusProcessor, bucketNum int, chanSize int) *asyncEventProcessor {
	var buckets []*eventBucket
	for i := 0; i < bucketNum; i++ {
		bucket := newEventBucket(chanSize, i)
		buckets = append(buckets, bucket)
		go func() {
			for {
				select {
				case event := <-bucket.eventChannel:
					err := t.ProcessStatusUpdate(event.MesosTaskStatus)
					if err != nil {
						log.WithError(err).
							WithField("bucket_num", bucket.index).
							WithField("status", event.MesosTaskStatus).
							Error("Error applying taskSatus")
					}
					atomic.AddInt32(bucket.processedCount, 1)
					atomic.StoreUint64(bucket.processedOffset, event.Offset)
				case <-bucket.shutdownChannel:
					log.WithField("bucket_num", bucket.index).Info("bucket is shutdown")
					return
				}
			}
		}()
	}
	return &asyncEventProcessor{
		eventBuckets: buckets,
	}
}

func (t *asyncEventProcessor) addEvent(event *pb_eventstream.Event) {
	mesosTaskID := event.MesosTaskStatus.GetTaskId().GetValue()
	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		log.WithError(err).
			WithField("mesos_task_id", mesosTaskID).
			Error("Failed to ParseTaskIDFromMesosTaskID")
		return
	}
	_, instanceID, _ := util.ParseTaskID(taskID)
	index := instanceID % len(t.eventBuckets)
	t.eventBuckets[index].eventChannel <- event
}

func (t *asyncEventProcessor) shutdown() {
	t.RLock()
	defer t.RUnlock()
	for _, bucket := range t.eventBuckets {
		bucket.shutdown()
	}
}

// GetEventProgress returns the current safe progress among all buckets
// This value is used to purge data on the event stream server side.
func (t *asyncEventProcessor) GetEventProgress() uint64 {
	t.RLock()
	defer t.RUnlock()
	var minOffset uint64
	minOffset = math.MaxUint64
	for _, bucket := range t.eventBuckets {
		offset := atomic.LoadUint64(bucket.processedOffset)
		if offset < minOffset {
			minOffset = offset
		}
	}
	return minOffset
}
