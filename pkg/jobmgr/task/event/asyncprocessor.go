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

package event

import (
	"context"
	"hash/fnv"
	"io"
	"sync"
	"sync/atomic"
	"time"

	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	v1pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/statusupdate"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/common/util"
)

const (
	// _waitForTransientErrorBeforeRetry is the time between successive retries
	// DB updates in case of transient errors in DB read/writes.
	_waitForTransientErrorBeforeRetry = 1 * time.Millisecond
	// _waitForDrainingEventBucket is the time between successive checks of
	// whether there are events remaining in the bucket during shutdownCh
	_waitForDrainingEventBucket = 1 * time.Millisecond
)

// StatusProcessor is the interface to process a task status update
type StatusProcessor interface {
	ProcessStatusUpdate(ctx context.Context, event *statusupdate.Event) error
	ProcessListeners(event *statusupdate.Event)
}

// asyncEventProcessor maps events to a list of buckets; and each bucket would
// be consumed by a single go routine in which the task updates are processed.
// This would allow quick response to mesos for those status updates; while
// for each individual task, the events are processed in order
type asyncEventProcessor struct {
	sync.RWMutex
	lifecycle      lifecycle.LifeCycle
	eventProcessor StatusProcessor
	// the buckets for events. Note that this slice is
	// set only when the object is created, so it is accessed
	// without using locks
	eventBuckets []*eventBucket
	// waitgroup for bucket goroutines
	bucketsWg sync.WaitGroup
}

// eventBucket is a bucket of task updates. All updates for one task would end
// up in one bucket in order; a bucket can hold status updates for multiple
// tasks.
type eventBucket struct {
	eventCh chan *statusupdate.Event
	// index is used to identify the bucket in eventBuckets
	index           int
	processedCount  *int32
	processedOffset *uint64
}

// Creates a new event bucket of specified size
func newEventBucket(size int, index int) *eventBucket {
	updates := make(chan *statusupdate.Event, size)
	var processedCount int32
	var processedOffset uint64
	return &eventBucket{
		eventCh:         updates,
		index:           index,
		processedCount:  &processedCount,
		processedOffset: &processedOffset,
	}
}

// Wait for all queued up events in the bucket to be processed.
// Note that there is no guarantee that the event queue will be
// empty after calling this function because addEvent() does not
// reject events during/after drain.
func (t *eventBucket) drain() {
	log.WithField("bucket_index", t.index).Info(
		"waiting for events in bucket to finish")
	for len(t.eventCh) > 0 {
		time.Sleep(_waitForDrainingEventBucket)
	}
}

// Returns the number of events processed by this bucket
func (t *eventBucket) getProcessedCount() int32 {
	return atomic.LoadInt32(t.processedCount)
}

// Returns the event offset of the last processed event
func (t *eventBucket) getProcessedOffset() uint64 {
	return atomic.LoadUint64(t.processedOffset)
}

// Loop to process events queued up in a bucket. Terminates when
// stopCh is notified/closed.
func dequeueEventsFromBucket(
	t StatusProcessor,
	bucket *eventBucket,
	stopCh <-chan struct{},
) {
	for {
		select {
		case event := <-bucket.eventCh:
			for {
				// Retry while getting AlreadyExists error.
				if err := t.ProcessStatusUpdate(
					context.Background(), event); err == nil {
					break
				} else if !common.IsTransientError(err) {
					log.WithError(err).
						WithField("bucket_num", bucket.index).
						WithField("event", event).
						Error("Error applying taskStatus")
					break
				}
				// sleep for a small duration to wait for the error to
				// clear up before retrying
				time.Sleep(_waitForTransientErrorBeforeRetry)
			}

			// Process listeners after handling the event.
			t.ProcessListeners(event)

			atomic.AddInt32(bucket.processedCount, 1)
			atomic.StoreUint64(bucket.processedOffset, event.Offset())
		case <-stopCh:
			log.WithField("bucket_num", bucket.index).Info(
				"Received bucket shutdown")
			return
		}
	}
}

// Creates the event processor
func newBucketEventProcessor(t StatusProcessor, bucketNum int,
	chanSize int) *asyncEventProcessor {
	var buckets []*eventBucket
	for i := 0; i < bucketNum; i++ {
		bucket := newEventBucket(chanSize, i)
		buckets = append(buckets, bucket)
	}
	return &asyncEventProcessor{
		lifecycle:      lifecycle.NewLifeCycle(),
		eventProcessor: t,
		eventBuckets:   buckets,
	}
}

// Enqueue an event for asynchronous processing
func (t *asyncEventProcessor) addV0Event(event *pbeventstream.Event) error {
	updateEvent, err := statusupdate.NewV0(event)
	if err != nil {
		return errors.Wrap(
			err, "failed to convert v0 event to StatusUpdateEvent")
	}

	var taskID string
	if event.Type == pbeventstream.Event_MESOS_TASK_STATUS {
		mesosTaskID := event.MesosTaskStatus.GetTaskId().GetValue()
		taskID, err = util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			return err
		}
	} else if event.Type == pbeventstream.Event_PELOTON_TASK_EVENT {
		// Note: Event_PELOTON_TASK_EVENT is not used
		taskID = event.PelotonTaskEvent.TaskId.Value
		log.WithField("Task ID", taskID).Error("Received Event " +
			"from resmgr")
	}

	_, instanceID, err := util.ParseTaskID(taskID)
	if err != nil {
		log.WithError(err).
			WithField("taskID", taskID).
			Error("Failed to ParseTaskID")
		return err
	}
	index := instanceID % uint32(len(t.eventBuckets))

	t.eventBuckets[index].eventCh <- updateEvent
	return nil
}

func (t *asyncEventProcessor) addV1Event(event *v1pbevent.Event) error {
	updateEvent, err := statusupdate.NewV1(event)
	if err != nil {
		return errors.Wrap(
			err, "failed to convert v1 event to StatusUpdateEvent")
	}
	// shard event to buckets based on podID
	podID := event.GetPodEvent().GetPodId().GetValue()
	h := fnv.New32()
	io.WriteString(h, podID)
	index := h.Sum32() % uint32(len(t.eventBuckets))

	t.eventBuckets[index].eventCh <- updateEvent
	return nil
}

// Starts the event processor. Launches goroutines to process events for
// each bucket.
func (t *asyncEventProcessor) start() {
	if !t.lifecycle.Start() {
		return
	}
	stopCh := t.lifecycle.StopCh()
	for _, bucket := range t.eventBuckets {
		t.bucketsWg.Add(1)
		go func(b *eventBucket) {
			dequeueEventsFromBucket(t.eventProcessor, b, stopCh)
			t.bucketsWg.Done()
		}(bucket)
	}
}

// Completes processing events and terminates the processing loop
// for each bucket.
func (t *asyncEventProcessor) drainAndShutdown() {
	for _, bucket := range t.eventBuckets {
		bucket.drain()
	}
	if t.lifecycle.Stop() {
		t.bucketsWg.Wait()
	}
}

// GetEventProgress returns the current max progress among all buckets
// This value is used to purge data on the event stream server side.
func (t *asyncEventProcessor) GetEventProgress() uint64 {
	var maxOffset uint64
	maxOffset = uint64(0)
	for _, bucket := range t.eventBuckets {
		offset := bucket.getProcessedOffset()
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	return maxOffset
}
