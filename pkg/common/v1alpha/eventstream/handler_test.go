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

package eventstream

import (
	"context"
	"sync"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"

	pbpeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	pbeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	"github.com/uber/peloton/pkg/common/cirbuf"
)

type PurgeEventCollector struct {
	sync.RWMutex
	data []*cirbuf.CircularBufferItem
}

func (p *PurgeEventCollector) EventPurged(events []*cirbuf.CircularBufferItem) {
	p.Lock()
	defer p.Unlock()
	p.data = append(p.data, events...)
}

func makeInitStreamRequest(client string) *pbeventstreamsvc.InitStreamRequest {
	return &pbeventstreamsvc.InitStreamRequest{
		ClientName: client,
	}
}

func makeWaitForEventsRequest(
	client string,
	streamID string,
	begin uint64,
	limit int32,
	purgeOffset uint64) *pbeventstreamsvc.WaitForEventsRequest {
	return &pbeventstreamsvc.WaitForEventsRequest{
		ClientName:  client,
		StreamId:    streamID,
		PurgeOffset: purgeOffset,
		Limit:       limit,
		BeginOffset: begin,
	}
}

func makePodEvent(id string) *pbevent.Event {
	if id == "" {
		id = uuid.New()
	}
	return &pbevent.Event{
		PodEvent: &pbpod.PodEvent{
			PodId: &pbpeloton.PodID{Value: id},
		},
		EventId: id,
	}
}

func TestGetEvents(t *testing.T) {
	var testScope = tally.NewTestScope("", map[string]string{})
	bufferSize := 100
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		nil,
		testScope,
	)

	// No events present in the circular buffer
	items, _ := eventStreamHandler.GetEvents()
	assert.Equal(t, 0, len(items))

	// Add partial events to the buffer
	for i := 0; i < bufferSize/2; i++ {
		err := eventStreamHandler.AddEvent(makePodEvent(""))
		assert.Nil(t, err)
		assert.Equal(t, int64(i+1), testScope.Snapshot().
			Counters()["v1alpha_eventstream_handler.api.add_event+"].Value())
		assert.Equal(t, int64(i+1), testScope.Snapshot().
			Counters()["v1alpha_eventstream_handler.add_event+result=success"].Value())
	}
	items, _ = eventStreamHandler.GetEvents()
	assert.Equal(t, bufferSize/2, len(items))

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makePodEvent(""))
	}

	items, _ = eventStreamHandler.GetEvents()
	assert.Equal(t, bufferSize, len(items))
}

func TestInitStream(t *testing.T) {
	purgeEventProcessor := PurgeEventCollector{}
	eventStreamHandler := NewEventStreamHandler(
		100,
		[]string{"jobMgr", "resMgr"},
		&purgeEventProcessor,
		tally.NoopScope,
	)
	// Unexpected client
	_, err := eventStreamHandler.InitStream(
		context.Background(), makeInitStreamRequest("test"))
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsInvalidArgument(err))

	// expected client
	response, err := eventStreamHandler.InitStream(
		context.Background(), makeInitStreamRequest("jobMgr"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamId)

	for i := 0; i < 10; i++ {
		eventStreamHandler.AddEvent(makePodEvent(""))
	}
	eventStreamHandler.circularBuffer.MoveTail(6)

	// MinOffset correctly passed
	response, err = eventStreamHandler.InitStream(
		context.Background(), makeInitStreamRequest("jobMgr"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(6), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamId)

	// Unexpected client
	_, err = eventStreamHandler.InitStream(
		context.Background(), makeInitStreamRequest("Hostmgr"))
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsInvalidArgument(err))
}

func TestWaitForEvent(t *testing.T) {
	var testScope = tally.NewTestScope("", map[string]string{})
	bufferSize := 43
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		nil,
		testScope,
	)

	streamID := eventStreamHandler.streamID

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makePodEvent(""))
		assert.Equal(t, int64(i+1), testScope.Snapshot().
			Counters()["v1alpha_eventstream_handler.api.add_event+"].Value())
		assert.Equal(t, int64(i+1), testScope.Snapshot().
			Counters()["v1alpha_eventstream_handler.add_event+result=success"].Value())
	}
	// start and end within head tail range
	request := makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(10), int32(23), uint64(10))
	response, _ := eventStreamHandler.WaitForEvents(context.Background(), request)
	events := response.Events
	assert.Equal(t, 23, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+10, int(events[i].Offset))
	}

	eventStreamHandler.circularBuffer.MoveTail(20)

	// request with beginOffset and end offset out of buffer range
	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(10), int32(2), uint64(10))
	_, err := eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsOutOfRange(err))

	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(1000), int32(2), uint64(10))
	_, err = eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsOutOfRange(err))

	// request with beginOffset less than buffer tail
	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(10), int32(25), uint64(9))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 15, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+20, int(events[i].Offset))
	}

	// request with endOffset larger than buffer head
	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(35), int32(25), uint64(22))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 8, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+35, int(events[i].Offset))
	}

	// Unsupported client
	request = makeWaitForEventsRequest(
		"test", streamID, uint64(35), int32(25), uint64(22))
	_, err = eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsInvalidArgument(err))

	// Invalid stream id
	request = makeWaitForEventsRequest(
		"jobMgr", "InvalidStreamID", uint64(35), int32(25), uint64(22))
	_, err = eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, err)
	assert.True(t, yarpcerrors.IsInvalidArgument(err))

	assert.Equal(t, int64(3), testScope.Snapshot().
		Counters()["v1alpha_eventstream_handler.wait_for_events+result=success"].Value())
	assert.Equal(t, int64(4), testScope.Snapshot().
		Counters()["v1alpha_eventstream_handler.wait_for_events+result=fail"].Value())
	assert.Equal(t, int64(7), testScope.Snapshot().
		Counters()["v1alpha_eventstream_handler.api.wait_for_events+"].Value())
}

func TestPurgeData(t *testing.T) {
	bufferSize := 221
	collector := &PurgeEventCollector{}
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		collector,
		tally.NoopScope,
	)

	streamID := eventStreamHandler.streamID

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makePodEvent(""))
	}

	// jobMgr consumes some data, with purgeOffset 120
	request := makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(120), int32(50), uint64(120))
	response, _ := eventStreamHandler.WaitForEvents(context.Background(), request)
	events := response.Events
	assert.Equal(t, 50, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+120, int(events[i].Offset))
	}
	// Since no progress from resMgr, tail not moved
	head, tail := eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 0, int(tail))
	assert.Equal(t, bufferSize, int(head))

	// regMgr consumes some data
	request = makeWaitForEventsRequest(
		"resMgr", streamID, uint64(130), int32(40), uint64(130))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 40, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+130, int(events[i].Offset))
	}
	// Tail should be 120 which is min of purge offset between resMgr and jobMgr
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 120, int(tail))
	assert.Equal(t, bufferSize, int(head))

	// jobMgr consumes more data
	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(170), int32(20), uint64(170))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 20, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+170, int(events[i].Offset))
	}
	// tail would move to 130
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 130, int(tail))
	assert.Equal(t, bufferSize, int(head))

	// add more data until 300, rolled over the buffer
	for i := 0; i < 300-bufferSize; i++ {
		eventStreamHandler.AddEvent(makePodEvent(""))
	}
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))

	// Both resMgr and jobMgr consumes all data
	request = makeWaitForEventsRequest(
		"resMgr", streamID, uint64(299), int32(20), uint64(299))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 1, len(events))

	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))
	assert.Equal(t, 170, int(tail))

	request = makeWaitForEventsRequest(
		"jobMgr", streamID, uint64(299), int32(20), uint64(299))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 1, len(events))
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))
	assert.Equal(t, 299, int(tail))

	assert.Equal(t, 299, len(collector.data))
	for i := 0; i < len(collector.data); i++ {
		assert.Equal(t, i, int(collector.data[i].SequenceID))
	}
}
