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

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"

	"github.com/uber/peloton/pkg/common/cirbuf"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
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

func makeInitStreamRequest(client string) *pb_eventstream.InitStreamRequest {
	return &pb_eventstream.InitStreamRequest{
		ClientName: client,
	}
}

func makeWaitForEventsRequest(
	client string,
	streamID string,
	begin uint64,
	limit int32,
	purgeOffset uint64) *pb_eventstream.WaitForEventsRequest {
	return &pb_eventstream.WaitForEventsRequest{
		ClientName:  client,
		StreamID:    streamID,
		PurgeOffset: purgeOffset,
		Limit:       limit,
		BeginOffset: begin,
	}
}

func makeEvent(uid, tid string) *pb_eventstream.Event {
	var b []byte
	if uid == "" {
		b = []byte(uuid.NewRandom())
	} else {
		b = []byte(uuid.Parse(uid))
	}
	ev := &pb_eventstream.Event{
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos.TaskStatus{Uuid: b},
	}
	if tid != "" {
		ev.MesosTaskStatus.TaskId = &mesos.TaskID{Value: &tid}
	}
	return ev
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
		eventStreamHandler.AddEvent(makeEvent("", ""))
		assert.Equal(t, int64(i+1), testScope.Snapshot().Counters()["EventStreamHandler.api.addEvent+"].Value())
		assert.Equal(t, int64(i+1), testScope.Snapshot().Counters()["EventStreamHandler.addEvent+result=success"].Value())
	}
	items, _ = eventStreamHandler.GetEvents()
	assert.Equal(t, bufferSize/2, len(items))

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makeEvent("", ""))
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
	response, _ := eventStreamHandler.InitStream(context.Background(), makeInitStreamRequest("test"))
	assert.NotNil(t, response.Error.ClientUnsupported)

	// expected client
	response, _ = eventStreamHandler.InitStream(context.Background(), makeInitStreamRequest("jobMgr"))
	assert.Nil(t, response.Error)
	assert.Equal(t, uint64(0), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamID)

	for i := 0; i < 10; i++ {
		eventStreamHandler.AddEvent(makeEvent("", ""))
	}
	eventStreamHandler.circularBuffer.MoveTail(6)

	// MinOffset correctly passed
	response, _ = eventStreamHandler.InitStream(context.Background(), makeInitStreamRequest("jobMgr"))
	assert.Nil(t, response.Error)
	assert.Equal(t, uint64(6), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamID)

	// Unexpected client
	response, _ = eventStreamHandler.InitStream(context.Background(), makeInitStreamRequest("Hostmgr"))
	assert.NotNil(t, response.Error.ClientUnsupported)
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
		eventStreamHandler.AddEvent(makeEvent("", ""))
		assert.Equal(t, int64(i+1), testScope.Snapshot().Counters()["EventStreamHandler.api.addEvent+"].Value())
		assert.Equal(t, int64(i+1), testScope.Snapshot().Counters()["EventStreamHandler.addEvent+result=success"].Value())
	}
	// start and end within head tail range
	request := makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(23), uint64(10))
	response, _ := eventStreamHandler.WaitForEvents(context.Background(), request)
	events := response.Events
	assert.Equal(t, 23, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+10, int(events[i].Offset))
	}

	eventStreamHandler.circularBuffer.MoveTail(20)

	// request with beginOffset and end offset out of buffer range
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(2), uint64(10))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 0, len(events))

	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(1000), int32(2), uint64(10))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 0, len(events))

	// request with beginOffset less than buffer tail
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(25), uint64(9))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 15, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+20, int(events[i].Offset))
	}

	// request with endOffset larger than buffer head
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(35), int32(25), uint64(22))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 8, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+35, int(events[i].Offset))
	}

	// Unsupported client
	request = makeWaitForEventsRequest("test", streamID, uint64(35), int32(25), uint64(22))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, response.Error.ClientUnsupported)

	// Invalid stream id
	request = makeWaitForEventsRequest("jobMgr", "InvalidStreamID", uint64(35), int32(25), uint64(22))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	assert.NotNil(t, response.Error.InvalidStreamID)

	assert.Equal(t, int64(3), testScope.Snapshot().Counters()["EventStreamHandler.waitForEvents+result=success"].Value())
	assert.Equal(t, int64(4), testScope.Snapshot().Counters()["EventStreamHandler.waitForEvents+result=fail"].Value())
	assert.Equal(t, int64(7), testScope.Snapshot().Counters()["EventStreamHandler.api.waitForEvents+"].Value())

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
		eventStreamHandler.AddEvent(makeEvent("", ""))
	}

	// jobMgr consumes some data, with purgeOffset 120
	request := makeWaitForEventsRequest("jobMgr", streamID, uint64(120), int32(50), uint64(120))
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
	request = makeWaitForEventsRequest("resMgr", streamID, uint64(130), int32(40), uint64(130))
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
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(170), int32(20), uint64(170))
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
		eventStreamHandler.AddEvent(makeEvent("", ""))
	}
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))

	// Both resMgr and jobMgr consumes all data
	request = makeWaitForEventsRequest("resMgr", streamID, uint64(299), int32(20), uint64(299))
	response, _ = eventStreamHandler.WaitForEvents(context.Background(), request)
	events = response.Events
	assert.Equal(t, 1, len(events))

	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))
	assert.Equal(t, 170, int(tail))

	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(299), int32(20), uint64(299))
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

func TestDeDupeEvent(t *testing.T) {
	testScope := tally.NewTestScope("", map[string]string{})
	const bufferSize = 2
	var collector PurgeEventCollector
	handler := NewEventStreamHandler(bufferSize, []string{"jobMgr", "resMgr"}, &collector, testScope)
	streamID := handler.streamID

	var uids [bufferSize]string
	var events [bufferSize]*pb_eventstream.Event
	for i := 0; i < bufferSize; i++ {
		uids[i] = uuid.New()
		events[i] = makeEvent(uids[i], "")
		err := handler.AddEvent(events[i])
		assert.Nil(t, err)
	}
	assert.Equal(t, int64(2), testScope.Snapshot().Counters()["EventStreamHandler.api.addEvent+"].Value())
	assert.Equal(t, int64(0), testScope.Snapshot().Counters()["EventStreamHandler.api.addEventDeDupe+"].Value())
	assert.Equal(t, 2, len(handler.eventIndex))

	// add a dupe is not an error
	err := handler.AddEvent(makeEvent(uids[0], ""))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), testScope.Snapshot().Counters()["EventStreamHandler.api.addEventDeDupe+"].Value())
	assert.Equal(t, 2, len(handler.eventIndex))

	// jobmgr consume event 0, but nothing is purged yet
	request := makeWaitForEventsRequest("jobMgr", streamID, 1, 1, 1)
	response, _ := handler.WaitForEvents(context.Background(), request)
	assert.Equal(t, 1, len(response.Events))
	assert.Equal(t, 2, len(handler.eventIndex))

	// add a dupe again
	err = handler.AddEvent(makeEvent(uids[0], ""))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), testScope.Snapshot().Counters()["EventStreamHandler.api.addEventDeDupe+"].Value())
	assert.Equal(t, 2, len(handler.eventIndex))

	// event 0 is purged after resmgr consumes it
	request = makeWaitForEventsRequest("resMgr", streamID, 1, 1, 1)
	response, _ = handler.WaitForEvents(context.Background(), request)
	assert.Equal(t, 1, len(response.Events))
	assert.Equal(t, 1, len(handler.eventIndex))

	// event 0 is not a dupe anymore
	err = handler.AddEvent(makeEvent(uids[0], ""))
	assert.Nil(t, err)
	assert.Equal(t, int64(2), testScope.Snapshot().Counters()["EventStreamHandler.api.addEventDeDupe+"].Value())
	assert.Equal(t, 2, len(handler.eventIndex))
}

func makeHostEvent(hostname string) *pb_eventstream.Event {
	ev := &pb_eventstream.Event{
		Type:      pb_eventstream.Event_HOST_EVENT,
		HostEvent: &pb_host.HostEvent{Hostname: hostname},
	}
	return ev
}

// TestGetEventsHostEvent tests GetEvents for HostEvent
func TestGetEventsHostEvent(t *testing.T) {
	testScope := tally.NewTestScope("", map[string]string{})
	bufferSize := 10
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		nil,
		testScope,
	)

	// Add partial events to the buffer
	for i := 0; i < bufferSize/2; i++ {
		eventStreamHandler.AddEvent(makeHostEvent("h1"))
	}
	items, _ := eventStreamHandler.GetEvents()
	assert.Equal(t, bufferSize/2, len(items))
	for i := 0; i < bufferSize/2; i++ {
		assert.Equal(t, pb_eventstream.Event_HOST_EVENT, items[i].GetType())
	}

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makeHostEvent("h2"))
	}

	items, _ = eventStreamHandler.GetEvents()
	assert.Equal(t, bufferSize, len(items))
	for i := 0; i < bufferSize; i++ {
		assert.Equal(t, pb_eventstream.Event_HOST_EVENT, items[i].GetType())
	}
}

// TestWaitForEventHostEvent tests TestWaitForEvent for HostEvent
func TestWaitForEventHostEvent(t *testing.T) {
	testScope := tally.NewTestScope("", map[string]string{})
	bufferSize := 5
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		nil,
		testScope,
	)

	streamID := eventStreamHandler.streamID

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(makeHostEvent("h1"))
	}
	// start and end within head tail range
	request := makeWaitForEventsRequest(
		"jobMgr",
		streamID,
		uint64(1),
		int32(3),
		uint64(1))
	response, _ := eventStreamHandler.WaitForEvents(
		context.Background(),
		request)
	events := response.Events
	assert.Equal(t, 3, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, pb_eventstream.Event_HOST_EVENT, events[i].GetType())
	}
}
