package eventstream

import (
	"context"
	mesos "mesos/v1"
	pb_eventstream "peloton/private/eventstream"
	"sync"
	"testing"

	"code.uber.internal/infra/peloton/common/cirbuf"
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

func TestInitStream(t *testing.T) {
	purgeEventProcessor := PurgeEventCollector{}
	eventStreamHandler := NewEventStreamHandler(
		100,
		[]string{"jobMgr", "resMgr"},
		&purgeEventProcessor,
		tally.NoopScope,
	)
	// Unexpected client
	response, _, _ := eventStreamHandler.InitStream(context.Background(), nil, makeInitStreamRequest("test"))
	assert.NotNil(t, response.Error.ClientUnsupported)

	// expected client
	response, _, _ = eventStreamHandler.InitStream(context.Background(), nil, makeInitStreamRequest("jobMgr"))
	assert.Nil(t, response.Error)
	assert.Equal(t, uint64(0), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamID)

	for i := 0; i < 10; i++ {
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	eventStreamHandler.circularBuffer.MoveTail(6)

	// MinOffset correctly passed
	response, _, _ = eventStreamHandler.InitStream(context.Background(), nil, makeInitStreamRequest("jobMgr"))
	assert.Nil(t, response.Error)
	assert.Equal(t, uint64(6), response.MinOffset)
	assert.Equal(t, eventStreamHandler.streamID, response.StreamID)

	// Unexpected client
	response, _, _ = eventStreamHandler.InitStream(context.Background(), nil, makeInitStreamRequest("Hostmgr"))
	assert.NotNil(t, response.Error.ClientUnsupported)
}

func TestWaitForEvent(t *testing.T) {
	bufferSize := 43
	eventStreamHandler := NewEventStreamHandler(
		bufferSize,
		[]string{"jobMgr", "resMgr"},
		nil,
		tally.NoopScope,
	)

	streamID := eventStreamHandler.streamID

	// Add some data into the circular buffer
	for i := 0; i < bufferSize; i++ {
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	// start and end within head tail range
	request := makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(23), uint64(10))
	response, _, _ := eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events := response.Events
	assert.Equal(t, 23, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+10, int(events[i].Offset))
	}

	eventStreamHandler.circularBuffer.MoveTail(20)

	// request with beginOffset and end offset out of buffer range
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(2), uint64(10))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events = response.Events
	assert.Equal(t, 0, len(events))

	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(1000), int32(2), uint64(10))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events = response.Events
	assert.Equal(t, 0, len(events))

	// request with beginOffset less than buffer tail
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(10), int32(25), uint64(9))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events = response.Events
	assert.Equal(t, 15, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+20, int(events[i].Offset))
	}

	// request with endOffset larger than buffer head
	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(35), int32(25), uint64(22))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events = response.Events
	assert.Equal(t, 8, len(events))
	for i := 0; i < len(events); i++ {
		assert.Equal(t, i+35, int(events[i].Offset))
	}

	// Unsupported client
	request = makeWaitForEventsRequest("test", streamID, uint64(35), int32(25), uint64(22))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	assert.NotNil(t, response.Error.ClientUnsupported)

	// Invalid stream id
	request = makeWaitForEventsRequest("jobMgr", "InvalidStreamID", uint64(35), int32(25), uint64(22))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	assert.NotNil(t, response.Error.InvalidStreamID)

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
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}

	// jobMgr consumes some data, with purgeOffset 120
	request := makeWaitForEventsRequest("jobMgr", streamID, uint64(120), int32(50), uint64(120))
	response, _, _ := eventStreamHandler.WaitForEvents(context.Background(), nil, request)
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
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
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
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
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
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))

	// Both resMgr and jobMgr consumes all data
	request = makeWaitForEventsRequest("resMgr", streamID, uint64(299), int32(20), uint64(299))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
	events = response.Events
	assert.Equal(t, 1, len(events))

	head, tail = eventStreamHandler.circularBuffer.GetRange()
	assert.Equal(t, 300, int(head))
	assert.Equal(t, 170, int(tail))

	request = makeWaitForEventsRequest("jobMgr", streamID, uint64(299), int32(20), uint64(299))
	response, _, _ = eventStreamHandler.WaitForEvents(context.Background(), nil, request)
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
