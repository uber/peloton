package eventstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/yarpc"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
)

var addEventSleepInterval = 2 * time.Millisecond
var waitEventConsumedInterval = 100 * time.Millisecond

type testClient struct {
	sync.Mutex
	localClient *localClient
	returnError bool
}

// Controls if the client would error out the mock RPC call
func (c *testClient) setErrorFlag(errorFlag bool) {
	c.Lock()
	defer c.Unlock()
	c.returnError = errorFlag
}

func (c *testClient) changeStreamID(streamID string) {
	c.localClient.handler.Lock()
	defer c.localClient.handler.Unlock()
	c.localClient.handler.streamID = streamID
}

func (c *testClient) InitStream(
	ctx context.Context,
	request *eventstream.InitStreamRequest,
	opts ...yarpc.CallOption) (*eventstream.InitStreamResponse, error) {
	c.Lock()
	if c.returnError {
		c.Unlock()
		return nil, errors.New("Mocked RPC server error")
	}
	c.Unlock()

	return c.localClient.InitStream(ctx, request, opts...)
}

// WaitForEvents forwards the call to the handler, dropping the options.
func (c *testClient) WaitForEvents(
	ctx context.Context,
	request *eventstream.WaitForEventsRequest,
	opts ...yarpc.CallOption) (*eventstream.WaitForEventsResponse, error) {
	c.Lock()
	if c.returnError {
		c.Unlock()
		return nil, errors.New("Mocked RPC server error")
	}
	c.Unlock()

	return c.localClient.WaitForEvents(ctx, request, opts...)
}

type TestEventProcessor struct {
	sync.Mutex
	events []*eventstream.Event
}

func (p *TestEventProcessor) OnEvent(event *eventstream.Event) {
	p.Lock()
	defer p.Unlock()
	p.events = append(p.events, event)
}

func (p *TestEventProcessor) OnEvents(events []*eventstream.Event) {

}

func (p *TestEventProcessor) GetEventProgress() uint64 {
	p.Lock()
	defer p.Unlock()
	if len(p.events) > 0 {
		return p.events[len(p.events)-1].Offset
	}
	return uint64(0)
}

func makeStreamClient(clientName string, client *testClient) (*Client, *TestEventProcessor) {
	eventProcessor := &TestEventProcessor{}
	var shutdownFlag int32
	var runningState int32
	eventStreamClient := &Client{
		rpcClient:    client,
		shutdownFlag: &shutdownFlag,
		runningState: &runningState,
		eventHandler: eventProcessor,
		clientName:   clientName,
		metrics:      NewClientMetrics(tally.NewTestScope(clientName, map[string]string{})),
	}
	return eventStreamClient, eventProcessor
}

// Two clients consumes and purges data
func TestHappycase(t *testing.T) {
	bufferSize := 100
	clientName1 := "jobMgr"
	clientName2 := "resMgr"
	purgedEventCollector := &PurgeEventCollector{}
	handler := NewEventStreamHandler(
		bufferSize,
		[]string{clientName1, clientName2},
		purgedEventCollector,
		tally.NoopScope,
	)
	client := &testClient{
		localClient: &localClient{
			handler: handler,
		},
	}
	eventStreamClient1, eventProcessor1 := makeStreamClient(clientName1, client)
	eventStreamClient1.Start()

	eventStreamClient2, eventProcessor2 := makeStreamClient(clientName2, client)
	eventStreamClient2.Start()

	batches := 20
	batchSize := 50
	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", i*batchSize+j)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)
	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	eventStreamClient1.Lock()
	defer eventStreamClient1.Unlock()
	eventStreamClient2.Lock()
	defer eventStreamClient2.Unlock()
	purgedEventCollector.Lock()
	defer purgedEventCollector.Unlock()

	assert.Equal(t, batches*batchSize, len(eventProcessor1.events))
	assert.Equal(t, batches*batchSize, len(eventProcessor2.events))
	assert.Equal(t, batches*batchSize, len(purgedEventCollector.data))

	for i := 0; i < batches*batchSize; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor1.events[i].MesosTaskStatus.TaskId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor2.events[i].MesosTaskStatus.TaskId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, batches*batchSize, int(head))
	assert.Equal(t, batches*batchSize, int(tail))
}

// Two clients consumes and purges data; while stream ID changes in between
func TestStreamIDChange(t *testing.T) {
	bufferSize := 123
	clientName1 := "jobMgr"
	clientName2 := "resMgr"
	purgedEventCollector := &PurgeEventCollector{}
	handler := NewEventStreamHandler(
		bufferSize,
		[]string{clientName1, clientName2},
		purgedEventCollector,
		tally.NoopScope,
	)
	client := &testClient{
		localClient: &localClient{
			handler: handler,
		},
	}
	eventStreamClient1, eventProcessor1 := makeStreamClient(clientName1, client)
	eventStreamClient1.Start()

	eventStreamClient2, eventProcessor2 := makeStreamClient(clientName2, client)
	eventStreamClient2.Start()

	count := 0

	batches := 20
	batchSize := 50
	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	client.changeStreamID("23456")

	// Drain existing calls.
	time.Sleep(_maxWaitForEventsIdle)

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	eventStreamClient1.Lock()
	defer eventStreamClient1.Unlock()
	eventStreamClient2.Lock()
	defer eventStreamClient2.Unlock()
	purgedEventCollector.Lock()
	defer purgedEventCollector.Unlock()

	assert.Equal(t, batches*batchSize*2, len(eventProcessor1.events))
	assert.Equal(t, batches*batchSize*2, len(eventProcessor2.events))
	assert.Equal(t, batches*batchSize*2, len(purgedEventCollector.data))

	for i := 0; i < batches*batchSize*2; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor1.events[i].MesosTaskStatus.TaskId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor2.events[i].MesosTaskStatus.TaskId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, batches*batchSize*2, int(head))
	assert.Equal(t, batches*batchSize*2, int(tail))
}

// Two clients consumes and purges data; while for a period of time the rpc client errors out
func TestMockRPCError(t *testing.T) {
	errorRetrySleep = 10 * time.Millisecond
	bufferSize := 367
	clientName1 := "jobMgr"
	clientName2 := "resMgr"
	purgedEventCollector := &PurgeEventCollector{}
	handler := NewEventStreamHandler(
		bufferSize,
		[]string{clientName1, clientName2},
		purgedEventCollector,
		tally.NoopScope,
	)
	client := &testClient{
		localClient: &localClient{
			handler: handler,
		},
	}

	eventStreamClient1, eventProcessor1 := makeStreamClient(clientName1, client)
	eventStreamClient1.Start()

	eventStreamClient2, eventProcessor2 := makeStreamClient(clientName2, client)
	eventStreamClient2.Start()

	count := 0
	batches := 20
	batchSize := 50
	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	client.setErrorFlag(true)
	delta := 10

	for i := 0; i < delta; i++ {
		id := fmt.Sprintf("%d", count)
		handler.AddEvent(&eventstream.Event{
			Type: eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &id,
				}},
		})
		count++
		time.Sleep(addEventSleepInterval)
	}
	time.Sleep(waitEventConsumedInterval)
	client.setErrorFlag(false)
	time.Sleep(waitEventConsumedInterval)

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	eventStreamClient1.Lock()
	defer eventStreamClient1.Unlock()
	eventStreamClient2.Lock()
	defer eventStreamClient2.Unlock()
	purgedEventCollector.Lock()
	defer purgedEventCollector.Unlock()

	assert.Equal(t, count, len(eventProcessor1.events))
	assert.Equal(t, count, len(eventProcessor2.events))
	assert.Equal(t, count, len(purgedEventCollector.data))

	for i := 0; i < count; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor1.events[i].MesosTaskStatus.TaskId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor2.events[i].MesosTaskStatus.TaskId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, count, int(head))
	assert.Equal(t, count, int(tail))
}

// Client 1 fails over and another client with same name is created.
// Validate that the new client can recover from the old client's progress
// and still consume all events.
func TestClientFailover(t *testing.T) {
	errorRetrySleep = 10 * time.Millisecond
	bufferSize := 367
	clientName1 := "jobMgr"
	clientName2 := "resMgr"
	purgedEventCollector := &PurgeEventCollector{}
	handler := NewEventStreamHandler(
		bufferSize,
		[]string{clientName1, clientName2},
		purgedEventCollector,
		tally.NoopScope,
	)
	client := &testClient{
		localClient: &localClient{
			handler: handler,
		},
	}

	eventStreamClient1, eventProcessor1 := makeStreamClient(clientName1, client)
	eventStreamClient1.Start()

	eventStreamClient2, eventProcessor2 := makeStreamClient(clientName2, client)
	eventStreamClient2.Start()

	count := 0
	batches := 20
	batchSize := 50
	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	// Kill eventStreamClient1
	eventStreamClient1.Stop()

	delta := 20
	for i := 0; i < delta; i++ {
		id := fmt.Sprintf("%d", count)
		handler.AddEvent(&eventstream.Event{
			Type: eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &id,
				}},
		})
		count++
		time.Sleep(addEventSleepInterval)
	}

	// Create eventStreamClient3 with the same clientName
	eventStreamClient3, _ := makeStreamClient(clientName1, client)
	eventStreamClient3.eventHandler = eventProcessor1
	eventStreamClient3.Start()

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(&eventstream.Event{
				Type: eventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &id,
					}},
			})
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient2.Stop()
	eventStreamClient3.Stop()

	eventStreamClient1.Lock()
	defer eventStreamClient1.Unlock()
	eventStreamClient2.Lock()
	defer eventStreamClient2.Unlock()
	purgedEventCollector.Lock()
	defer purgedEventCollector.Unlock()

	assert.Equal(t, count, len(eventProcessor1.events))
	assert.Equal(t, count, len(eventProcessor2.events))
	assert.Equal(t, count, len(purgedEventCollector.data))

	for i := 0; i < count; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor1.events[i].MesosTaskStatus.TaskId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), *eventProcessor2.events[i].MesosTaskStatus.TaskId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, count, int(head))
	assert.Equal(t, count, int(tail))
}
