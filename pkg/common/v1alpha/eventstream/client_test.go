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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common/lifecycle"

	pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	pbeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	"go.uber.org/yarpc"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

var addEventSleepInterval = 2 * time.Millisecond
var waitEventConsumedInterval = 100 * time.Millisecond

type testClient struct {
	mu          sync.Mutex
	handler     *Handler
	returnError bool
}

// Controls if the client would error out the mock RPC call
func (c *testClient) setErrorFlag(errorFlag bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.returnError = errorFlag
}

func (c *testClient) changeStreamID(streamID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler.streamID = streamID
}

func (c *testClient) InitStream(
	ctx context.Context,
	request *pbeventstreamsvc.InitStreamRequest,
	opts ...yarpc.CallOption,
) (*pbeventstreamsvc.InitStreamResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.returnError {
		return nil, errors.New("Mocked RPC server error")
	}

	return c.handler.InitStream(ctx, request)
}

// WaitForEvents forwards the call to the handler, dropping the options.
func (c *testClient) WaitForEvents(
	ctx context.Context,
	request *pbeventstreamsvc.WaitForEventsRequest,
	opts ...yarpc.CallOption,
) (*pbeventstreamsvc.WaitForEventsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.returnError {
		return nil, errors.New("Mocked RPC server error")
	}

	return c.handler.WaitForEvents(ctx, request)
}

type TestEventProcessor struct {
	sync.Mutex
	events []*pbevent.Event
}

func (p *TestEventProcessor) OnV1Event(event *pbevent.Event) {
	p.Lock()
	defer p.Unlock()
	p.events = append(p.events, event)
}

func (p *TestEventProcessor) OnV1Events(events []*pbevent.Event) {

}

func (p *TestEventProcessor) GetEventProgress() uint64 {
	p.Lock()
	defer p.Unlock()
	if len(p.events) > 0 {
		return p.events[len(p.events)-1].Offset
	}
	return uint64(0)
}

func makeStreamClient(
	clientName string,
	client *testClient,
) (*Client, *TestEventProcessor) {
	eventProcessor := &TestEventProcessor{}
	eventStreamClient := &Client{
		rpcClient:    client,
		eventHandler: eventProcessor,
		clientName:   clientName,
		metrics:      NewClientMetrics(tally.NewTestScope(clientName, map[string]string{})),
		lifeCycle:    lifecycle.NewLifeCycle(),
		log:          log.WithField("client", clientName),
	}
	return eventStreamClient, eventProcessor
}

// Two clients consumes and purges data
func TestConsumeAndPurgeEvents(t *testing.T) {
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
	client := &testClient{handler: handler}
	eventStreamClient1, eventProcessor1 := makeStreamClient(clientName1, client)
	eventStreamClient1.Start()

	eventStreamClient2, eventProcessor2 := makeStreamClient(clientName2, client)
	eventStreamClient2.Start()

	batches := 20
	batchSize := 50
	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", i*batchSize+j)
			handler.AddEvent(makePodEvent(id))
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)
	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	assert.Equal(t, batches*batchSize, len(eventProcessor1.events))
	assert.Equal(t, batches*batchSize, len(eventProcessor2.events))
	assert.Equal(t, batches*batchSize, len(purgedEventCollector.data))

	for i := 0; i < batches*batchSize; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor1.events[i].PodEvent.PodId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor2.events[i].PodEvent.PodId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, batches*batchSize, int(head))
	assert.Equal(t, batches*batchSize, int(tail))

	// client should be able to process events after start -> stop -> start
	eventStreamClient1.Start()
	eventStreamClient2.Start()

	for i := batches; i < 2*batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", i*batchSize+j)
			handler.AddEvent(makePodEvent(id))
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)
	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	for i := batches * batchSize; i < 2*batches*batchSize; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor1.events[i].PodEvent.PodId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor2.events[i].PodEvent.PodId.Value)
	}
	head, tail = handler.circularBuffer.GetRange()
	assert.Equal(t, 2*batches*batchSize, int(head))
	assert.Equal(t, 2*batches*batchSize, int(tail))
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
	client := &testClient{handler: handler}
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
			handler.AddEvent(makePodEvent(id))
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	client.changeStreamID("23456")

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(makePodEvent(id))
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	assert.Equal(t, batches*batchSize*2, len(eventProcessor1.events))
	assert.Equal(t, batches*batchSize*2, len(eventProcessor2.events))
	assert.Equal(t, batches*batchSize*2, len(purgedEventCollector.data))

	for i := 0; i < batches*batchSize*2; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor1.events[i].PodEvent.PodId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor2.events[i].PodEvent.PodId.Value)
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
	client := &testClient{handler: handler}

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
			handler.AddEvent(makePodEvent(id))
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	client.setErrorFlag(true)
	delta := 10

	for i := 0; i < delta; i++ {
		id := fmt.Sprintf("%d", count)
		handler.AddEvent(makePodEvent(id))
		count++
		time.Sleep(addEventSleepInterval)
	}
	time.Sleep(waitEventConsumedInterval)
	client.setErrorFlag(false)
	time.Sleep(waitEventConsumedInterval)

	for i := 0; i < batches; i++ {
		for j := 0; j < batchSize; j++ {
			id := fmt.Sprintf("%d", count)
			handler.AddEvent(makePodEvent(id))
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient1.Stop()
	eventStreamClient2.Stop()

	assert.Equal(t, count, len(eventProcessor1.events))
	assert.Equal(t, count, len(eventProcessor2.events))
	assert.Equal(t, count, len(purgedEventCollector.data))

	for i := 0; i < count; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor1.events[i].PodEvent.PodId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor2.events[i].PodEvent.PodId.Value)
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
	client := &testClient{handler: handler}

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
			handler.AddEvent(makePodEvent(id))
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
		handler.AddEvent(makePodEvent(id))
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
			handler.AddEvent(makePodEvent(id))
			count++
			time.Sleep(addEventSleepInterval)
		}
	}
	time.Sleep(waitEventConsumedInterval)

	eventStreamClient2.Stop()
	eventStreamClient3.Stop()

	assert.Equal(t, count, len(eventProcessor1.events))
	assert.Equal(t, count, len(eventProcessor2.events))
	assert.Equal(t, count, len(purgedEventCollector.data))

	for i := 0; i < count; i++ {
		assert.Equal(t, i, int(eventProcessor1.events[i].Offset))
		assert.Equal(t, i, int(eventProcessor2.events[i].Offset))
		assert.Equal(t, i, int(purgedEventCollector.data[i].SequenceID))
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor1.events[i].PodEvent.PodId.Value)
		assert.Equal(t, fmt.Sprintf("%d", i), eventProcessor2.events[i].PodEvent.PodId.Value)
	}
	head, tail := handler.circularBuffer.GetRange()
	assert.Equal(t, count, int(head))
	assert.Equal(t, count, int(tail))
}
