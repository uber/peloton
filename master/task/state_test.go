package task

import (
	"context"
	mesos "mesos/v1"
	pb_eventstream "peloton/private/eventstream"
	"peloton/private/resmgrsvc"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
)

type MockJSONClient struct {
	sync.Mutex
	events      []*pb_eventstream.Event
	returnError bool
}

func (c *MockJSONClient) Call(
	ctx context.Context,
	reqMeta yarpc.CallReqMeta,
	reqBody interface{},
	resBodyOut interface{}) (yarpc.CallResMeta, error) {
	c.Lock()
	defer c.Unlock()
	if c.returnError {
		return nil, errors.New("Mocked RPC server error")
	}
	request, ok := reqBody.(*resmgrsvc.NotifyTaskUpdatesRequest)
	if ok {
		c.events = append(c.events, request.Events...)
		var response resmgrsvc.NotifyTaskUpdatesResponse
		response.PurgeOffset = c.events[len(c.events)-1].Offset
		resBodyOut = &response
	}
	return nil, nil
}

func (c *MockJSONClient) CallOneway(
	ctx context.Context,
	reqMeta yarpc.CallReqMeta,
	reqBody interface{}) (transport.Ack, error) {
	return nil, nil
}

func (c *MockJSONClient) setError(errorFlag bool) {
	c.Lock()
	defer c.Unlock()
	c.returnError = errorFlag
}

func (c *MockJSONClient) validateData(t *testing.T, nEvents int) {
	c.Lock()
	defer c.Unlock()
	assert.Equal(t, nEvents, len(c.events))
	for i := 0; i < nEvents; i++ {
		assert.Equal(t, i, int(c.events[i].Offset))
	}
}

func TestEventForwarder(t *testing.T) {
	errorWaitInterval = 10 * time.Millisecond
	var progress uint64
	c := &MockJSONClient{}
	forwarder := &eventForwarder{
		jsonClient: c,
		progress:   &progress,
	}
	eventStreamHandler := eventstream.NewEventStreamHandler(
		400,
		[]string{common.PelotonJobManager},
		nil,
		tally.NoopScope,
	)

	eventstream.NewLocalEventStreamClient(
		common.PelotonJobManager,
		eventStreamHandler,
		forwarder,
		tally.NoopScope,
	)
	// Add event in event handler, make sure that all get correctly forwarded
	for i := 0; i < 100; i++ {
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	time.Sleep(200 * time.Millisecond)

	c.validateData(t, 100)

	c.setError(true)
	for i := 0; i < 100; i++ {
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	time.Sleep(100 * time.Millisecond)

	c.validateData(t, 100)

	c.setError(false)
	// Simulate temp RPC error recovery, after recovery, all events are
	// still correctly forwarded
	for i := 0; i < 100; i++ {
		eventStreamHandler.AddEvent(&pb_eventstream.Event{
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{},
		})
	}
	time.Sleep(200 * time.Millisecond)

	c.validateData(t, 300)

}
