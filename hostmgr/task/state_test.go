package task

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/yarpc"

	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type MockClient struct {
	sync.Mutex
	events      []*pb_eventstream.Event
	returnError bool
}

func (c *MockClient) EnqueueGangs(context.Context, *resmgrsvc.EnqueueGangsRequest, ...yarpc.CallOption) (*resmgrsvc.EnqueueGangsResponse, error) {
	return nil, nil
}

func (c *MockClient) DequeueTasks(context.Context, *resmgrsvc.DequeueTasksRequest, ...yarpc.CallOption) (*resmgrsvc.DequeueTasksResponse, error) {
	return nil, nil
}

func (c *MockClient) SetPlacements(context.Context, *resmgrsvc.SetPlacementsRequest, ...yarpc.CallOption) (*resmgrsvc.SetPlacementsResponse, error) {
	return nil, nil
}

func (c *MockClient) GetPlacements(context.Context, *resmgrsvc.GetPlacementsRequest, ...yarpc.CallOption) (*resmgrsvc.GetPlacementsResponse, error) {
	return nil, nil
}

func (c *MockClient) NotifyTaskUpdates(ctx context.Context, request *resmgrsvc.NotifyTaskUpdatesRequest, opts ...yarpc.CallOption) (*resmgrsvc.NotifyTaskUpdatesResponse, error) {
	c.Lock()
	defer c.Unlock()
	if c.returnError {
		return nil, errors.New("Mocked RPC server error")
	}
	c.events = append(c.events, request.Events...)
	response := &resmgrsvc.NotifyTaskUpdatesResponse{}
	response.PurgeOffset = c.events[len(c.events)-1].Offset
	return response, nil
}

func (c *MockClient) setError(errorFlag bool) {
	c.Lock()
	defer c.Unlock()
	c.returnError = errorFlag
}

func (c *MockClient) validateData(t *testing.T, nEvents int) {
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
	c := &MockClient{}
	forwarder := &eventForwarder{
		client:   c,
		progress: &progress,
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
