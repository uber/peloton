package cli

import (
	"context"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	hostMocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type eventStreamTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostMgr *hostMocks.MockInternalHostServiceYARPCClient
	ctx         context.Context
}

func (suite *eventStreamTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = hostMocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *eventStreamTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *eventStreamTestSuite) TestNoStatusUpdates() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	resp := &hostsvc.GetStatusUpdateEventsResponse{
		Events: nil,
		Error: &hostsvc.GetStatusUpdateEventsResponse_Error{
			Message: "no pending status updates present",
		},
	}

	suite.mockHostMgr.EXPECT().GetStatusUpdateEvents(
		gomock.Any(),
		&hostsvc.GetStatusUpdateEventsRequest{}).Return(resp, nil)

	suite.NoError(c.EventStreamAction())

	suite.mockHostMgr.EXPECT().GetStatusUpdateEvents(
		gomock.Any(),
		&hostsvc.GetStatusUpdateEventsRequest{}).
		Return(&hostsvc.GetStatusUpdateEventsResponse{},
			errors.New("unable to get status update events"))

	suite.NoError(c.EventStreamAction())
}

func (suite *eventStreamTestSuite) TestEventStatusUpdates() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	var events []*pb_eventstream.Event
	for i := 0; i < 5; i++ {
		events = append(events, createEvent(uuid.New(), i+1))
	}

	resp := &hostsvc.GetStatusUpdateEventsResponse{
		Events: events,
		Error:  nil,
	}

	suite.mockHostMgr.EXPECT().GetStatusUpdateEvents(
		gomock.Any(),
		&hostsvc.GetStatusUpdateEventsRequest{}).Return(resp, nil)

	suite.NoError(c.EventStreamAction())
}

func TestEventStreamAction(t *testing.T) {
	suite.Run(t, new(eventStreamTestSuite))
}

func createEvent(_uuid string, offset int) *pb_eventstream.Event {
	state := mesos.TaskState_TASK_STARTING
	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_uuid,
		},
		State: &state,
		Uuid:  []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38},
		AgentId: &mesos.AgentID{
			Value: &_uuid,
		},
	}

	taskStatusUpdate := &sched.Event{
		Update: &sched.Event_Update{
			Status: status,
		},
	}

	return &pb_eventstream.Event{
		Offset:          uint64(offset),
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: taskStatusUpdate.GetUpdate().GetStatus(),
	}
}
