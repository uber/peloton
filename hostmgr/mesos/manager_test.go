package mesos

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"

	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type managerTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	store   *storage_mocks.MockFrameworkInfoStore
	manager *mesosManager
}

func (suite *managerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.store = storage_mocks.NewMockFrameworkInfoStore(suite.ctrl)
	suite.manager = &mesosManager{
		suite.store,
		_frameworkName,
	}
}

func (suite *managerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *managerTestSuite) TestCallbacks() {
	callbacks := getCallbacks(suite.manager)
	frameworkID := _frameworkID

	testTable := []struct {
		eventType sched.Event_Type
		body      *sched.Event
		mismatch  bool
		mockCalls []*gomock.Call
	}{
		{
			eventType: sched.Event_SUBSCRIBED,
			body: &sched.Event{
				Subscribed: &sched.Event_Subscribed{
					FrameworkId: &mesos.FrameworkID{
						Value: &frameworkID,
					},
				},
			},
			mockCalls: []*gomock.Call{
				suite.store.EXPECT().
					SetMesosFrameworkID(_frameworkName, _frameworkID).
					Return(nil),
			},
		},
		{
			eventType: sched.Event_MESSAGE,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_FAILURE,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_ERROR,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_HEARTBEAT,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_UNKNOWN,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_OFFERS,
			mismatch:  true,
		},
	}

	for _, tt := range testTable {
		s := tt.eventType.String()
		c := callbacks[s]
		if tt.mismatch {
			suite.Nil(c, "Event %s should not have callback", s)
			continue
		}

		suite.NotNil(c, "Event %s has unmatch callback", s)

		if tt.body == nil {
			continue
		}

		if len(tt.mockCalls) > 0 {
			gomock.InOrder(tt.mockCalls...)
		}

		tmp := tt.eventType
		tt.body.Type = &tmp
		err := c(nil, tt.body)
		suite.NoError(err)
	}
}

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(managerTestSuite))
}
