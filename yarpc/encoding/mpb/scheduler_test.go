package mpb

import (
	"bytes"
	"io/ioutil"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"

	transport_mocks "go.uber.org/yarpc/transport/transporttest"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type schedulerClientTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	mockClientCfg   *transport_mocks.MockClientConfig
	defaultEncoding string
}

func (suite *schedulerClientTestSuite) SetupTest() {
	log.Debug("setup")
	ctrl := gomock.NewController(suite.T())
	suite.ctrl = ctrl
	mockClientCfg := transport_mocks.NewMockClientConfig(suite.ctrl)
	suite.mockClientCfg = mockClientCfg
	suite.defaultEncoding = ContentTypeProtobuf
}

func (suite *schedulerClientTestSuite) TearDownTest() {
	log.Debug("tear down")
	if suite.ctrl != nil {
		suite.ctrl.Finish()
	}
}

func (suite *schedulerClientTestSuite) TestSchedulerClient_Call() {
	mockCaller := "testCall"
	mockSvc := "testSvc"
	callType := sched.Call_DECLINE
	mockValidValue := new(string)
	*mockValidValue = uuid.NIL.String()

	mockValidFrameWorkID := &mesos.FrameworkID{
		Value: mockValidValue,
	}

	tests := []struct {
		call     bool
		callMsg  *sched.Call
		callErr  bool
		encoding string
		errMsg   string
		headers  yarpc.Headers
	}{
		{
			callMsg: &sched.Call{
				FrameworkId: mockValidFrameWorkID,
				Type:        &callType,
				Decline: &sched.Call_Decline{
					OfferIds: []*mesos.OfferID{},
				},
			},
			call:     true,
			callErr:  true,
			encoding: suite.defaultEncoding,
			errMsg:   "connection error",
			headers:  yarpc.NewHeaders().With("a", "b"),
		},
		{
			callMsg: &sched.Call{
				FrameworkId: mockValidFrameWorkID,
				Type:        &callType,
				Decline: &sched.Call_Decline{
					OfferIds: []*mesos.OfferID{},
				},
			},
			call:     true,
			callErr:  false,
			encoding: suite.defaultEncoding,
			headers:  yarpc.NewHeaders().With("a", "b"),
		},
		{
			callMsg:  nil,
			call:     false,
			encoding: suite.defaultEncoding,
			errMsg: "failed to marshal subscribe call to contentType x-protobuf " +
				"Failed to marshal subscribe call to x-protobuf: proto: Marshal " +
				"called with nil",
			headers: yarpc.NewHeaders().With("a", "b"),
		},
	}

	for _, tt := range tests {
		if tt.call {
			mockUnaryOutbound := transport_mocks.NewMockUnaryOutbound(
				suite.ctrl,
			)

			var err error
			var response *transport.Response

			if tt.callErr {
				err = errors.New("connection error")
			} else {
				response = &transport.Response{
					Body: ioutil.NopCloser(
						bytes.NewBuffer([]byte{}),
					),
					Headers: transport.Headers(tt.headers),
				}

			}

			// Set expectations
			gomock.InOrder(
				suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
				suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
				suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
					mockUnaryOutbound,
				),

				mockUnaryOutbound.EXPECT().Call(
					gomock.Any(),
					gomock.Any(),
				).Return(
					response,
					err,
				),
			)

		}

		schedClient := NewSchedulerClient(suite.mockClientCfg, tt.encoding)
		err := schedClient.Call("123", tt.callMsg)
		if tt.errMsg != "" {
			suite.EqualError(err, tt.errMsg)
		} else {
			suite.NoError(err)
		}

	}
}

func TestSchedulerClientTestSuite(t *testing.T) {
	suite.Run(t, new(schedulerClientTestSuite))
}
