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

package forward

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
	"go.uber.org/yarpc/yarpcerrors"
)

// StreamHandlerTestSuite is test suite for stream handler.
type StreamHandlerTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	mockStreamOutbound *transporttest.MockStreamOutbound

	overrideService string
	handler         *streamForward
}

// SetupTest is setup function for each test in this suite.
func (suite *StreamHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockStreamOutbound = transporttest.NewMockStreamOutbound(suite.ctrl)

	suite.handler = NewStreamForward(suite.mockStreamOutbound, suite.overrideService)
}

// TearDownTest is teardown function for each test in this suite.
func (suite *StreamHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestStreamHandlerTestSuite runs StreamHandlerTestSuite.
func TestStreamHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(StreamHandlerTestSuite))
}

// TestHandle tests the HandleStream implementation of streamForward.
func (suite *StreamHandlerTestSuite) TestHandleStream() {
	forwardedReq := &transport.StreamRequest{
		Meta: &transport.RequestMeta{
			Service: suite.overrideService,
		},
	}

	testCases := map[string]struct {
		serverStream   transport.Stream
		clientStream   transport.StreamCloser
		callStreamErr  error
		expectedErrMsg string
	}{
		"outbound-callstream-failure": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
			},
			clientStream: &fakeStream{},
			callStreamErr: yarpcerrors.InternalErrorf(
				"some bad things happened"),
			expectedErrMsg: "code:internal message:some bad things happened",
		},
		"server-stop-sending": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receiveErr: io.EOF,
			},
			clientStream: &fakeStream{
				ctx:            context.Background(),
				receiveBlocked: true,
				receiveErr:     io.EOF,
			},
		},
		"client-stop-sending": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receiveBlocked: true,
			},
			clientStream: &fakeStream{
				ctx:        context.Background(),
				receiveErr: io.EOF,
			},
		},
		"server-received-rpc-error": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receiveErr: yarpcerrors.InternalErrorf(
					"some bad things happened"),
			},
			clientStream: &fakeStream{
				ctx:            context.Background(),
				receiveBlocked: true,
			},
			expectedErrMsg: "code:internal message:some bad things happened",
		},
		"client-received-rpc-error": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receiveBlocked: true,
			},
			clientStream: &fakeStream{
				ctx: context.Background(),
				receiveErr: yarpcerrors.InternalErrorf(
					"some bad things happened"),
			},
			expectedErrMsg: "code:internal message:some bad things happened",
		},
		"server-sending-failure": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receiveBlocked: true,
				sendErr: yarpcerrors.InternalErrorf(
					"some bad things happened"),
			},
			clientStream: &fakeStream{
				ctx:         context.Background(),
				receivedMsg: &transport.StreamMessage{},
			},
			expectedErrMsg: "code:internal message:some bad things happened",
		},
		"client-sending-failure": {
			serverStream: &fakeStream{
				ctx: context.Background(),
				request: &transport.StreamRequest{
					Meta: &transport.RequestMeta{},
				},
				receivedMsg: &transport.StreamMessage{},
			},
			clientStream: &fakeStream{
				ctx:            context.Background(),
				receiveBlocked: true,
				sendErr: yarpcerrors.InternalErrorf(
					"some bad things happened"),
			},
			expectedErrMsg: "code:internal message:some bad things happened",
		},
	}

	for tcName, tc := range testCases {
		ss, _ := transport.NewServerStream(tc.serverStream)
		cs, _ := transport.NewClientStream(tc.clientStream)

		// Ensure preprocess request works.
		suite.mockStreamOutbound.EXPECT().CallStream(
			gomock.Any(),
			forwardedReq,
		).Return(cs, tc.callStreamErr)

		err := suite.handler.HandleStream(ss)
		if tc.expectedErrMsg != "" {
			suite.EqualErrorf(err, tc.expectedErrMsg,
				"run test case: %s", tcName)
		} else {
			suite.NoError(err,
				"run test case: %s", tcName)
		}
	}
}

// fakeStream implements StreamCloser interface for testing only.
type fakeStream struct {
	ctx            context.Context
	request        *transport.StreamRequest
	receiveBlocked bool
	sendErr        error
	receivedMsg    *transport.StreamMessage
	receiveErr     error
	closeErr       error
}

func (fs *fakeStream) Context() context.Context {
	return fs.ctx
}

func (fs *fakeStream) Request() *transport.StreamRequest {
	return fs.request
}

func (fs *fakeStream) SendMessage(context.Context, *transport.StreamMessage) error {
	return fs.sendErr
}

func (fs *fakeStream) ReceiveMessage(context.Context) (*transport.StreamMessage, error) {
	// Optionally block receiving message to avoid unnecessary receives.
	if fs.receiveBlocked {
		time.Sleep(time.Second)
	}
	return fs.receivedMsg, fs.receiveErr
}

func (fs *fakeStream) Close(context.Context) error {
	return fs.closeErr
}
