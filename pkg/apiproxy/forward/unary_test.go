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
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_bodyStr = "outbound call succeeded"
)

// UnaryHandlerTestSuite is test suite for unary handler.
type UnaryHandlerTestSuite struct {
	suite.Suite

	ctrl              *gomock.Controller
	mockUnaryOutbound *transporttest.MockUnaryOutbound

	overrideService string
	handler         *unaryForward
}

// SetupTest is setup function for each test in this suite.
func (suite *UnaryHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockUnaryOutbound = transporttest.NewMockUnaryOutbound(suite.ctrl)

	suite.handler = NewUnaryForward(suite.mockUnaryOutbound, suite.overrideService)
}

// TearDownTest is teardown function for each test in this suite.
func (suite *UnaryHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestUnaryHandlerTestSuite runs UnaryHandlerTestSuite.
func TestUnaryHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(UnaryHandlerTestSuite))
}

// TestHandle tests the Handle implementation of unaryForward.
func (suite *UnaryHandlerTestSuite) TestHandle() {
	testCases := map[string]struct {
		outboundCallResp *transport.Response
		outboundCallErr  error
		expectedErrMsg   string
	}{
		"outbound-call-failure": {
			outboundCallResp: &transport.Response{},
			outboundCallErr:  yarpcerrors.InternalErrorf("some bad things happened"),
			expectedErrMsg:   "code:internal message:some bad things happened",
		},
		"outbound-call-success-with-app-error": {
			outboundCallResp: &transport.Response{
				ApplicationError: true,
			},
		},
		"outbound-call-success-with-no-app-error": {
			outboundCallResp: &transport.Response{},
		},
	}

	originReq := &transport.Request{}

	forwardedReq := &transport.Request{
		Service: suite.overrideService,
	}

	for tcName, tc := range testCases {
		// Setup outbound call response body.
		body := ioutil.NopCloser(strings.NewReader(_bodyStr))
		tc.outboundCallResp.Body = body

		// Ensure preprocess request works.
		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			forwardedReq,
		).Return(tc.outboundCallResp, tc.outboundCallErr)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		w := &transporttest.FakeResponseWriter{}

		err := suite.handler.Handle(ctx, originReq, w)
		if tc.expectedErrMsg != "" {
			suite.EqualErrorf(err, tc.expectedErrMsg, "run test case: %s", tcName)
		} else {
			suite.NoError(err, "run test case: %s", tcName)
			suite.Equal(_bodyStr, w.Body.String(), "run test case: %s", tcName)
		}

		_ = body.Close()
		cancel()
	}
}
