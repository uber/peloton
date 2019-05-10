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

package inbound

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_testProcedure = "peloton.api.v1alpha.job.stateless.svc.TestService:testProcedure"
)

var _errorCounterNamePattern = fmt.Sprintf("error+error=%%s,procedure=%s", _testProcedure)

type YAPRCMetricsInboundMiddlewareSuite struct {
	suite.Suite

	ctrl *gomock.Controller
	m    *YAPRCMetricsInboundMiddleware
	r    *transport.Request
	s    tally.TestScope
}

func (suite *YAPRCMetricsInboundMiddlewareSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.s = tally.NewTestScope("", nil)
	suite.m = &YAPRCMetricsInboundMiddleware{Scope: suite.s}
	suite.r = &transport.Request{
		Procedure: _testProcedure,
	}
}

func (suite *YAPRCMetricsInboundMiddlewareSuite) TestHandle() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)

	// no error captured on this call
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.m.Handle(context.Background(), suite.r, nil, h)

	// error captured on this call
	internalError := yarpcerrors.InternalErrorf("test error")
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(internalError)
	suite.m.Handle(context.Background(), suite.r, nil, h)

	// error captured on this call
	invalidArugumentError := yarpcerrors.InvalidArgumentErrorf("test error")
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(invalidArugumentError)
	suite.m.Handle(context.Background(), suite.r, nil, h)

	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(internalError)].Value())
	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(invalidArugumentError)].Value())
}

func (suite *YAPRCMetricsInboundMiddlewareSuite) TestHandleOneway() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)

	// no error captured on this call
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.m.HandleOneway(context.Background(), suite.r, h)

	// error captured on this call
	internalError := yarpcerrors.InternalErrorf("test error")
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(internalError)
	suite.m.HandleOneway(context.Background(), suite.r, h)

	// error captured on this call
	invalidArugumentError := yarpcerrors.InvalidArgumentErrorf("test error")
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(invalidArugumentError)
	suite.m.HandleOneway(context.Background(), suite.r, h)

	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(internalError)].Value())
	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(invalidArugumentError)].Value())
}

func (suite *YAPRCMetricsInboundMiddlewareSuite) TestHandleStream() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)

	// no error captured on this call
	h.EXPECT().HandleStream(gomock.Any()).Return(nil)
	suite.m.HandleStream(ss, h)

	// error captured on this call
	internalError := yarpcerrors.InternalErrorf("test error")
	h.EXPECT().HandleStream(gomock.Any()).Return(internalError)
	s.EXPECT().Request().Return(
		&transport.StreamRequest{
			Meta: &transport.RequestMeta{Procedure: _testProcedure, Service: _testService}},
	)
	suite.m.HandleStream(ss, h)

	// error captured on this call
	invalidArugumentError := yarpcerrors.InvalidArgumentErrorf("test error")
	h.EXPECT().HandleStream(gomock.Any()).Return(invalidArugumentError)
	s.EXPECT().Request().Return(
		&transport.StreamRequest{
			Meta: &transport.RequestMeta{Procedure: _testProcedure, Service: _testService}},
	)
	suite.m.HandleStream(ss, h)

	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(internalError)].Value())
	suite.Equal(int64(1), suite.s.Snapshot().Counters()[errorCounterName(invalidArugumentError)].Value())
}

func errorCounterName(err error) string {
	return fmt.Sprintf(_errorCounterNamePattern, errorCode(err))
}

func (suite *YAPRCMetricsInboundMiddlewareSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestYAPRCMetricsInboundMiddlewareSuite(t *testing.T) {
	suite.Run(t, &YAPRCMetricsInboundMiddlewareSuite{})
}
