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
	"testing"

	auth_mocks "github.com/uber/peloton/pkg/auth/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
)

const (
	_testService       = "peloton.api.v1alpha.job.stateless.svc.TestService"
	_passwordHeaderKey = "password"
)

type AuthInboundMiddlewareSuite struct {
	suite.Suite

	ctrl *gomock.Controller
	m    *AuthInboundMiddleware
	s    *auth_mocks.MockSecurityManager
	u    *auth_mocks.MockUser
	r    *transport.Request
}

func (suite *AuthInboundMiddlewareSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.s = auth_mocks.NewMockSecurityManager(suite.ctrl)
	suite.u = auth_mocks.NewMockUser(suite.ctrl)
	suite.m = NewAuthInboundMiddleware(suite.s)
	suite.r = &transport.Request{
		Service: _testService,
		Headers: transport.HeadersFromMap(map[string]string{_passwordHeaderKey: "password"}),
	}
}

func (suite *AuthInboundMiddlewareSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *AuthInboundMiddlewareSuite) TestHandleSuccess() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.Handle(context.Background(), suite.r, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleAuthenticateFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(nil, errors.New("test error"))
	suite.Error(suite.m.Handle(context.Background(), suite.r, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandlePermitCheckFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(false)
	suite.Error(suite.m.Handle(context.Background(), suite.r, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleHandlerFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.Handle(context.Background(), suite.r, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewaySuccess() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.HandleOneway(context.Background(), suite.r, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayAuthenticateFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(nil, errors.New("test error"))
	suite.Error(suite.m.HandleOneway(context.Background(), suite.r, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayPermitCheckFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(false)
	suite.Error(suite.m.HandleOneway(context.Background(), suite.r, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayHandlerFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.HandleOneway(context.Background(), suite.r, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleStreamSuccess() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Service: _testService}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleStream(gomock.Any()).Return(nil)
	suite.NoError(suite.m.HandleStream(ss, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleStreamAuthenticateFail() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Service: _testService}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(nil, errors.New("test error"))
	suite.Error(suite.m.HandleStream(ss, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleStreamPermitCheckFail() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Service: _testService}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(false)
	suite.Error(suite.m.HandleStream(ss, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleStreamHandlerFail() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Service: _testService}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.s.EXPECT().RedactToken(gomock.Any()).Return()
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleStream(gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.HandleStream(ss, h))
}

func TestAuthInboundMiddlewareSuite(t *testing.T) {
	suite.Run(t, &AuthInboundMiddlewareSuite{})
}
