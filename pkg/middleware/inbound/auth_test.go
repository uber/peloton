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

type AuthInboundMiddlewareSuite struct {
	suite.Suite

	ctrl *gomock.Controller
	m    DispatcherInboundMiddleWare
	s    *auth_mocks.MockSecurityManager
	u    *auth_mocks.MockUser
}

func (suite *AuthInboundMiddlewareSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.s = auth_mocks.NewMockSecurityManager(suite.ctrl)
	suite.u = auth_mocks.NewMockUser(suite.ctrl)
	suite.m = NewAuthInboundMiddleware(suite.s)
}

func (suite *AuthInboundMiddlewareSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *AuthInboundMiddlewareSuite) TestHandleSuccess() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.Handle(context.Background(), &transport.Request{}, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleAuthenticateFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(nil, errors.New("test error"))
	suite.Error(suite.m.Handle(context.Background(), &transport.Request{}, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandlePermitCheckFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(false)
	suite.Error(suite.m.Handle(context.Background(), &transport.Request{}, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleHandlerFail() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.Handle(context.Background(), &transport.Request{}, nil, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewaySuccess() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.HandleOneway(context.Background(), &transport.Request{}, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayAuthenticateFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(nil, errors.New("test error"))
	suite.Error(suite.m.HandleOneway(context.Background(), &transport.Request{}, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayPermitCheckFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(false)
	suite.Error(suite.m.HandleOneway(context.Background(), &transport.Request{}, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleOnewayHandlerFail() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.HandleOneway(context.Background(), &transport.Request{}, h))
}

func (suite *AuthInboundMiddlewareSuite) TestHandleStreamSuccess() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
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
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{}}).
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
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
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
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{}}).
		MinTimes(1)
	suite.s.EXPECT().Authenticate(gomock.Any()).Return(suite.u, nil)
	suite.u.EXPECT().IsPermitted(gomock.Any()).Return(true)
	h.EXPECT().HandleStream(gomock.Any()).Return(errors.New("test error"))
	suite.Error(suite.m.HandleStream(ss, h))
}

func TestAuthInboundMiddlewareSuite(t *testing.T) {
	suite.Run(t, &AuthInboundMiddlewareSuite{})
}
