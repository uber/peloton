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

package outbound

import (
	"context"
	"testing"

	auth_mocks "github.com/uber/peloton/pkg/auth/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
)

type AuthOutboundMiddlewareSuite struct {
	suite.Suite

	ctrl       *gomock.Controller
	m          *AuthOutboundMiddleware
	s          *auth_mocks.MockSecurityClient
	tokenItems map[string]string
}

func (suite *AuthOutboundMiddlewareSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.s = auth_mocks.NewMockSecurityClient(suite.ctrl)
	suite.m = NewAuthOutboundMiddleware(suite.s)
	suite.tokenItems = map[string]string{
		"k1": "v1",
		"k2": "v2",
	}
	suite.s.EXPECT().
		GetToken().
		Return(&testToken{items: suite.tokenItems}).
		AnyTimes()
}

func (suite *AuthOutboundMiddlewareSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *AuthOutboundMiddlewareSuite) TestCallSuccess() {
	out := transporttest.NewMockUnaryOutbound(suite.ctrl)
	out.EXPECT().
		Call(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, request *transport.Request) {
			for k, v := range suite.tokenItems {
				value, ok := request.Headers.Get(k)
				suite.True(ok)
				suite.Equal(v, value)
			}
		}).Return(nil, nil)
	_, err := suite.m.Call(context.Background(), &transport.Request{}, out)
	suite.NoError(err)
}

func (suite *AuthOutboundMiddlewareSuite) TestCallOnewaySuccess() {
	out := transporttest.NewMockOnewayOutbound(suite.ctrl)
	out.EXPECT().
		CallOneway(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, request *transport.Request) {
			for k, v := range suite.tokenItems {
				value, ok := request.Headers.Get(k)
				suite.True(ok)
				suite.Equal(v, value)
			}
		}).Return(nil, nil)
	_, err := suite.m.CallOneway(context.Background(), &transport.Request{}, out)
	suite.NoError(err)
}

func (suite *AuthOutboundMiddlewareSuite) TestCallStreamSuccess() {
	out := transporttest.NewMockStreamOutbound(suite.ctrl)
	out.EXPECT().
		CallStream(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, request *transport.StreamRequest) {
			for k, v := range suite.tokenItems {
				value, ok := request.Meta.Headers.Get(k)
				suite.True(ok)
				suite.Equal(v, value)
			}
		}).Return(nil, nil)
	_, err := suite.m.CallStream(
		context.Background(),
		&transport.StreamRequest{Meta: &transport.RequestMeta{}},
		out,
	)
	suite.NoError(err)
}

func TestAuthOutboundMiddlewareSuite(t *testing.T) {
	suite.Run(t, &AuthOutboundMiddlewareSuite{})
}

type testToken struct {
	items map[string]string
}

func (t *testToken) Get(k string) (string, bool) {
	result, ok := t.items[k]
	return result, ok
}

func (t *testToken) Items() map[string]string {
	return t.items
}

func (t *testToken) Del(k string) {
	delete(t.items, k)
}
