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
	"github.com/golang/mock/gomock"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/time/rate"
)

type RateLimitInboundMiddlewareTestSuite struct {
	suite.Suite

	r    *transport.Request
	ctrl *gomock.Controller
}

func (suite *RateLimitInboundMiddlewareTestSuite) SetupTest() {
	suite.r = &transport.Request{
		Procedure: "testService::get1",
	}
	suite.ctrl = gomock.NewController(suite.T())
}

func (suite *RateLimitInboundMiddlewareTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestAllow tests the normal case of rate limit config
func (suite *RateLimitInboundMiddlewareTestSuite) TestAllow() {
	config := RateLimitConfig{
		Enabled: true,
		Methods: []struct {
			Name        string
			TokenBucket `yaml:",inline"`
		}{
			{Name: "testService:get1", TokenBucket: TokenBucket{Rate: rate.Inf, Burst: 1}},
			{Name: "testService:get2", TokenBucket: TokenBucket{Rate: rate.Inf, Burst: 1}},
			{Name: "testService:get3", TokenBucket: TokenBucket{Rate: rate.Inf, Burst: 1}},
			{Name: "testService:read*", TokenBucket: TokenBucket{Rate: rate.Inf, Burst: 1}},
			{Name: "testService1:*", TokenBucket: TokenBucket{Rate: rate.Inf, Burst: 1}},
		},
		// anything fall into default would be rejected
		Default: &TokenBucket{Rate: 0, Burst: 0},
	}

	tests := []struct {
		procedure string
		allow     bool
	}{
		{procedure: "testService::get1", allow: true},
		{procedure: "testService::get2", allow: true},
		{procedure: "testService::get3", allow: true},
		{procedure: "testService::get4", allow: false},
		{procedure: "testService::read1", allow: true},
		{procedure: "testService::read2", allow: true},
		{procedure: "testService::read3", allow: true},
		{procedure: "testService::list", allow: false},
		{procedure: "testService::list1", allow: false},
		{procedure: "testService1::get1", allow: true},
		{procedure: "testService1::list", allow: true},
		{procedure: "testService2::get1", allow: false},
		{procedure: "testService2::get2", allow: false},
	}

	mw, err := NewRateLimitInboundMiddleware(config)
	suite.NoError(err)

	for _, test := range tests {
		suite.Equal(mw.allow(test.procedure), test.allow, test.procedure)
	}
}

// TestAllowDefaultConfig tests whether all methods are allowed given the
// default config
func (suite *RateLimitInboundMiddlewareTestSuite) TestAllowDefaultConfig() {
	tests := []struct {
		procedure string
		allow     bool
	}{
		{procedure: "testService::get1", allow: true},
		{procedure: "testService::get2", allow: true},
		{procedure: "testService::get3", allow: true},
		{procedure: "testService::get4", allow: true},
		{procedure: "testService::read1", allow: true},
		{procedure: "testService::read2", allow: true},
		{procedure: "testService::read3", allow: true},
		{procedure: "testService::list", allow: true},
		{procedure: "testService::list1", allow: true},
		{procedure: "testService1::get1", allow: true},
		{procedure: "testService1::list", allow: true},
		{procedure: "testService2::get1", allow: true},
		{procedure: "testService2::get2", allow: true},
	}

	mw, err := NewRateLimitInboundMiddleware(RateLimitConfig{})
	suite.NoError(err)

	for _, test := range tests {
		suite.Equal(mw.allow(test.procedure), test.allow, test.procedure)
	}
}

// TestHandleSuccess tests handle method passes rate limit check successfully
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleSuccess() {
	mw, err := NewRateLimitInboundMiddleware(RateLimitConfig{})
	suite.NoError(err)

	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(mw.Handle(context.Background(), suite.r, nil, h))
}

// TestHandleFailure tests handle method passing rate limit check failed
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleFailure() {
	mw, err := NewRateLimitInboundMiddleware(
		RateLimitConfig{Enabled: true, Default: &TokenBucket{Rate: 0, Burst: 0}})
	suite.NoError(err)

	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.Error(mw.Handle(context.Background(), suite.r, nil, h))
}

// TestHandleOnewaySuccess tests handleOneWay method passes rate limit check successfully
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleOnewaySuccess() {
	mw, err := NewRateLimitInboundMiddleware(RateLimitConfig{Enabled: true})
	suite.NoError(err)

	h := transporttest.NewMockOnewayHandler(suite.ctrl)

	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(mw.HandleOneway(context.Background(), suite.r, h))
}

// TestHandleOnewayFailure tests handleOneWay method passing rate limit check failed
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleOnewayFailure() {
	mw, err := NewRateLimitInboundMiddleware(
		RateLimitConfig{Enabled: true, Default: &TokenBucket{Rate: 0, Burst: 0}})
	suite.NoError(err)

	h := transporttest.NewMockOnewayHandler(suite.ctrl)

	suite.Error(mw.HandleOneway(context.Background(), suite.r, h))
}

// TestHandleStreamSuccess tests handleStream method passes rate limit check successfully
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleStreamSuccess() {
	mw, err := NewRateLimitInboundMiddleware(RateLimitConfig{})
	suite.NoError(err)

	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)

	s.EXPECT().Request().Return(
		&transport.StreamRequest{
			Meta: &transport.RequestMeta{Procedure: suite.r.Procedure}},
	)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	h.EXPECT().HandleStream(gomock.Any()).Return(nil)
	suite.NoError(mw.HandleStream(ss, h))
}

// TestHandleStreamFailure tests handleOneWay method passing rate limit check failed
func (suite *RateLimitInboundMiddlewareTestSuite) TestHandleStreamFailure() {
	mw, err := NewRateLimitInboundMiddleware(
		RateLimitConfig{Enabled: true, Default: &TokenBucket{Rate: 0, Burst: 0}})
	suite.NoError(err)

	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)

	s.EXPECT().Request().Return(
		&transport.StreamRequest{
			Meta: &transport.RequestMeta{Procedure: suite.r.Procedure}},
	)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	suite.Error(mw.HandleStream(ss, h))
}

func TestRateLimitInboundMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, &RateLimitInboundMiddlewareTestSuite{})
}
