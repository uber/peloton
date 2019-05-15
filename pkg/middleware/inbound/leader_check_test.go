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

	nomination_mocks "github.com/uber/peloton/pkg/common/leader/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport/transporttest"
)

type LeaderCheckInboundMiddlewareSuite struct {
	suite.Suite

	ctrl       *gomock.Controller
	m          *LeaderCheckInboundMiddleware
	nomination *nomination_mocks.MockNomination
}

func (suite *LeaderCheckInboundMiddlewareSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.nomination = nomination_mocks.NewMockNomination(suite.ctrl)
	suite.m = &LeaderCheckInboundMiddleware{Nomination: suite.nomination}
}

func (suite *LeaderCheckInboundMiddlewareSuite) TestHandle() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)

	// Success Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(true)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.Nil(suite.m.Handle(context.Background(), nil, nil, h))

	// Failure Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(false)
	suite.Error(suite.m.Handle(context.Background(), nil, nil, h))
}

func (suite *LeaderCheckInboundMiddlewareSuite) TestHandleOneway() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)

	// Success Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(true)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.Nil(suite.m.HandleOneway(context.Background(), nil, h))

	// Failure Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(false)
	suite.Error(suite.m.HandleOneway(context.Background(), nil, h))
}

func (suite *LeaderCheckInboundMiddlewareSuite) TestHandleStream() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)

	// Success Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(true)
	h.EXPECT().HandleStream(gomock.Any()).Return(nil)
	suite.Nil(suite.m.HandleStream(nil, h))

	// Failure Case
	suite.nomination.EXPECT().HasGainedLeadership().Return(false)
	suite.Error(suite.m.HandleStream(nil, h))
}

func (suite *LeaderCheckInboundMiddlewareSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestLeaderCheckInboundMiddlewareSuite(t *testing.T) {
	suite.Run(t, &LeaderCheckInboundMiddlewareSuite{})
}
