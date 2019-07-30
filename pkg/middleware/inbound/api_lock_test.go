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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
)

const (
	readAPIConfig  = "testService:Read"
	writeAPIConfig = "testService:Write"

	readAPI = "testService::Read"
)

type apiLockInboundMiddlewareTestSuite struct {
	suite.Suite

	ctrl *gomock.Controller

	m *APILockInboundMiddleware
	r *transport.Request
}

func (suite *apiLockInboundMiddlewareTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.m = NewAPILockInboundMiddleware(&APILockConfig{
		ReadAPIs:  []string{readAPIConfig},
		WriteAPIs: []string{writeAPIConfig},
	})
}

func (suite *apiLockInboundMiddlewareTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestAPILockInboundMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(apiLockInboundMiddlewareTestSuite))
}

// TestReadLock tests read lock on API
func (suite *apiLockInboundMiddlewareTestSuite) TestReadLock() {
	// no lock at beginning
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())

	// read lock
	suite.m.LockRead()
	suite.False(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.True(suite.m.hasReadLock())

	// read unlock
	suite.m.UnlockRead()
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())
}

// TestWriteLock tests write lock on API
func (suite *apiLockInboundMiddlewareTestSuite) TestWriteLock() {
	// no lock at beginning
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())

	// write lock
	suite.m.LockWrite()
	suite.False(suite.m.hasNoLock())
	suite.True(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())

	// write unlock
	suite.m.UnlockWrite()
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())
}

// TestReadWriteLock tests mixed use of Read/Write
func (suite *apiLockInboundMiddlewareTestSuite) TestReadWriteLock() {
	// no lock at beginning
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())

	// read lock
	suite.m.LockRead()
	suite.False(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.True(suite.m.hasReadLock())

	// both read and write lock
	suite.m.LockWrite()
	suite.False(suite.m.hasNoLock())
	suite.True(suite.m.hasWriteLock())
	suite.True(suite.m.hasReadLock())

	// write unlock, read still locked
	suite.m.UnlockWrite()
	suite.False(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.True(suite.m.hasReadLock())

	// read unlock
	suite.m.UnlockRead()
	suite.True(suite.m.hasNoLock())
	suite.False(suite.m.hasWriteLock())
	suite.False(suite.m.hasReadLock())
}

// TestHandleSuccess tests the success case of Handle call
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleSuccess() {
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	h.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.Handle(context.Background(), &transport.Request{
		Procedure: readAPI,
	}, nil, h))

}

// TestHandleOnewaySuccess tests the success case of HandleOneway call
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleOnewaySuccess() {
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	h.EXPECT().HandleOneway(gomock.Any(), gomock.Any()).Return(nil)
	suite.NoError(suite.m.HandleOneway(context.Background(), &transport.Request{
		Procedure: readAPI,
	}, h))
}

// TestHandleStreamSuccess tests the success case of HandleStream call
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleStreamSuccess() {
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Procedure: readAPI}}).
		MinTimes(1)
	h.EXPECT().HandleStream(gomock.Any()).Return(nil)
	suite.NoError(suite.m.HandleStream(ss, h))
}

// TestHandleFailOnLock tests fail to lock upon Handle
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleFailOnLock() {
	suite.m.LockRead()
	h := transporttest.NewMockUnaryHandler(suite.ctrl)
	suite.Error(suite.m.Handle(context.Background(), &transport.Request{
		Procedure: readAPI,
	}, nil, h))

}

// TestHandleOnewayFailOnLock tests fail to lock upon HandleOneway
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleOnewayFailOnLock() {
	suite.m.LockRead()
	h := transporttest.NewMockOnewayHandler(suite.ctrl)
	suite.Error(suite.m.HandleOneway(context.Background(), &transport.Request{
		Procedure: readAPI,
	}, h))
}

// TestHandleStreamFailOnLock tests fail to lock upon HandleStream
func (suite *apiLockInboundMiddlewareTestSuite) TestHandleStreamFailOnLock() {
	suite.m.LockRead()
	h := transporttest.NewMockStreamHandler(suite.ctrl)
	s := transporttest.NewMockStream(suite.ctrl)
	ss, err := transport.NewServerStream(s)
	suite.NoError(err)
	s.EXPECT().
		Request().
		Return(&transport.StreamRequest{Meta: &transport.RequestMeta{Procedure: readAPI}}).
		MinTimes(1)
	suite.Error(suite.m.HandleStream(ss, h))
}
