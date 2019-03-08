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

package mesos

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"

	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"
)

type managerTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	store   *storage_mocks.MockFrameworkInfoStore
	manager *mesosManager
}

func (suite *managerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.store = storage_mocks.NewMockFrameworkInfoStore(suite.ctrl)
	suite.manager = &mesosManager{
		suite.store,
		_frameworkName,
	}
}

func (suite *managerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *managerTestSuite) TestCallbacks() {
	callbacks := getCallbacks(suite.manager)
	frameworkID := _frameworkID

	testTable := []struct {
		eventType sched.Event_Type
		body      *sched.Event
		mismatch  bool
		mockCalls []*gomock.Call
	}{
		{
			eventType: sched.Event_SUBSCRIBED,
			body: &sched.Event{
				Subscribed: &sched.Event_Subscribed{
					FrameworkId: &mesos.FrameworkID{
						Value: &frameworkID,
					},
				},
			},
			mockCalls: []*gomock.Call{
				suite.store.EXPECT().
					SetMesosFrameworkID(context.Background(), _frameworkName, _frameworkID).
					Return(nil),
			},
		},
		{
			eventType: sched.Event_MESSAGE,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_FAILURE,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_ERROR,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_HEARTBEAT,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_UNKNOWN,
			body:      &sched.Event{},
		},
		{
			eventType: sched.Event_OFFERS,
			mismatch:  true,
		},
	}

	for _, tt := range testTable {
		s := tt.eventType.String()
		c := callbacks[s]
		if tt.mismatch {
			suite.Nil(c, "Event %s should not have callback", s)
			continue
		}

		suite.NotNil(c, "Event %s has unmatch callback", s)

		if tt.body == nil {
			continue
		}

		if len(tt.mockCalls) > 0 {
			gomock.InOrder(tt.mockCalls...)
		}

		tmp := tt.eventType
		tt.body.Type = &tmp
		err := c(context.Background(), tt.body)
		suite.NoError(err)
	}
}

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(managerTestSuite))
}
