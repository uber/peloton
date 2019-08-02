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

package adminsvc

import (
	"context"
	"testing"

	adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"

	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	lifecyclemgrmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type adminServiceHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl            *gomock.Controller
	goalStateDriver *goalstatemocks.MockDriver
}

func (suite *adminServiceHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	lockable := lifecyclemgrmocks.NewMockLockable(suite.ctrl)
	suite.goalStateDriver.EXPECT().GetLockable().Return(lockable).AnyTimes()
	suite.handler = createServiceHandler(suite.goalStateDriver, nil)
}

func (suite *adminServiceHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestAdminServiceHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(adminServiceHandlerTestSuite))
}

func (suite *adminServiceHandlerTestSuite) TestLockdownSuccess() {
	suite.goalStateDriver.EXPECT().Stop(false)

	resp, err := suite.handler.Lockdown(context.Background(), &adminsvc.LockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	})
	suite.NoError(err)
	suite.Len(resp.GetSuccesses(), 1)
	suite.Contains(resp.GetSuccesses(), adminsvc.Component_GoalStateEngine)
}

func (suite *adminServiceHandlerTestSuite) TestRemoveLockdownSuccess() {
	suite.goalStateDriver.EXPECT().Start()

	resp, err := suite.handler.RemoveLockdown(context.Background(), &adminsvc.RemoveLockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	})
	suite.NoError(err)
	suite.Len(resp.GetSuccesses(), 1)
	suite.Contains(resp.GetSuccesses(), adminsvc.Component_GoalStateEngine)
}

func (suite *adminServiceHandlerTestSuite) TestLockdownFailDueToUnknownComponent() {
	resp, err := suite.handler.Lockdown(context.Background(), &adminsvc.LockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component(-1)},
	})
	suite.Error(err)
	suite.Empty(resp.GetSuccesses())
}

func (suite *adminServiceHandlerTestSuite) TestRemoveLockdownFailDueToUnknownComponent() {
	resp, err := suite.handler.RemoveLockdown(context.Background(), &adminsvc.RemoveLockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component(-1)},
	})
	suite.Error(err)
	suite.Empty(resp.GetSuccesses())
}
