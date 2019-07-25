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

package cli

import (
	"context"
	"testing"

	adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	adminmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type adminActionsTestSuite struct {
	suite.Suite
	client Client

	mockCtrl  *gomock.Controller
	mockAdmin *adminmocks.MockAdminServiceYARPCClient
	ctx       context.Context
}

func TestAdminActions(t *testing.T) {
	suite.Run(t, new(hostmgrActionsTestSuite))
}

func (suite *adminActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockAdmin = adminmocks.NewMockAdminServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:       false,
		dispatcher:  nil,
		ctx:         suite.ctx,
		adminClient: suite.mockAdmin,
	}
}

func (suite *adminActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *adminActionsTestSuite) TestLockComponentsSuccess() {
	suite.mockAdmin.EXPECT().Lockdown(suite.ctx, &adminsvc.LockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}).Return(&adminsvc.LockdownResponse{
		Successes: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}, nil)

	suite.NoError(suite.client.LockComponents([]string{adminsvc.Component_GoalStateEngine.String()}))
}

func (suite *adminActionsTestSuite) TestLockComponentsFailure() {
	suite.mockAdmin.EXPECT().Lockdown(suite.ctx, &adminsvc.LockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}).Return(&adminsvc.LockdownResponse{
		Failures: []*adminsvc.ComponentFailure{
			{Component: adminsvc.Component_GoalStateEngine, FailureMessage: "test error"},
		},
	}, yarpcerrors.InternalErrorf("test error"))

	suite.NoError(suite.client.LockComponents([]string{adminsvc.Component_GoalStateEngine.String()}))
}

func (suite *adminActionsTestSuite) TestRemoveLockdownSuccess() {
	suite.mockAdmin.EXPECT().RemoveLockdown(suite.ctx, &adminsvc.RemoveLockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}).Return(&adminsvc.RemoveLockdownResponse{
		Successes: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}, nil)

	suite.NoError(suite.client.UnlockComponents([]string{adminsvc.Component_GoalStateEngine.String()}))
}

func (suite *adminActionsTestSuite) TestRemoveLockdownFailure() {
	suite.mockAdmin.EXPECT().RemoveLockdown(suite.ctx, &adminsvc.RemoveLockdownRequest{
		Components: []adminsvc.Component{adminsvc.Component_GoalStateEngine},
	}).Return(&adminsvc.RemoveLockdownResponse{
		Failures: []*adminsvc.ComponentFailure{
			{Component: adminsvc.Component_GoalStateEngine, FailureMessage: "test error"},
		},
	}, yarpcerrors.InternalErrorf("test error"))

	suite.NoError(suite.client.UnlockComponents([]string{adminsvc.Component_GoalStateEngine.String()}))
}
