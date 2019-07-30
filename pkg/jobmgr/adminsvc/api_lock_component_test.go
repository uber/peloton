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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	"testing"

	inboundmocks "github.com/uber/peloton/pkg/middleware/inbound/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type APILockComponentTestSuite struct {
	suite.Suite

	readAPILockComponent  *readAPILockComponent
	writeAPILockComponent *writeAPILockComponent

	ctrl    *gomock.Controller
	apiLock *inboundmocks.MockAPILockInterface
}

func (suite *APILockComponentTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.apiLock = inboundmocks.NewMockAPILockInterface(suite.ctrl)
	suite.readAPILockComponent = &readAPILockComponent{apiLock: suite.apiLock}
	suite.writeAPILockComponent = &writeAPILockComponent{apiLock: suite.apiLock}
}

func (suite *APILockComponentTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestAPILockComponentTestSuite(t *testing.T) {
	suite.Run(t, new(APILockComponentTestSuite))
}

func (suite *APILockComponentTestSuite) TestReadAPILockComponent() {
	suite.apiLock.EXPECT().LockRead()
	suite.apiLock.EXPECT().UnlockRead()

	suite.NoError(suite.readAPILockComponent.lock())
	suite.NoError(suite.readAPILockComponent.unlock())
	suite.Equal(suite.readAPILockComponent.component(), svc.Component_Read)
}

func (suite *APILockComponentTestSuite) TestWriteAPILockComponent() {
	suite.apiLock.EXPECT().LockWrite()
	suite.apiLock.EXPECT().UnlockWrite()

	suite.NoError(suite.writeAPILockComponent.lock())
	suite.NoError(suite.writeAPILockComponent.unlock())
	suite.Equal(suite.writeAPILockComponent.component(), svc.Component_Write)
}
