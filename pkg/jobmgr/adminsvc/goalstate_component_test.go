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
	"testing"

	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type goalStateComponentTestSuite struct {
	suite.Suite

	mockCtrl *gomock.Controller
	driver   *goalstatemocks.MockDriver

	goalStateComponent *goalStateComponent
}

func (suite *goalStateComponentTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.driver = goalstatemocks.NewMockDriver(suite.mockCtrl)
	suite.goalStateComponent = &goalStateComponent{driver: suite.driver}
}

func (suite *goalStateComponentTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestGoalStateComponentTestSuite(t *testing.T) {
	suite.Run(t, new(goalStateComponentTestSuite))
}

func (suite *goalStateComponentTestSuite) TestLock() {
	suite.driver.EXPECT().Stop(false)

	suite.NoError(suite.goalStateComponent.lock())
}

func (suite *goalStateComponentTestSuite) TestUnlock() {
	suite.driver.EXPECT().Start()

	suite.NoError(suite.goalStateComponent.unlock())
}
