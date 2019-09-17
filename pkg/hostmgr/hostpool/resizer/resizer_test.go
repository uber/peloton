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

package resizer

import (
	"testing"

	cqosmocks "github.com/uber/peloton/.gen/qos/v1alpha1/mocks"
	poolmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

// ResizerTestSuite is test suite for host pool manager.
type ResizerTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	mockHostPoolMgr *poolmocks.MockHostPoolManager
	mockCQosClient  *cqosmocks.MockQoSAdvisorServiceYARPCClient
}

// SetupTest is setup function for this suite.
func (suite *ResizerTestSuite) SetupSuite() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.mockCQosClient = cqosmocks.NewMockQoSAdvisorServiceYARPCClient(
		suite.ctrl,
	)
	suite.mockHostPoolMgr = poolmocks.NewMockHostPoolManager(suite.ctrl)
}

func (suite *ResizerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestResizerTestSuite runs ResizerTestSuite.
func TestResizerTestSuite(t *testing.T) {
	suite.Run(t, new(ResizerTestSuite))
}

// TestResizerStartStop tests start and stop of resizer instance.
func (suite *ResizerTestSuite) TestResizerStartStop() {
	r := NewResizer(
		suite.mockHostPoolMgr,
		suite.mockCQosClient,
		tally.NoopScope,
	)
	r.Start()
	r.Stop()
}
