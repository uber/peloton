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

package aurorabridge

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
)

type BootstrapperTestSuite struct {
	suite.Suite

	ctrl          *gomock.Controller
	respoolClient *mocks.MockResourceManagerYARPCClient

	config BootstrapConfig

	bootstrapper *Bootstrapper
}

func (suite *BootstrapperTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.respoolClient = mocks.NewMockResourceManagerYARPCClient(suite.ctrl)

	suite.config = BootstrapConfig{
		RespoolPath: "/AuroraBridge",
		DefaultRespoolSpec: DefaultRespoolSpec{
			OwningTeam:  "some-team",
			LDAPGroups:  []string{"some-group"},
			Description: "some description",
			Resources: []*respool.ResourceConfig{{
				Kind:        "cpu",
				Reservation: 12,
				Limit:       12,
				Share:       1,
			}},
			Policy: respool.SchedulingPolicy_PriorityFIFO,
			ControllerLimit: &respool.ControllerLimit{
				MaxPercent: 30,
			},
		},
	}

	suite.bootstrapper = NewBootstrapper(
		suite.config,
		suite.respoolClient,
	)
}

func (suite *BootstrapperTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestBootstrapper(t *testing.T) {
	suite.Run(t, &BootstrapperTestSuite{})
}

func (suite *BootstrapperTestSuite) TestBootstrapRespool_ExistingPool() {
	id := &peloton.ResourcePoolID{Value: "bridge-id"}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: suite.config.RespoolPath},
		}).
		Return(&respool.LookupResponse{
			Id: id,
		}, nil)

	result, err := suite.bootstrapper.BootstrapRespool()
	suite.NoError(err)
	suite.Equal(id.GetValue(), result.GetValue())
}

func (suite *BootstrapperTestSuite) TestBootstrapRespool_NewPoolUsesDefaults() {
	rootID := &peloton.ResourcePoolID{Value: "root-id"}
	id := &peloton.ResourcePoolID{Value: "bridge-id"}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: suite.config.RespoolPath},
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: "/"},
		}).
		Return(&respool.LookupResponse{
			Id: rootID,
		}, nil)

	suite.respoolClient.EXPECT().
		CreateResourcePool(gomock.Any(), &respool.CreateRequest{
			Config: &respool.ResourcePoolConfig{
				Name:            "AuroraBridge",
				OwningTeam:      suite.config.DefaultRespoolSpec.OwningTeam,
				LdapGroups:      suite.config.DefaultRespoolSpec.LDAPGroups,
				Description:     suite.config.DefaultRespoolSpec.Description,
				Resources:       suite.config.DefaultRespoolSpec.Resources,
				Parent:          rootID,
				Policy:          suite.config.DefaultRespoolSpec.Policy,
				ControllerLimit: suite.config.DefaultRespoolSpec.ControllerLimit,
				SlackLimit:      suite.config.DefaultRespoolSpec.SlackLimit,
			},
		}).
		Return(&respool.CreateResponse{
			Result: id,
		}, nil)

	result, err := suite.bootstrapper.BootstrapRespool()
	suite.NoError(err)
	suite.Equal(id.GetValue(), result.GetValue())
}
