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

package volumesvc

import (
	"context"
	"testing"

	volume_svc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"

	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type VolumeHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler
}

func (suite *VolumeHandlerTestSuite) SetupTest() {
	suite.handler = &serviceHandler{}
}

func TestVolumeHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeHandlerTestSuite))
}

func (suite *VolumeHandlerTestSuite) TestAll() {
	_, err := suite.handler.GetVolume(
		context.Background(),
		&volume_svc.GetVolumeRequest{},
	)
	suite.True(yarpcerrors.IsUnimplemented(err))

	_, err = suite.handler.ListVolumes(
		context.Background(),
		&volume_svc.ListVolumesRequest{},
	)
	suite.True(yarpcerrors.IsUnimplemented(err))

	_, err = suite.handler.DeleteVolume(
		context.Background(),
		&volume_svc.DeleteVolumeRequest{},
	)
	suite.True(yarpcerrors.IsUnimplemented(err))
}
