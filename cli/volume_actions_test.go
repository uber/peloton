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
	"time"

	volumemocks "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc/mocks"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type volumeActions struct {
	suite.Suite
	mockCtrl      *gomock.Controller
	mockVolumeSvc *volumemocks.MockVolumeServiceYARPCClient
	ctx           context.Context
	jobID         *peloton.JobID
}

func (suite *volumeActions) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockVolumeSvc = volumemocks.NewMockVolumeServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
}

func TestVolumeHandler(t *testing.T) {
	suite.Run(t, new(volumeActions))
}

func (suite *volumeActions) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

// TestVolumeListAction tests listing the volumes of a job
func (suite *volumeActions) TestVolumeListAction() {
	c := Client{
		Debug:        false,
		volumeClient: suite.mockVolumeSvc,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	req := &svc.ListVolumesRequest{
		JobId: suite.jobID,
	}

	tt := []struct {
		debug bool
		resp  *svc.ListVolumesResponse
		err   error
	}{
		{
			// list volumes with no error
			resp: &svc.ListVolumesResponse{
				Volumes: map[string]*volume.PersistentVolumeInfo{
					"v1": {
						Id: &peloton.VolumeID{
							Value: uuid.NewRandom().String(),
						},
						JobId:      suite.jobID,
						InstanceId: uint32(0),
						Hostname:   "host1",
						State:      volume.VolumeState_CREATED,
						SizeMB:     10,
						CreateTime: time.Now().UTC().Format(time.RFC3339Nano),
						UpdateTime: time.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			},
			err: nil,
		},
		{
			// json
			debug: true,
			resp: &svc.ListVolumesResponse{
				Volumes: map[string]*volume.PersistentVolumeInfo{
					"v1": {
						Id: &peloton.VolumeID{
							Value: uuid.NewRandom().String(),
						},
						JobId:      suite.jobID,
						InstanceId: uint32(0),
						Hostname:   "host1",
						State:      volume.VolumeState_CREATED,
						SizeMB:     10,
						CreateTime: time.Now().UTC().Format(time.RFC3339Nano),
						UpdateTime: time.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			},
			err: nil,
		},
		{
			// list volume returns error
			resp: nil,
			err:  errors.New("did not find any volume"),
		},
		{
			// no volumes found
			resp: &svc.ListVolumesResponse{
				Volumes: map[string]*volume.PersistentVolumeInfo{},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockVolumeSvc.EXPECT().
			ListVolumes(gomock.Any(), req).
			Return(t.resp, t.err)
		if t.err != nil {
			suite.Error(c.VolumeListAction(suite.jobID.Value))
		} else {
			suite.NoError(c.VolumeListAction(suite.jobID.Value))
		}
	}
}

// TestVolumeDeleteAction tests deleting a volume
func (suite *volumeActions) TestVolumeDeleteAction() {
	c := Client{
		Debug:        false,
		volumeClient: suite.mockVolumeSvc,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	volumeID := &peloton.VolumeID{
		Value: uuid.NewRandom().String(),
	}

	req := &svc.DeleteVolumeRequest{
		Id: volumeID,
	}
	resp := &svc.DeleteVolumeResponse{}

	tt := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("did not find update in cache"),
		},
	}

	for _, t := range tt {
		suite.mockVolumeSvc.EXPECT().
			DeleteVolume(gomock.Any(), req).
			Return(resp, t.err)
		if t.err != nil {
			suite.Error(c.VolumeDeleteAction(volumeID.Value))
		} else {
			suite.NoError(c.VolumeDeleteAction(volumeID.Value))
		}
	}
}
