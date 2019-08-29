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

	volume_svc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"

	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// serviceHandler implements peloton.api.volume.VolumeService
type serviceHandler struct {
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
) {

	handler := &serviceHandler{}

	d.Register(volume_svc.BuildVolumeServiceYARPCProcedures(handler))
}

// DeleteVolume implements VolumeService.DeleteVolume.
func (h *serviceHandler) DeleteVolume(
	ctx context.Context,
	req *volume_svc.DeleteVolumeRequest,
) (*volume_svc.DeleteVolumeResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("Unimplemented")
}

// ListVolumes implements VolumeService.ListVolumes.
func (h *serviceHandler) ListVolumes(
	ctx context.Context,
	req *volume_svc.ListVolumesRequest,
) (*volume_svc.ListVolumesResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("Unimplemented")
}

// GetVolume implements VolumeService.GetVolume.
func (h *serviceHandler) GetVolume(
	ctx context.Context,
	req *volume_svc.GetVolumeRequest,
) (*volume_svc.GetVolumeResponse, error) {
	return nil, yarpcerrors.UnimplementedErrorf("Unimplemented")
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
