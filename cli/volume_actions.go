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
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	volume_svc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"
)

const (
	volumeListFormatHeader = "VolumeID\tJobID\tInstance\tHostname\tState\tGoalState\t" +
		"SizeMB\tContainerPath\tCreateTime\tUpdateTime\t\n"
	volumeListFormatBody = "%s\t%s\t%d\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t\n"
)

func printVolumeListResponse(r *volume_svc.ListVolumesResponse, debug bool) {
	if debug {
		printResponseJSON(r)
		tabWriter.Flush()
		return
	}
	if len(r.GetVolumes()) == 0 {
		fmt.Fprintf(tabWriter, "No volume was found\n")
		return
	}
	fmt.Fprintf(tabWriter, volumeListFormatHeader)
	for _, volume := range r.GetVolumes() {
		// Print the volume record
		fmt.Fprintf(
			tabWriter,
			volumeListFormatBody,
			volume.GetId().GetValue(),
			volume.GetJobId().GetValue(),
			volume.GetInstanceId(),
			volume.GetHostname(),
			volume.GetState(),
			volume.GetGoalState(),
			volume.GetSizeMB(),
			volume.GetContainerPath(),
			volume.GetCreateTime(),
			volume.GetUpdateTime(),
		)
	}
	tabWriter.Flush()
}

// VolumeListAction is the action to list volume for a job.
func (c *Client) VolumeListAction(jobID string) error {
	var request = &volume_svc.ListVolumesRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
	}
	response, err := c.volumeClient.ListVolumes(c.ctx, request)

	if err != nil {
		return err
	}
	printVolumeListResponse(response, c.Debug)
	return nil
}

// VolumeDeleteAction is the action to delete given volume.
func (c *Client) VolumeDeleteAction(volumeID string) error {
	var request = &volume_svc.DeleteVolumeRequest{
		Id: &peloton.VolumeID{
			Value: volumeID,
		},
	}
	response, err := c.volumeClient.DeleteVolume(c.ctx, request)

	if err != nil {
		return err
	}
	printResponseJSON(response)
	return nil
}
