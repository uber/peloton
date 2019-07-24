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

package v0_testutil

import (
	"fmt"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/pborman/uuid"
)

// SetupHostOffer creates a v0 host offer for testing.
func SetupHostOffer() *hostsvc.HostOffer {
	attribute := "attribute"
	text := "text"
	cpuName := "cpus"
	memoryName := "mem"
	diskName := "disk"
	gpuName := "gpus"
	ports := "ports"
	cpuValue := 48.0
	gpuValue := 128.0
	memoryValue := 128.0 * 1024.0
	diskValue := 6.0 * 1024.0 * 1024.0
	scalar := 1.0
	begin := uint64(31000)
	end := uint64(31009)
	textType := mesos_v1.Value_TEXT
	scalarType := mesos_v1.Value_SCALAR
	rangesType := mesos_v1.Value_RANGES
	offerID := fmt.Sprintf("host-offer-id-%s", uuid.NewUUID().String())
	return &hostsvc.HostOffer{
		Id:       &peloton.HostOfferID{Value: offerID},
		Hostname: "hostname",
		Attributes: []*mesos_v1.Attribute{
			{
				Name: &attribute,
				Type: &textType,
				Text: &mesos_v1.Value_Text{
					Value: &text,
				},
			},
			{
				Name: &attribute,
				Type: &scalarType,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
			{
				Name: &attribute,
				Type: &rangesType,
				Ranges: &mesos_v1.Value_Ranges{
					Range: []*mesos_v1.Value_Range{
						{
							Begin: &begin,
							End:   &end,
						},
					},
				},
			},
		},
		Resources: []*mesos_v1.Resource{
			{
				Name: &cpuName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &cpuValue,
				},
			},
			{
				Name: &memoryName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &memoryValue,
				},
			},
			{
				Name: &diskName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &diskValue,
				},
			},
			{
				Name: &gpuName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &gpuValue,
				},
			},
			{
				Name: &ports,
				Ranges: &mesos_v1.Value_Ranges{
					Range: []*mesos_v1.Value_Range{
						{
							Begin: &begin,
							End:   &end,
						},
					},
				},
			},
		},
	}
}
