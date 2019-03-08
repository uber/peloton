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

package reservation

import (
	"fmt"
	"strconv"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/pkg/common/util"
)

var (
	_jobKey      = "job"
	_instanceKey = "instance"
	_hostnameKey = "hostname"
)

// CreateReservationLabels creates reservation labels for stateful task.
func CreateReservationLabels(
	jobID string, instanceID uint32, hostname string) *mesos.Labels {
	return &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_jobKey,
				Value: &jobID,
			},
			{
				Key:   &_instanceKey,
				Value: util.PtrPrintf(fmt.Sprint(instanceID)),
			},
			{
				Key:   &_hostnameKey,
				Value: util.PtrPrintf(hostname),
			},
		},
	}
}

// ParseReservationLabels parses jobid and instanceid from given reservation labels.
func ParseReservationLabels(labels *mesos.Labels) (string, uint32, error) {
	var jobID string
	instanceID := -1
	// iterates all the label and parse out jobID and instanceID.
	for _, label := range labels.GetLabels() {
		if label.GetKey() == _jobKey {
			jobID = label.GetValue()
		} else if label.GetKey() == _instanceKey {
			instanceID, _ = strconv.Atoi(label.GetValue())
		}
	}
	// jobID and instanceID are required in reservation labels.
	if len(jobID) == 0 || instanceID == -1 {
		return jobID, uint32(instanceID), fmt.Errorf("invalid reservation labels: %v", labels)
	}
	return jobID, uint32(instanceID), nil
}
