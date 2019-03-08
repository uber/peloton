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

package ptoa

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/util"
)

// NewConfigSummary returns aurora config summary for provided list of
// peloton pod infos
func NewConfigSummary(
	jobInfo *stateless.JobInfo,
	podInfos []*pod.PodInfo,
) (*api.ConfigSummary, error) {
	podInfosByVersion := make(map[string][]*pod.PodInfo)
	var configGroups []*api.ConfigGroup

	jobKey, err := NewJobKey(jobInfo.GetSpec().GetName())
	if err != nil {
		return nil, err
	}

	// get pods with same entity version
	for _, podInfo := range podInfos {
		podVersion := podInfo.GetStatus().GetVersion().GetValue()
		pods := podInfosByVersion[podVersion]

		podInfosByVersion[podVersion] = append(pods, podInfo)
	}

	// create instanceID ranges for pods with same entity version
	// and convert to aurora config group
	for _, pods := range podInfosByVersion {

		var instanceIDList []uint32
		for _, pod := range pods {
			_, instanceID, err := util.ParseTaskID(pod.GetSpec().GetPodName().GetValue())
			if err != nil {
				return nil, fmt.Errorf("unable to parse pod name: %s",
					pod.GetSpec().GetPodName().GetValue())
			}

			instanceIDList = append(instanceIDList, instanceID)
		}

		configGroup, err := NewConfigGroup(
			jobInfo,
			pods[0].GetSpec(),
			instanceIDList,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to get config group %s", err)
		}

		configGroups = append(configGroups, configGroup)
	}

	return &api.ConfigSummary{
		Key:    jobKey,
		Groups: configGroups,
	}, nil
}
