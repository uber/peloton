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
	"context"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/common/concurrency"
	"github.com/uber/peloton/pkg/common/util"
)

// NewConfigSummary returns aurora config summary for provided list of
// peloton pod infos
func NewConfigSummary(
	jobSummary *stateless.JobSummary,
	podInfos []*pod.PodInfo,
) (*api.ConfigSummary, error) {
	podInfoGroups := groupPodInfos(podInfos)
	var configGroups []*api.ConfigGroup

	jobKey, err := NewJobKey(jobSummary.GetName())
	if err != nil {
		return nil, err
	}

	// create instanceID ranges for pods with same entity version
	// and convert to aurora config group
	for _, group := range podInfoGroups {

		if len(group) == 0 {
			return nil, fmt.Errorf("pod info group with zero element")
		}

		var instanceIDList []uint32
		for _, pod := range group {
			_, instanceID, err := util.ParseTaskID(pod.GetSpec().GetPodName().GetValue())
			if err != nil {
				return nil, fmt.Errorf("unable to parse pod name: %s",
					pod.GetSpec().GetPodName().GetValue())
			}

			instanceIDList = append(instanceIDList, instanceID)
		}

		configGroup, err := NewConfigGroup(
			jobSummary,
			group[0].GetSpec(),
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

// matchGroupsInParallel match pod info with pod info groups in parallel,
// returns a boolean indicating whether there is a match and a index
// in the group if there is.
func matchGroupsInParallel(
	podInfo *pod.PodInfo,
	groups [][]*pod.PodInfo,
) (bool, int) {
	var inputs []interface{}
	for i := int(0); i < len(groups); i++ {
		inputs = append(inputs, i)
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		i := input.(int)
		group := groups[i]

		if len(group) == 0 {
			return -1, nil
		}

		groupPodSpec := group[0].GetSpec()
		if common.IsPodSpecWithoutBridgeUpdateLabelChanged(
			podInfo.GetSpec(),
			groupPodSpec) {
			return -1, nil
		}

		return i, nil
	}

	match := false
	matchIndex := int(-1)

	outputs, _ := concurrency.Map(
		context.TODO(),
		concurrency.MapperFunc(f),
		inputs,
		5)
	for _, o := range outputs {
		index := o.(int)
		if index >= 0 {
			match = true
			matchIndex = index
			break
		}
	}

	return match, matchIndex
}

// groupPodInfos groups a list of PodInfo together if their PodSpec is the
// same, determined by IsPodSpecWithoutBridgeUpdateLabelChanged until function.
func groupPodInfos(podInfos []*pod.PodInfo) [][]*pod.PodInfo {
	var groups [][]*pod.PodInfo

	// TODO(kevinxu): This part is n^2 operation, and needs to be optimized
	for _, podInfo := range podInfos {
		groupMatched, groupIndex := matchGroupsInParallel(podInfo, groups)

		if !groupMatched {
			groups = append(groups, []*pod.PodInfo{podInfo})
			continue
		}

		groups[groupIndex] = append(groups[groupIndex], podInfo)
	}

	return groups
}
