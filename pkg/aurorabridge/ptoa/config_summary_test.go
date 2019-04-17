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
	"reflect"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

// Tests the success scenario to get config summary for provided
// list of pod infos
func TestNewConfigSumamry_Success(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()

	jobSummary := &stateless.JobSummary{
		Name: atop.NewJobName(jobKey),
	}

	md1 := fixture.AuroraMetadata()
	md2 := fixture.AuroraMetadata()
	md3 := fixture.AuroraMetadata()
	mdLabels1 := label.NewAuroraMetadataLabels(md1)
	mdLabels2 := label.NewAuroraMetadataLabels(md2)
	mdLabels3 := label.NewAuroraMetadataLabels(md3)

	var podInfos []*pod.PodInfo
	for i := 0; i < 6; i++ {
		var mdLabels []*peloton.Label
		if i < 2 {
			mdLabels = mdLabels1
		} else if i < 5 {
			mdLabels = mdLabels2
		} else {
			mdLabels = mdLabels3
		}

		entityVersion := "1-0-0"
		podName := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		podID := fmt.Sprintf("%s-%d", podName, 1)

		podInfos = append(podInfos, &pod.PodInfo{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{
					Value: podName,
				},
				Labels:     mdLabels,
				Containers: []*pod.ContainerSpec{{}},
			},
			Status: &pod.PodStatus{
				PodId: &peloton.PodID{
					Value: podID,
				},
				Version: &peloton.EntityVersion{
					Value: entityVersion,
				},
			},
		})
	}

	configSummary, err := NewConfigSummary(
		jobSummary,
		podInfos,
	)

	assert.NoError(t, err)
	assert.Equal(t, jobKey, configSummary.GetKey())
	assert.Len(t, configSummary.GetGroups(), 3)
	for _, group := range configSummary.GetGroups() {
		switch {

		case reflect.DeepEqual(group.GetConfig().GetMetadata(), md1):
			assert.Equal(t, []*api.Range{
				{First: ptr.Int32(0), Last: ptr.Int32(1)},
			}, group.GetInstances())

		case reflect.DeepEqual(group.GetConfig().GetMetadata(), md2):
			assert.Equal(t, []*api.Range{
				{First: ptr.Int32(2), Last: ptr.Int32(4)},
			}, group.GetInstances())

		case reflect.DeepEqual(group.GetConfig().GetMetadata(), md3):
			assert.Equal(t, []*api.Range{
				{First: ptr.Int32(5), Last: ptr.Int32(5)},
			}, group.GetInstances())

		default:
			assert.Fail(t, "unexpected metadata in config summary group")
		}
	}

}

// Test the error scenario for invalid task ID for provided pod info

func TestNewConfigSummary_InvalidTaskID(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	podName := fmt.Sprintf("%s-%d", "dummy_job_id", 0)
	podID := fmt.Sprintf("%s-%d", podName, 1)
	entityVersion := fixture.PelotonEntityVersion()

	jobSummary := &stateless.JobSummary{
		Name: atop.NewJobName(jobKey),
	}

	podInfos := []*pod.PodInfo{{
		Spec: &pod.PodSpec{
			PodName: &peloton.PodName{
				Value: podName,
			},
			Containers: []*pod.ContainerSpec{{}},
		},
		Status: &pod.PodStatus{
			PodId: &peloton.PodID{
				Value: podID,
			},
			Version: entityVersion,
		},
	}}
	_, err := NewConfigSummary(jobSummary, podInfos)
	assert.Error(t, err)
}

// Test the error scenario for incorrect config group
func TestNewConfigSummary_ConfigGroupError(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	podName := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	podID := fmt.Sprintf("%s-%d", podName, 1)
	entityVersion := fixture.PelotonEntityVersion()

	jobSummary := &stateless.JobSummary{
		Name: atop.NewJobName(jobKey),
	}

	podInfos := []*pod.PodInfo{{
		Spec: &pod.PodSpec{
			PodName: &peloton.PodName{
				Value: podName,
			},
			// missing ContainerSpec, expect error
			Containers: []*pod.ContainerSpec{},
		},
		Status: &pod.PodStatus{
			PodId: &peloton.PodID{
				Value: podID,
			},
			Version: entityVersion,
		},
	}}
	_, err := NewConfigSummary(jobSummary, podInfos)
	assert.Error(t, err)
}

// TestGroupPodInfos tests groupPodInfos util function groups a list of
// PodInfo correctly based on PodSpec equality determined using
// IsPodSpecWithoutBridgeUpdateLabelChanged.
func TestGroupPodInfos(t *testing.T) {
	jobID := fixture.PelotonJobID()
	podInfos := []*pod.PodInfo{
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-0"},
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-1"},
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k3", Value: "v3"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-2"},
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k4", Value: "v4"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-3"},
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
					{Key: "k3", Value: "v3"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-4"},
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-5"},
				Labels: []*peloton.Label{
					{Key: "k4", Value: "v4"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
		{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{Value: jobID.GetValue() + "-6"},
				Labels: []*peloton.Label{
					{Key: "k3", Value: "v3"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}

	groups := groupPodInfos(podInfos)
	groupTotal := 0
	for _, group := range groups {
		assert.NotEmpty(t, group)
		groupTotal += len(group)
		// Make sure pod spec inside the group is equal
		for i, groupPodInfo := range group {
			j := i + 1
			if j >= len(group) {
				break
			}
			assert.False(t, common.IsPodSpecWithoutBridgeUpdateLabelChanged(
				groupPodInfo.GetSpec(),
				group[j].GetSpec(),
			))
		}
	}
	assert.Equal(t, len(podInfos), groupTotal)
}
