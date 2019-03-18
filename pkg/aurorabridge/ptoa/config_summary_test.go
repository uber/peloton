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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"github.com/stretchr/testify/assert"
)

// Tests the success scenario to get config summary for provided
// list of pod infos
func TestNewConfigSumamry_Success(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()

	jobInfo := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name: atop.NewJobName(jobKey),
		},
	}

	var podInfos []*pod.PodInfo
	for i := 0; i < 6; i++ {
		var entityVersion string
		if i < 2 {
			entityVersion = "1"
		} else if i < 5 {
			entityVersion = "2"
		} else {
			entityVersion = "3"
		}

		podName := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		podID := fmt.Sprintf("%s-%d", podName, 1)
		mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())

		podInfos = append(podInfos, &pod.PodInfo{
			Spec: &pod.PodSpec{
				PodName: &peloton.PodName{
					Value: podName,
				},
				Labels:     mdLabel,
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
		jobInfo,
		podInfos,
	)
	assert.NoError(t, err)
	// Creates two config groups indicating set of pods which have same entity version (same pod spec)
	assert.Equal(t, 3, len(configSummary.GetGroups()))
}

// Test the error scenario for invalid task ID for provided pod info

func TestNewConfigSummary_InvalidTaskID(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	podName := fmt.Sprintf("%s-%d", "dummy_job_id", 0)
	podID := fmt.Sprintf("%s-%d", podName, 1)
	entityVersion := fixture.PelotonEntityVersion()

	jobInfo := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name: atop.NewJobName(jobKey),
		},
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
	_, err := NewConfigSummary(jobInfo, podInfos)
	assert.Error(t, err)
}

// Test the error scenario for incorrect config group
func TestNewConfigSummary_ConfigGroupError(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	podName := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	podID := fmt.Sprintf("%s-%d", podName, 1)
	entityVersion := fixture.PelotonEntityVersion()

	jobInfo := &stateless.JobInfo{
		Spec: &stateless.JobSpec{
			Name: atop.NewJobName(jobKey),
		},
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
	_, err := NewConfigSummary(jobInfo, podInfos)
	assert.Error(t, err)
}
