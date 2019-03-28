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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

var _pendingPodStates = []string{
	pod.PodState_POD_STATE_INITIALIZED.String(),
	pod.PodState_POD_STATE_PENDING.String(),
	pod.PodState_POD_STATE_READY.String(),
	pod.PodState_POD_STATE_PLACING.String(),
}

var _activePodStates = []string{
	pod.PodState_POD_STATE_PLACED.String(),
	pod.PodState_POD_STATE_LAUNCHING.String(),
	pod.PodState_POD_STATE_LAUNCHED.String(),
	pod.PodState_POD_STATE_STARTING.String(),
	pod.PodState_POD_STATE_RUNNING.String(),
	pod.PodState_POD_STATE_KILLING.String(),
	pod.PodState_POD_STATE_PREEMPTING.String(),
}

var _finishedPodStates = []string{
	pod.PodState_POD_STATE_SUCCEEDED.String(),
	pod.PodState_POD_STATE_KILLED.String(),
	pod.PodState_POD_STATE_DELETED.String(),
}

var _failedPodStates = []string{
	pod.PodState_POD_STATE_FAILED.String(),
	pod.PodState_POD_STATE_LOST.String(),
}

// NewJobSummary creates a JobSummary object.
func NewJobSummary(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
) (*api.JobSummary, error) {
	stats := newJobStats(jobSummary.GetStatus())

	job, err := NewJobConfiguration(jobSummary, podSpec, false)
	if err != nil {
		return nil, err
	}

	return &api.JobSummary{
		Stats: stats,
		Job:   job,
		//NextCronRunMs: nil,
	}, nil
}

// NewJobConfiguration creates a JobConfiguration object.
func NewJobConfiguration(
	jobSummary *stateless.JobSummary,
	podSpec *pod.PodSpec,
	activeOnly bool,
) (*api.JobConfiguration, error) {
	var instanceCount int32
	if activeOnly {
		// activeOnly is set to true when called from getJobs endpoint
		for _, activeState := range _activePodStates {
			instanceCount += int32(jobSummary.GetStatus().GetPodStats()[activeState])
		}
	} else {
		// for getJobSummary endpoint (activeOnly = false)
		// JobSummary.Job.InstanceCount in Aurora represents
		// the sum of current, completed and failed tasks.
		// We return current instance count here only. Reference:
		// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java#L465
		// TODO(kevinxu): Need to match Aurora's behavior?
		instanceCount = int32(jobSummary.GetInstanceCount())
	}

	auroraOwner := NewIdentity(jobSummary.GetOwner())

	jobKey, err := NewJobKey(jobSummary.GetName())
	if err != nil {
		return nil, err
	}

	taskConfig, err := NewTaskConfig(jobSummary, podSpec)
	if err != nil {
		return nil, err
	}

	return &api.JobConfiguration{
		Key:           jobKey,
		Owner:         auroraOwner,
		TaskConfig:    taskConfig,
		InstanceCount: ptr.Int32(instanceCount),
		//CronSchedule:        nil,
		//CronCollisionPolicy: nil,
	}, nil
}

// newJobStats creates a JobStats object.
// Reference:
// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/base/Jobs.java#L54
func newJobStats(s *stateless.JobStatus) *api.JobStats {
	var (
		active   uint32
		finished uint32
		failed   uint32
		pending  uint32
	)

	for _, state := range _activePodStates {
		active += s.GetPodStats()[state]
	}

	for _, state := range _finishedPodStates {
		finished += s.GetPodStats()[state]
	}

	for _, state := range _failedPodStates {
		failed += s.GetPodStats()[state]
	}

	for _, state := range _pendingPodStates {
		pending += s.GetPodStats()[state]
	}

	return &api.JobStats{
		ActiveTaskCount:   ptr.Int32(int32(active)),
		FinishedTaskCount: ptr.Int32(int32(finished)),
		FailedTaskCount:   ptr.Int32(int32(failed)),
		PendingTaskCount:  ptr.Int32(int32(pending)),
	}
}
