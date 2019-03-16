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
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

var _pendingTaskStates = []string{
	task.TaskState_INITIALIZED.String(),
	task.TaskState_PENDING.String(),
}

var _activeTaskStates = []string{
	task.TaskState_READY.String(),
	task.TaskState_PLACING.String(),
	task.TaskState_PLACED.String(),
	task.TaskState_LAUNCHING.String(),
	task.TaskState_LAUNCHED.String(),
	task.TaskState_STARTING.String(),
	task.TaskState_RUNNING.String(),
	task.TaskState_KILLING.String(),
	task.TaskState_PREEMPTING.String(),
}

var _finishedTaskStates = []string{
	task.TaskState_SUCCEEDED.String(),
	task.TaskState_KILLED.String(),
	task.TaskState_DELETED.String(),
}

var _failedTaskStates = []string{
	task.TaskState_FAILED.String(),
	task.TaskState_LOST.String(),
}

// NewJobSummary creates a JobSummary object.
func NewJobSummary(
	jobInfo *stateless.JobInfo,
	podSpec *pod.PodSpec,
) (*api.JobSummary, error) {
	stats := newJobStats(jobInfo.GetStatus())

	job, err := NewJobConfiguration(jobInfo, podSpec, false)
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
	jobInfo *stateless.JobInfo,
	podSpec *pod.PodSpec,
	activeOnly bool,
) (*api.JobConfiguration, error) {
	var instanceCount int32
	if activeOnly {
		// activeOnly is set to true when called from getJobs endpoint
		for _, activeState := range _activeTaskStates {
			instanceCount += int32(jobInfo.GetStatus().GetPodStats()[activeState])
		}
	} else {
		// for getJobSummary endpoint (activeOnly = false)
		// JobSummary.Job.InstanceCount in Aurora represents
		// the sum of current, completed and failed tasks.
		// We return current instance count here only. Reference:
		// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java#L465
		// TODO(kevinxu): Need to match Aurora's behavior?
		instanceCount = int32(jobInfo.GetSpec().GetInstanceCount())
	}

	auroraOwner := NewIdentity(jobInfo.GetSpec().GetOwner())

	jobKey, err := NewJobKey(jobInfo.GetSpec().GetName())
	if err != nil {
		return nil, err
	}

	taskConfig, err := NewTaskConfig(jobInfo, podSpec)
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

	for _, state := range _activeTaskStates {
		active += s.GetPodStats()[state]
	}

	for _, state := range _finishedTaskStates {
		finished += s.GetPodStats()[state]
	}

	for _, state := range _failedTaskStates {
		failed += s.GetPodStats()[state]
	}

	for _, state := range _pendingTaskStates {
		pending += s.GetPodStats()[state]
	}

	return &api.JobStats{
		ActiveTaskCount:   ptr.Int32(int32(active)),
		FinishedTaskCount: ptr.Int32(int32(finished)),
		FailedTaskCount:   ptr.Int32(int32(failed)),
		PendingTaskCount:  ptr.Int32(int32(pending)),
	}
}
