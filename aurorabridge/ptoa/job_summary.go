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

// NewJobSummary creates a JobSummary object.
func NewJobSummary(
	jobInfo *stateless.JobInfo,
	podSpec *pod.PodSpec,
) (*api.JobSummary, error) {
	stats := newJobStats(jobInfo.GetStatus())

	job, err := NewJobConfiguration(jobInfo, podSpec)
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
) (*api.JobConfiguration, error) {
	// JobSummary.Job.InstanceCount in Aurora represents
	// the sum of current, completed or failed tasks.
	// We return current instance count here only. Reference:
	// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java#L465
	// TODO(kevinxu): Need to match Aurora's behavior?
	instanceCount := jobInfo.GetSpec().GetInstanceCount()

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
		InstanceCount: ptr.Int32(int32(instanceCount)),
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

	for state, c := range s.GetPodStats() {
		switch state {
		// pending
		case task.TaskState_INITIALIZED.String():
			fallthrough
		case task.TaskState_PENDING.String():
			pending += c

		// active
		case task.TaskState_READY.String():
			fallthrough
		case task.TaskState_PLACING.String():
			fallthrough
		case task.TaskState_PLACED.String():
			fallthrough
		case task.TaskState_LAUNCHING.String():
			fallthrough
		case task.TaskState_LAUNCHED.String():
			fallthrough
		case task.TaskState_STARTING.String():
			fallthrough
		case task.TaskState_RUNNING.String():
			fallthrough
		case task.TaskState_KILLING.String():
			fallthrough
		case task.TaskState_PREEMPTING.String():
			active += c

		// finished
		case task.TaskState_SUCCEEDED.String():
			fallthrough
		case task.TaskState_KILLED.String():
			fallthrough
		case task.TaskState_DELETED.String():
			finished += c

		// failed
		case task.TaskState_FAILED.String():
			fallthrough
		case task.TaskState_LOST.String():
			failed += c
		}
	}

	return &api.JobStats{
		ActiveTaskCount:   ptr.Int32(int32(active)),
		FinishedTaskCount: ptr.Int32(int32(finished)),
		FailedTaskCount:   ptr.Int32(int32(failed)),
		PendingTaskCount:  ptr.Int32(int32(pending)),
	}
}
