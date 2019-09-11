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

package cached

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
)

type FakeJobListener struct {
	jobID               *v0peloton.JobID
	jobType             pbjob.JobType
	statelessJobSummary *stateless.JobSummary
	jobSummary          *pbjob.JobSummary
}

func (l *FakeJobListener) Name() string {
	return "fake_job_listener"
}

func (l *FakeJobListener) StatelessJobSummaryChanged(
	jobSummary *stateless.JobSummary,
) {
	l.statelessJobSummary = jobSummary
	l.jobType = pbjob.JobType_SERVICE
}

func (l *FakeJobListener) BatchJobSummaryChanged(
	jobID *v0peloton.JobID,
	jobSummary *pbjob.JobSummary,
) {
	l.jobID = jobID
	l.jobSummary = jobSummary
	l.jobType = pbjob.JobType_BATCH
}

func (l *FakeJobListener) PodSummaryChanged(
	jobType pbjob.JobType,
	summary *pod.PodSummary,
	labels []*peloton.Label,
) {
}

func (l *FakeJobListener) Reset() {
	l.jobID = nil
	l.jobSummary = nil
	l.statelessJobSummary = nil
}

type FakeTaskListener struct {
	jobType pbjob.JobType
	summary *pod.PodSummary
	labels  []*peloton.Label
}

func (l *FakeTaskListener) Name() string {
	return "fake_task_listener"
}

func (l *FakeTaskListener) StatelessJobSummaryChanged(
	jobSummary *stateless.JobSummary,
) {
}

func (l *FakeTaskListener) BatchJobSummaryChanged(
	jobID *v0peloton.JobID,
	jobSummary *pbjob.JobSummary,
) {
}

func (l *FakeTaskListener) PodSummaryChanged(
	jobType pbjob.JobType,
	summary *pod.PodSummary,
	labels []*peloton.Label,
) {
	l.jobType = jobType
	l.summary = summary
	l.labels = labels
}
