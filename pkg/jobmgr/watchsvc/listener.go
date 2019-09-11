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

package watchsvc

import (
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	log "github.com/sirupsen/logrus"
)

const _listenerName = "WatchListener"

// WatchListener is a job / task runtime event listener which implements
// cached.JobTaskListener interface, used by watch api.
type WatchListener struct {
	processor WatchProcessor
}

// NewWatchListener returns a new instance of watchsvc.WatchListener
func NewWatchListener(processor WatchProcessor) WatchListener {
	return WatchListener{
		processor: processor,
	}
}

// Name returns a user-friendly name for the listener
func (l WatchListener) Name() string {
	return _listenerName
}

// JobSummaryChanged is invoked when the runtime for a job is updated
// in cache and persistent store.
func (l WatchListener) StatelessJobSummaryChanged(
	jobSummary *stateless.JobSummary,
) {
	if jobSummary == nil {
		log.Debug("skip JobRuntimeChanged due to jobSummary being nil")
		return
	}

	if len(jobSummary.GetJobId().GetValue()) == 0 {
		log.Debug("skip JobRuntimeChanged due to jobID being nil")
		return
	}

	l.processor.NotifyJobChange(jobSummary)
}

func (l WatchListener) BatchJobSummaryChanged(
	jobID *v0peloton.JobID,
	jobSummary *job.JobSummary,
) {
	// for now watch api only supports stateless
}

// PodSummaryChanged is invoked when the summary for a pod is updated
// in cache and persistent store.
func (l WatchListener) PodSummaryChanged(
	jobType job.JobType,
	summary *pod.PodSummary,
	labels []*v1peloton.Label,
) {
	// for now watch api only supports stateless
	if jobType != job.JobType_SERVICE {
		log.Debug("skip TaskRuntimeChanged due to not being service type job")
		return
	}

	if summary == nil {
		log.Debug("skip TaskRuntimeChanged due to runtime being nil")
		return
	}

	if len(summary.GetPodName().GetValue()) == 0 {
		log.Debug("skip TaskRuntimeChanged due to pod name being nil")
		return
	}

	l.processor.NotifyPodChange(summary, labels)
}
