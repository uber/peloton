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
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"
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
func (l WatchListener) JobSummaryChanged(
	jobID *v0peloton.JobID,
	jobType job.JobType,
	jobSummary *job.JobSummary,
	updateInfo *models.UpdateModel,
) {
	// TODO(kevinxu): to be implemented
}

// TaskRuntimeChanged is invoked when the runtime for a task is updated
// in cache and persistent store.
func (l WatchListener) TaskRuntimeChanged(
	jobID *v0peloton.JobID,
	instanceID uint32,
	jobType job.JobType,
	runtime *task.RuntimeInfo,
	labels []*v0peloton.Label,
) {
	// for now watch api only supports stateless
	if jobType != job.JobType_SERVICE {
		log.Debug("skip TaskRuntimeChanged due to not being service type job")
		return
	}

	if jobID == nil {
		log.Debug("skip TaskRuntimeChanged due to jobID being nil")
		return
	}

	if runtime == nil {
		log.Debug("skip TaskRuntimeChanged due to runtime being nil")
		return
	}

	p := &pod.PodSummary{
		PodName: &v1peloton.PodName{
			Value: util.CreatePelotonTaskID(jobID.GetValue(), instanceID),
		},
		Status: api.ConvertTaskRuntimeToPodStatus(runtime),
	}
	l.processor.NotifyTaskChange(p, labels)
}
