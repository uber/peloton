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
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"
)

// JobTaskListener defines an interface that must to be implemented by
// a listener interested in job and task changes. The callbacks are
// invoked after updates to the cache are written through to the
// persistent store. Note that callbacks may not get invoked in the
// same order as the changes to objects in cache; the version field
// of the changed object (e.g. Changelog) is a better indicator of
// order. To keep things simple, the callbacks are invoked
// synchronously when the cached object is changed. Thus slow
// listeners can make operations on the cache slower, which must be
// avoided.
// Implementations must not
// - modify the provided objects in any way
// - do processing that can take a long time, such as blocking on
//   locks or making remote calls. Such activities must be done
//   in separate goroutines that are managed by the listener.
type JobTaskListener interface {
	// Name returns a user-friendly name for the listener
	Name() string

	// JobSummaryChanged is invoked when the runtime for a job is updated
	// in cache and persistent store.
	JobSummaryChanged(
		jobID *peloton.JobID,
		jobType pbjob.JobType,
		jobSummary *pbjob.JobSummary,
		updateInfo *models.UpdateModel,
	)

	// TaskRuntimeChanged is invoked when the runtime for a task is updated
	// in cache and persistent store.
	TaskRuntimeChanged(
		jobID *peloton.JobID,
		instanceID uint32,
		jobType pbjob.JobType,
		runtime *pbtask.RuntimeInfo,
		labels []*peloton.Label,
	)
}
