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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
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

	// StatelessJobSummaryChanged is invoked when the runtime for a stateless
	// job is updated in cache and persistent store.
	StatelessJobSummaryChanged(jobSummary *stateless.JobSummary)

	// BatchJobSummaryChanged is invoked when the runtime for a batch
	// job is updated in cache and persistent store.
	BatchJobSummaryChanged(
		jobID *peloton.JobID,
		jobSummary *pbjob.JobSummary,
	)

	// PodSummaryChanged is invoked when the status for a task is updated
	// in cache and persistent store.
	PodSummaryChanged(
		// TODO Remove once batch moves to v1 alpha apis
		jobType pbjob.JobType,
		summary *pod.PodSummary,
		labels []*v1peloton.Label,
	)
}
