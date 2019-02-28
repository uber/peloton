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

package update

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
)

// HasUpdate returns if a job has any update going
func HasUpdate(jobRuntime *pbjob.RuntimeInfo) bool {
	return jobRuntime.GetUpdateID() != nil &&
		len(jobRuntime.GetUpdateID().GetValue()) > 0
}

// HasFailedUpdate returns if a task has failed during an update due
// to too many failures.
// If maxAttempts is 0, HasFailedUpdate always returns false.
func HasFailedUpdate(
	taskRuntime *pbtask.RuntimeInfo,
	maxAttempts uint32) bool {
	if maxAttempts == 0 {
		return false
	}

	return taskRuntime.GetFailureCount() >= maxAttempts
}
