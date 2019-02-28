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

package atop

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewUpdateSpec creates a new UpdateSpec.
func NewUpdateSpec(s *api.JobUpdateSettings) *stateless.UpdateSpec {
	return &stateless.UpdateSpec{
		BatchSize:                    uint32(s.GetUpdateGroupSize()),
		RollbackOnFailure:            s.GetRollbackOnFailure(),
		MaxInstanceRetries:           uint32(s.GetMaxPerInstanceFailures()),
		MaxTolerableInstanceFailures: uint32(s.GetMaxFailedInstances()),

		// Peloton does not support pulsed updates, so if block if no pulse is
		// set, then we start the update in a paused state such that it must
		// be manually continued.
		StartPaused: s.GetBlockIfNoPulsesAfterMs() > 0,
	}
}
