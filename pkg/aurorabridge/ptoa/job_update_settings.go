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
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

// NewJobUpdateSettings converts UpdateSpec to aurora JobUpdateSettings.
func NewJobUpdateSettings(u *stateless.UpdateSpec) *api.JobUpdateSettings {
	return &api.JobUpdateSettings{
		UpdateGroupSize:   ptr.Int32(int32(u.GetBatchSize())),
		RollbackOnFailure: ptr.Bool(u.GetRollbackOnFailure()),

		// In Peloton max_instances_retries means number of times
		// an instance will be retried before failing the instance,
		// however in Aurora maxPerInstanceFailures means maximum
		// instance failures will be tolerated, so we need to decrement
		// by 1.
		MaxPerInstanceFailures: ptr.Int32(int32(u.GetMaxInstanceRetries()) - 1),

		// In Peloton max_tolerable_instance_failures means number of
		// instances to fail to trigger an update failure, so we need to
		// decrement it by 1 when converting to maxFailedInstances from
		// Aurora
		MaxFailedInstances: ptr.Int32(int32(u.GetMaxTolerableInstanceFailures()) - 1),

		// Skip mapping to BlockIfNoPulsesAfterMs since we don't have
		// enough information in UpdateSpec
	}
}
