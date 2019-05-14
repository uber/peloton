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

	"github.com/uber/peloton/pkg/aurorabridge/common"

	"go.uber.org/thriftrw/ptr"
)

// NewTaskTier converts Peloton SlaSpec to Aurora TaskTier string.
func NewTaskTier(s *stateless.SlaSpec) *string {
	if s.GetPreemptible() {
		if s.GetRevocable() {
			return ptr.String(common.Revocable)
		}
		return ptr.String(common.Preemptible)
	}
	return ptr.String(common.Preemptible)
}
