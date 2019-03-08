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

package respool

import (
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
)

const (
	// ResourcePoolPathDelimiter is the delimiter for the resource pool path
	ResourcePoolPathDelimiter = "/"

	// DefaultResPoolSchedulingPolicy is the default scheduling policy for respool
	DefaultResPoolSchedulingPolicy = respool.SchedulingPolicy_PriorityFIFO
)
