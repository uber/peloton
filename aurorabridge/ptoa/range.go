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
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/util"
	"go.uber.org/thriftrw/ptr"
)

// NewRange converts peloton's instance id list to aurora's range
func NewRange(
	instanceIDList []uint32,
) []*api.Range {

	var ranges []*api.Range
	instanceIDRange := util.ConvertInstanceIDListToInstanceRange(instanceIDList)

	for _, instanceRange := range instanceIDRange {
		ranges = append(ranges, &api.Range{
			First: ptr.Int32(int32(instanceRange.GetFrom())),
			Last:  ptr.Int32(int32(instanceRange.GetTo())),
		})
	}

	return ranges
}
