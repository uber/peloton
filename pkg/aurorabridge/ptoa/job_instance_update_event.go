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
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"

	"go.uber.org/thriftrw/ptr"
)

// NewJobInstanceUpdateEvent returns a new instance update event
// from peloton instance update event
func NewJobInstanceUpdateEvent(
	instanceID uint32,
	e *stateless.WorkflowEvent,
	d *opaquedata.Data,
) (*api.JobInstanceUpdateEvent, error) {

	jobUpdateAction, err := NewJobUpdateAction(e.GetState(), d)
	if err != nil {
		return nil, fmt.Errorf("unable to get job update action for instance update event %s", err)
	}

	ts, err := common.ConvertTimestampToUnixMS(e.GetTimestamp())
	if err != nil {
		return nil, fmt.Errorf("unable to parse instance update event timestamp %s", err)
	}

	return &api.JobInstanceUpdateEvent{
		InstanceId:  ptr.Int32(int32(instanceID)),
		TimestampMs: ts,
		Action:      jobUpdateAction,
	}, nil
}
