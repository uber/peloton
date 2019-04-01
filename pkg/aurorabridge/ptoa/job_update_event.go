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

// NewJobUpdateEvent returns aurora job update event from
// peloton job update event
func NewJobUpdateEvent(
	e *stateless.WorkflowEvent,
	d *opaquedata.Data,
) (*api.JobUpdateEvent, error) {

	jobUpdateStatus, err := NewJobUpdateStatus(
		e.GetState(),
		d)
	if err != nil {
		return nil, fmt.Errorf("unable to parse job update event state %s", err)
	}

	ts, err := common.ConvertTimestampToUnixMS(e.GetTimestamp())
	if err != nil {
		return nil, fmt.Errorf("unable to parse job update event timestamp %s", err)
	}

	var updateMessage *string
	if jobUpdateStatus == api.JobUpdateStatusRollingForward ||
		jobUpdateStatus == api.JobUpdateStatusRollForwardAwaitingPulse {
		// message from startJobUpdate should only appear in either
		// ROLLING_FORWARD or ROLL_FORWARD_AWAITING_PULSE update event
		//
		// TODO(kevinxu): In Aurora, the message should only appear in
		// either ROLLING_FORWARD or ROLL_FORWARD_AWAITING_PULSE once,
		// whichever happens first, but in current implementation, if a
		// ROLL_FORWARD_AWAITING_PULSE is followed by a ROLLING_FORWARD,
		// the message will be attached to both events.
		updateMessage = ptr.String(d.StartJobUpdateMessage)
	}

	return &api.JobUpdateEvent{
		Status:      &jobUpdateStatus,
		TimestampMs: ts,
		Message:     updateMessage,
	}, nil
}
