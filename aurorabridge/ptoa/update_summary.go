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
	"time"

	"go.uber.org/thriftrw/ptr"

	"github.com/pkg/errors"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/aurorabridge/opaquedata"
)

// NewJobUpdateSummary creates a new aurora job update summary using update info.
func NewJobUpdateSummary(
	jobKey *api.JobKey,
	u *stateless.WorkflowInfo,
) (*api.JobUpdateSummary, error) {
	var createTime int64
	var lastModifiedTime int64

	d, err := opaquedata.Deserialize(u.GetOpaqueData())
	if err != nil {
		return nil, fmt.Errorf("deserialize opaque data: %s", err)
	}

	aState, err := NewJobUpdateStatus(u.GetStatus().GetState(), d)
	if err != nil {
		return nil, err
	}

	events := u.GetEvents()
	eventsLen := len(u.GetEvents())
	if eventsLen > 0 {
		cTime, err := time.Parse(time.RFC3339, events[0].GetTimestamp())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse for create timestamp: %s", events[0].GetTimestamp())
		}
		createTime = cTime.UnixNano() / int64(time.Millisecond)
		lastModifiedTime = createTime
	}
	if eventsLen > 1 {
		mTime, err := time.Parse(time.RFC3339, events[eventsLen-1].GetTimestamp())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse for modified timestamp: %s", events[1].GetTimestamp())
		}
		lastModifiedTime = mTime.UnixNano() / int64(time.Millisecond)
	}

	return &api.JobUpdateSummary{
		Key: &api.JobUpdateKey{
			Job: &api.JobKey{
				Role:        ptr.String(jobKey.GetRole()),
				Environment: ptr.String(jobKey.GetEnvironment()),
				Name:        ptr.String(jobKey.GetName()),
			},
			ID: nil, // TODO: check if aggregator does not consume UpdateID
		},
		User: nil,
		State: &api.JobUpdateState{
			Status:                  &aState,
			CreatedTimestampMs:      &createTime,
			LastModifiedTimestampMs: &lastModifiedTime,
		},
		Metadata: nil, // TODO: convert opaque data to array to metadata(key-value pair)
	}, nil
}
